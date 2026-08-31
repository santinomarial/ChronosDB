#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] DistributedMutableVectorGroupedAggregateQuerySender::TimePoint
saturating_add(const DistributedMutableVectorGroupedAggregateQuerySender::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted = std::chrono::duration_cast<
      DistributedMutableVectorGroupedAggregateQuerySender::TimePoint::duration>(delay);
  if (now > DistributedMutableVectorGroupedAggregateQuerySender::TimePoint::max() - converted)
    return DistributedMutableVectorGroupedAggregateQuerySender::TimePoint::max();
  return now + converted;
}

[[nodiscard]] bool valid_payload_limits(
    const query::DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             query::distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             query::distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             query::distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             query::distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] bool
same_grouped_authority(const query::DistributedVectorGroupedAggregateAuthority& left,
                       const query::DistributedVectorGroupedAggregateAuthority& right) noexcept {
  if (left.keys.size() != right.keys.size() || left.aggregates.size() != right.aggregates.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < left.keys.size(); ++ordinal) {
    if (left.keys[ordinal].column_ordinal != right.keys[ordinal].column_ordinal ||
        left.keys[ordinal].type != right.keys[ordinal].type ||
        left.keys[ordinal].nullable != right.keys[ordinal].nullable) {
      return false;
    }
  }
  return std::equal(left.aggregates.begin(), left.aggregates.end(), right.aggregates.begin());
}

[[nodiscard]] bool same_grouped_authority(
    const query::DistributedVectorGroupedAggregateAuthority& authority,
    const std::span<const query::VectorGroupKeyDefinition> keys,
    const std::span<const query::VectorAggregateDefinition> aggregates) noexcept {
  if (authority.keys.size() != keys.size() || authority.aggregates.size() != aggregates.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
    if (authority.keys[ordinal].column_ordinal != keys[ordinal].column_ordinal ||
        authority.keys[ordinal].type != keys[ordinal].type ||
        authority.keys[ordinal].nullable != keys[ordinal].nullable) {
      return false;
    }
  }
  return std::equal(authority.aggregates.begin(), authority.aggregates.end(), aggregates.begin());
}

[[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateAuthority>
bind_worker_authority(DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
                      const query::DistributedMutableVectorFragment& fragment) noexcept {
  try {
    return worker.bind_authority(fragment);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query authority binding allocation failed"));
  } catch (...) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "mutable grouped vector query authority binding threw"});
  }
}

[[nodiscard]] common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
execute_worker(DistributedMutableVectorGroupedAggregateQueryWorkerService& worker,
               const query::DistributedMutableVectorFragment& fragment) noexcept {
  try {
    return worker.execute(fragment);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query worker allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "mutable grouped vector query worker threw"});
  }
}

[[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
one_response(std::vector<std::byte> encoded) {
  try {
    std::vector<std::vector<std::byte>> frames;
    frames.push_back(std::move(encoded));
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query response publication allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query response publication exceeds container limits"));
  }
}

} // namespace

common::Status validate_distributed_mutable_vector_grouped_aggregate_query_authority(
    const query::DistributedMutableVectorFragment& fragment,
    const std::span<const query::VectorGroupKeyDefinition> keys,
    const std::span<const query::VectorAggregateDefinition> aggregates) {
  common::Status structural = query::validate_distributed_mutable_vector_fragment(fragment);
  if (!structural.is_ok())
    return structural;
  const auto& plan = fragment.plan;
  const auto& columns = fragment.result_schema.columns;
  if (plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate || keys.empty() ||
      keys.size() != plan.group_key_input_indices.size() ||
      aggregates.size() != plan.aggregates.size() ||
      columns.size() != keys.size() + aggregates.size()) {
    return invalid("mutable grouped vector query requires exact key and aggregate authority");
  }
  common::Status authority =
      query::validate_distributed_vector_grouped_aggregate_authority(keys, aggregates);
  if (!authority.is_ok())
    return authority;
  for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
    if (keys[ordinal].column_ordinal != plan.group_key_input_indices[ordinal] ||
        keys[ordinal].type != columns[ordinal].type ||
        keys[ordinal].nullable != columns[ordinal].nullable) {
      return invalid("mutable grouped vector query key authority differs from the admitted plan");
    }
  }
  for (std::size_t ordinal = 0U; ordinal < aggregates.size(); ++ordinal) {
    const auto& intent = plan.aggregates[ordinal];
    const auto& definition = aggregates[ordinal];
    if (definition.operation != intent.operation ||
        definition.input.has_value() != intent.input_index.has_value() ||
        (definition.input.has_value() && definition.input->column_ordinal != *intent.input_index)) {
      return invalid(
          "mutable grouped vector query aggregate authority differs from the admitted plan");
    }
    const auto shape = query::vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return shape.error();
    const auto& column = columns[keys.size() + ordinal];
    if (shape->type != column.type || shape->nullable != column.nullable) {
      return invalid("mutable grouped vector query aggregate authority differs from result schema");
    }
  }
  return common::Status::ok();
}

DistributedMutableVectorGroupedAggregateQueryReceiver::
    DistributedMutableVectorGroupedAggregateQueryReceiver(
        const DistributedMutableVectorGroupedAggregateQueryReceiverConfig config) noexcept
    : config_(config) {}

common::Result<DistributedMutableVectorGroupedAggregateQueryReceiver>
DistributedMutableVectorGroupedAggregateQueryReceiver::create(
    const DistributedMutableVectorGroupedAggregateQueryReceiverConfig config) {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.worker == nullptr ||
      config.maximum_response_frames == 0U ||
      config.maximum_response_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      config.maximum_response_frames > config.payload.maximum_groups ||
      config.maximum_response_bytes < kMinimumResponseBytes ||
      config.maximum_response_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes ||
      config.maximum_decode_memory_bytes == 0U ||
      config.maximum_decode_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes ||
      !valid_payload_limits(config.payload)) {
    return common::make_unexpected(
        invalid("mutable grouped vector query receiver configuration is invalid"));
  }
  return DistributedMutableVectorGroupedAggregateQueryReceiver{config};
}

common::Result<std::vector<std::vector<std::byte>>>
DistributedMutableVectorGroupedAggregateQueryReceiver::receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  auto bound = receive_bound(request_bytes, authenticated_peer);
  if (!bound.has_value())
    return common::make_unexpected(bound.error());
  return std::move(bound->encoded_responses);
}

common::Result<DistributedMutableVectorGroupedAggregateQueryBoundResponses>
DistributedMutableVectorGroupedAggregateQueryReceiver::receive_bound(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("mutable grouped vector query requires an authenticated principal"));
  }
  auto request = decode_distributed_mutable_vector_query_request_exact(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized) {
    return common::make_unexpected(unauthenticated(
        "authenticated principal cannot claim mutable grouped vector query source node"));
  }
  if (request->target_node_id != config_.local_node_id) {
    return common::make_unexpected(
        unavailable("mutable grouped vector query targets a different node"));
  }
  if (request->fragment.plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate) {
    return common::make_unexpected(
        invalid("mutable grouped vector query receiver requires a grouped aggregate plan"));
  }

  auto authority = bind_worker_authority(*config_.worker, request->fragment);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  const common::Status authority_status =
      validate_distributed_mutable_vector_grouped_aggregate_query_authority(
          request->fragment, authority->keys, authority->aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  auto resources = query::QueryResourceContext::create(config_.maximum_decode_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());

  const auto& identity = request->fragment;
  const auto encode_failure = [&](const common::StatusCode code,
                                  const std::optional<DistributedQueryLeaderHint> leader_hint =
                                      std::nullopt)
      -> common::Result<DistributedMutableVectorGroupedAggregateQueryBoundResponses> {
    auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
        {.source_node_id = config_.local_node_id,
         .target_node_id = request->source_node_id,
         .query_id = identity.query_id,
         .tablet_id = identity.tablet_id,
         .status_code = code,
         .leader_hint = leader_hint},
        authority->keys, authority->aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    auto frames = one_response(std::move(*encoded));
    if (!frames.has_value())
      return common::make_unexpected(frames.error());
    return DistributedMutableVectorGroupedAggregateQueryBoundResponses{
        .authority = std::move(*authority), .encoded_responses = std::move(*frames)};
  };

  auto result = execute_worker(*config_.worker, request->fragment);
  if (!result.has_value()) {
    std::optional<DistributedQueryLeaderHint> leader_hint;
    if (result.error().code() == common::StatusCode::kUnavailable &&
        config_.leader_hint_provider != nullptr) {
      auto resolved = config_.leader_hint_provider->current_leader_hint(identity.tablet_id,
                                                                        identity.raft_group_id);
      if (!resolved.has_value())
        return common::make_unexpected(resolved.error());
      leader_hint = *resolved;
    }
    return encode_failure(result.error().code(), leader_hint);
  }
  if (!same_grouped_authority(result->authority, *authority)) {
    return common::make_unexpected(
        invalid("mutable grouped vector query worker authority changed after binding"));
  }
  if (result->messages.empty()) {
    return common::make_unexpected(
        invalid("mutable grouped vector query worker returned an empty response vector"));
  }
  if (result->messages.size() > config_.maximum_response_frames)
    return encode_failure(common::StatusCode::kResourceExhausted);

  std::size_t total_response_bytes{};
  try {
    std::vector<std::vector<std::byte>> frames;
    frames.reserve(result->messages.size());
    for (std::size_t ordinal = 0U; ordinal < result->messages.size(); ++ordinal) {
      auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          result->messages[ordinal].bytes(), authority->keys, authority->aggregates, *resources,
          config_.payload);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      const auto& position = decoded->position();
      const bool last = ordinal + 1U == result->messages.size();
      const bool valid_empty = position.empty && result->messages.size() == 1U &&
                               position.group_count == 0U && position.group_ordinal == 0U &&
                               position.sequence == 1U && position.terminal;
      const bool valid_groups = !position.empty &&
                                position.group_count == result->messages.size() &&
                                position.group_ordinal == ordinal &&
                                position.sequence == ordinal + 1U && position.terminal == last;
      if (position.query_id != identity.query_id || position.tablet_id != identity.tablet_id ||
          (!valid_empty && !valid_groups)) {
        return common::make_unexpected(
            invalid("mutable grouped vector query worker stream is not correlated and complete"));
      }
      auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
          {.source_node_id = config_.local_node_id,
           .target_node_id = request->source_node_id,
           .query_id = identity.query_id,
           .tablet_id = identity.tablet_id,
           .status_code = common::StatusCode::kOk,
           .payload = std::move(*decoded)},
          authority->keys, authority->aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > config_.maximum_response_bytes - total_response_bytes)
        return encode_failure(common::StatusCode::kResourceExhausted);
      total_response_bytes += encoded->size();
      frames.push_back(std::move(*encoded));
    }
    return DistributedMutableVectorGroupedAggregateQueryBoundResponses{
        .authority = std::move(*authority), .encoded_responses = std::move(frames)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query response publication allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query response publication exceeds container limits"));
  }
}

DistributedMutableVectorGroupedAggregateQuerySender::
    DistributedMutableVectorGroupedAggregateQuerySender(
        const raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
        std::vector<query::VectorGroupKeyDefinition>&& keys,
        std::vector<query::VectorAggregateDefinition>&& aggregates,
        query::QueryResourceContext resources, std::vector<std::byte>&& request_bytes,
        const DistributedMutableVectorGroupedAggregateQuerySenderLimits limits,
        const bool local) noexcept
    : source_node_id_(source_node_id), fragment_(std::move(fragment)), keys_(std::move(keys)),
      aggregates_(std::move(aggregates)), resources_(std::move(resources)),
      request_bytes_(std::move(request_bytes)), limits_(limits),
      next_backoff_(limits.retry.initial_backoff), local_(local) {}

common::Result<DistributedMutableVectorGroupedAggregateQuerySender>
DistributedMutableVectorGroupedAggregateQuerySender::create(
    const raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedMutableVectorGroupedAggregateQuerySenderLimits limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (source_node_id == 0U || limits.retry.maximum_attempts == 0U ||
      limits.retry.maximum_attempts > 1024U || limits.retry.initial_backoff.count() <= 0 ||
      limits.retry.maximum_backoff < limits.retry.initial_backoff ||
      limits.retry.maximum_backoff > maximum_supported_backoff ||
      limits.maximum_response_frames == 0U ||
      limits.maximum_response_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      limits.maximum_response_frames > limits.payload.maximum_groups ||
      limits.maximum_response_bytes < kMinimumResponseBytes ||
      limits.maximum_response_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes ||
      fragment.serving_node == source_node_id || !valid_payload_limits(limits.payload)) {
    return common::make_unexpected(
        invalid("mutable grouped vector query sender configuration is invalid"));
  }
  const common::Status authority_status =
      validate_distributed_mutable_vector_grouped_aggregate_query_authority(fragment, keys,
                                                                            aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  if (keys.size() > limits.payload.maximum_group_keys ||
      aggregates.size() > limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("mutable grouped vector query sender authority exceeds decode limits"));
  }
  auto request_bytes =
      encode_distributed_mutable_vector_query_request({.source_node_id = source_node_id,
                                                       .target_node_id = fragment.serving_node,
                                                       .fragment = fragment});
  if (!request_bytes.has_value())
    return common::make_unexpected(request_bytes.error());
  return DistributedMutableVectorGroupedAggregateQuerySender{
      source_node_id,       std::move(fragment),       std::move(keys), std::move(aggregates),
      std::move(resources), std::move(*request_bytes), limits,          false};
}

common::Result<DistributedMutableVectorGroupedAggregateQuerySender>
DistributedMutableVectorGroupedAggregateQuerySender::create_local(
    const raft::NodeId local_node_id, query::DistributedMutableVectorFragment fragment,
    std::vector<query::VectorGroupKeyDefinition>&& keys,
    std::vector<query::VectorAggregateDefinition>&& aggregates,
    query::QueryResourceContext resources,
    const DistributedMutableVectorGroupedAggregateQuerySenderLimits limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
      kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
  if (local_node_id == 0U || fragment.serving_node != local_node_id ||
      limits.retry.maximum_attempts == 0U || limits.retry.maximum_attempts > 1024U ||
      limits.retry.initial_backoff.count() <= 0 ||
      limits.retry.maximum_backoff < limits.retry.initial_backoff ||
      limits.retry.maximum_backoff > maximum_supported_backoff ||
      limits.maximum_response_frames == 0U ||
      limits.maximum_response_frames >
          query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups ||
      limits.maximum_response_frames > limits.payload.maximum_groups ||
      limits.maximum_response_bytes < kMinimumResponseBytes ||
      limits.maximum_response_bytes >
          kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes ||
      !valid_payload_limits(limits.payload)) {
    return common::make_unexpected(
        invalid("local mutable grouped vector query sender configuration is invalid"));
  }
  const common::Status authority_status =
      validate_distributed_mutable_vector_grouped_aggregate_query_authority(fragment, keys,
                                                                            aggregates);
  if (!authority_status.is_ok())
    return common::make_unexpected(authority_status);
  if (keys.size() > limits.payload.maximum_group_keys ||
      aggregates.size() > limits.payload.maximum_aggregates) {
    return common::make_unexpected(
        invalid("local mutable grouped vector query sender authority exceeds decode limits"));
  }
  return DistributedMutableVectorGroupedAggregateQuerySender{local_node_id,
                                                             std::move(fragment),
                                                             std::move(keys),
                                                             std::move(aggregates),
                                                             std::move(resources),
                                                             {},
                                                             limits,
                                                             true};
}

common::Result<DistributedMutableVectorGroupedAggregateQueryAttempt>
DistributedMutableVectorGroupedAggregateQuerySender::begin_attempt(const TimePoint now) {
  if (local_)
    return common::make_unexpected(
        invalid("local mutable grouped vector query sender has no transport attempt"));
  if (state_ == DistributedQuerySenderState::kSucceeded ||
      state_ == DistributedQuerySenderState::kFailed) {
    return common::make_unexpected(invalid("mutable grouped vector query sender is terminal"));
  }
  if (state_ == DistributedQuerySenderState::kWaitingForResponse) {
    return common::make_unexpected(unavailable("mutable grouped vector query response is pending"));
  }
  if (state_ == DistributedQuerySenderState::kBackoff && now < *next_attempt_not_before_) {
    return common::make_unexpected(
        unavailable("mutable grouped vector query retry backoff is active"));
  }
  if (attempts_started_ >= limits_.retry.maximum_attempts) {
    return common::make_unexpected(
        invalid("mutable grouped vector query retry budget is exhausted"));
  }
  try {
    std::vector<std::byte> request_bytes = request_bytes_;
    ++attempts_started_;
    state_ = DistributedQuerySenderState::kWaitingForResponse;
    suggested_leader_.reset();
    next_attempt_not_before_.reset();
    return DistributedMutableVectorGroupedAggregateQueryAttempt{
        attempts_started_, fragment_.serving_node, std::move(request_bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query attempt allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable grouped vector query attempt exceeds container limits"));
  }
}

common::Status DistributedMutableVectorGroupedAggregateQuerySender::execute_local(
    DistributedMutableVectorGroupedAggregateQueryWorkerService& worker, const TimePoint now) {
  if (!local_)
    return invalid("remote mutable grouped vector query sender requires transport");
  if (state_ == DistributedQuerySenderState::kSucceeded ||
      state_ == DistributedQuerySenderState::kFailed) {
    return invalid("local mutable grouped vector query sender is terminal");
  }
  if (state_ == DistributedQuerySenderState::kWaitingForResponse)
    return unavailable("local mutable grouped vector query execution is already active");
  if (state_ == DistributedQuerySenderState::kBackoff && now < *next_attempt_not_before_)
    return unavailable("local mutable grouped vector query retry backoff is active");
  if (attempts_started_ >= limits_.retry.maximum_attempts)
    return invalid("local mutable grouped vector query retry budget is exhausted");

  ++attempts_started_;
  state_ = DistributedQuerySenderState::kWaitingForResponse;
  suggested_leader_.reset();
  next_attempt_not_before_.reset();
  auto authority = bind_worker_authority(worker, fragment_);
  if (!authority.has_value())
    return schedule(authority.error().code(), now);
  if (!same_grouped_authority(*authority, keys_, aggregates_))
    return schedule(common::StatusCode::kInvalidArgument, now);
  auto result = execute_worker(worker, fragment_);
  if (!result.has_value())
    return schedule(result.error().code(), now);
  if (!same_grouped_authority(result->authority, keys_, aggregates_) || result->messages.empty())
    return schedule(common::StatusCode::kInvalidArgument, now);
  if (result->messages.size() > limits_.maximum_response_frames)
    return schedule(common::StatusCode::kResourceExhausted, now);

  try {
    std::vector<DistributedVectorGroupedAggregateQueryResponseV2> responses;
    responses.reserve(result->messages.size());
    for (const auto& encoded : result->messages) {
      auto decoded = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          encoded.bytes(), keys_, aggregates_, resources_, limits_.payload);
      if (!decoded.has_value())
        return schedule(decoded.error().code(), now);
      responses.push_back({.source_node_id = fragment_.serving_node,
                           .target_node_id = source_node_id_,
                           .query_id = fragment_.query_id,
                           .tablet_id = fragment_.tablet_id,
                           .status_code = common::StatusCode::kOk,
                           .payload = std::move(*decoded)});
    }
    return accept_responses(responses, now);
  } catch (const std::bad_alloc&) {
    return schedule(common::StatusCode::kResourceExhausted, now);
  } catch (const std::length_error&) {
    return schedule(common::StatusCode::kResourceExhausted, now);
  }
}

common::Status DistributedMutableVectorGroupedAggregateQuerySender::accept_responses(
    const std::span<const DistributedVectorGroupedAggregateQueryResponseV2> responses,
    const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("mutable grouped vector query sender has no pending response");
  if (responses.empty())
    return invalid("mutable grouped vector query response vector is empty");
  if (responses.size() > limits_.maximum_response_frames)
    return exhausted("mutable grouped vector query response vector exceeds sender frame limit");

  const auto validate_correlation =
      [&](const DistributedVectorGroupedAggregateQueryResponseV2& response) {
        return response.source_node_id == fragment_.serving_node &&
               response.target_node_id == source_node_id_ &&
               response.query_id == fragment_.query_id && response.tablet_id == fragment_.tablet_id;
      };
  if (responses.front().status_code != common::StatusCode::kOk) {
    if (responses.size() != 1U || responses.front().payload.has_value() ||
        !validate_correlation(responses.front())) {
      return invalid("mutable grouped vector query failure response is invalid");
    }
    if (local_) {
      suggested_leader_ = responses.front().leader_hint;
      return schedule(responses.front().status_code, now);
    }
    auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
        responses.front(), keys_, aggregates_);
    if (!encoded.has_value())
      return encoded.error();
    if (encoded->size() > limits_.maximum_response_bytes)
      return exhausted("mutable grouped vector query response exceeds sender byte limit");
    auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
        *encoded, keys_, aggregates_, resources_, limits_.payload);
    if (!decoded.has_value())
      return decoded.error();
    suggested_leader_ = decoded->leader_hint;
    return schedule(decoded->status_code, now);
  }

  std::size_t total_response_bytes{};
  try {
    std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> accepted;
    accepted.reserve(responses.size());
    for (std::size_t ordinal = 0U; ordinal < responses.size(); ++ordinal) {
      const auto& response = responses[ordinal];
      if (!validate_correlation(response) || response.status_code != common::StatusCode::kOk ||
          !response.payload.has_value() || response.leader_hint.has_value()) {
        return invalid("mutable grouped vector query success response is invalid");
      }
      const auto& position = response.payload->position();
      const bool last = ordinal + 1U == responses.size();
      const bool valid_empty = position.empty && responses.size() == 1U &&
                               position.group_count == 0U && position.group_ordinal == 0U &&
                               position.sequence == 1U && position.terminal;
      const bool valid_groups = !position.empty && position.group_count == responses.size() &&
                                position.group_ordinal == ordinal &&
                                position.sequence == ordinal + 1U && position.terminal == last;
      if (position.query_id != fragment_.query_id || position.tablet_id != fragment_.tablet_id ||
          (!valid_empty && !valid_groups)) {
        return invalid("mutable grouped vector query success response sequence is invalid");
      }
      if (local_) {
        auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
            *response.payload, keys_, aggregates_);
        if (!nested.has_value())
          return nested.error();
        constexpr std::size_t kEnvelopeBytes =
            kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
            kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;
        if (total_response_bytes > limits_.maximum_response_bytes - kEnvelopeBytes ||
            nested->bytes().size() >
                limits_.maximum_response_bytes - kEnvelopeBytes - total_response_bytes) {
          return exhausted(
              "mutable grouped vector query response vector exceeds sender byte limit");
        }
        total_response_bytes += kEnvelopeBytes + nested->bytes().size();
        accepted.push_back(std::move(*nested));
        continue;
      }
      auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(response, keys_,
                                                                                   aggregates_);
      if (!encoded.has_value())
        return encoded.error();
      if (encoded->size() > limits_.maximum_response_bytes - total_response_bytes)
        return exhausted("mutable grouped vector query response vector exceeds sender byte limit");
      total_response_bytes += encoded->size();
      auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
          *encoded, keys_, aggregates_, resources_, limits_.payload);
      if (!decoded.has_value())
        return decoded.error();
      auto* payload = optional_pointer(decoded->payload);
      if (payload == nullptr)
        return corruption("mutable grouped vector query canonical success payload is absent");
      auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          *payload, keys_, aggregates_);
      if (!nested.has_value())
        return nested.error();
      accepted.push_back(std::move(*nested));
    }
    result_ = std::move(accepted);
  } catch (const std::bad_alloc&) {
    return exhausted("mutable grouped vector query sender result allocation failed");
  } catch (const std::length_error&) {
    return exhausted("mutable grouped vector query sender result exceeds container limits");
  }
  last_status_code_ = common::StatusCode::kOk;
  suggested_leader_.reset();
  state_ = DistributedQuerySenderState::kSucceeded;
  next_attempt_not_before_.reset();
  return common::Status::ok();
}

common::Status DistributedMutableVectorGroupedAggregateQuerySender::record_transport_failure(
    const common::StatusCode code, const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("mutable grouped vector query sender has no active transport attempt");
  if (code == common::StatusCode::kOk)
    return invalid("mutable grouped vector query transport failure cannot be OK");
  suggested_leader_.reset();
  return schedule(code, now);
}

common::Status
DistributedMutableVectorGroupedAggregateQuerySender::schedule(const common::StatusCode code,
                                                              const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.retry.maximum_attempts) {
    state_ = DistributedQuerySenderState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedQuerySenderState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.retry.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.retry.maximum_backoff.count();
    next_backoff_ = current > maximum / 2
                        ? limits_.retry.maximum_backoff
                        : std::min(next_backoff_ * 2, limits_.retry.maximum_backoff);
  }
  return common::Status::ok();
}

DistributedQuerySenderState
DistributedMutableVectorGroupedAggregateQuerySender::state() const noexcept {
  return state_;
}

std::size_t DistributedMutableVectorGroupedAggregateQuerySender::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedMutableVectorGroupedAggregateQuerySender::TimePoint>
DistributedMutableVectorGroupedAggregateQuerySender::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
DistributedMutableVectorGroupedAggregateQuerySender::last_status_code() const noexcept {
  return last_status_code_;
}

std::optional<DistributedQueryLeaderHint>
DistributedMutableVectorGroupedAggregateQuerySender::suggested_leader() const noexcept {
  return suggested_leader_;
}

std::span<const query::VectorGroupKeyDefinition>
DistributedMutableVectorGroupedAggregateQuerySender::keys() const noexcept {
  return keys_;
}

std::span<const query::VectorAggregateDefinition>
DistributedMutableVectorGroupedAggregateQuerySender::aggregates() const noexcept {
  return aggregates_;
}

const std::optional<std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>&
DistributedMutableVectorGroupedAggregateQuerySender::result() const noexcept {
  return result_;
}

common::Result<std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>
DistributedMutableVectorGroupedAggregateQuerySender::take_result() {
  if (state_ != DistributedQuerySenderState::kSucceeded || !result_.has_value()) {
    return common::make_unexpected(
        unavailable("mutable grouped vector query result is unavailable"));
  }
  auto result = std::move(*result_);
  result_.reset();
  return result;
}

} // namespace chronos::cluster
