#include "chronos/network/native_query_retry.hpp"

#include <algorithm>
#include <memory>
#include <new>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] NativeClientConfig client_config(const ConnectionBufferConfig& buffers) {
  return {.buffers = buffers,
          .maximum_in_flight_requests = 1U,
          .minimum_protocol_major = kProtocolV2Major,
          .maximum_protocol_major = kProtocolV2Major,
          .maximum_protocol_minor = kProtocolV2LatestMinor,
          .requested_feature_bits = kProtocolV2LeaderRedirectFeature};
}

[[nodiscard]] common::Status validate_limits(const NativeQueryRetryLimits& limits) {
  if (limits.query_result.protocol.maximum_payload_size == 0U ||
      limits.query_result.maximum_rows == 0U || limits.query_result.maximum_columns == 0U ||
      limits.query_result.maximum_column_name_bytes == 0U || limits.maximum_result_rows == 0U ||
      limits.maximum_result_batches == 0U || limits.maximum_result_payload_bytes == 0U) {
    return invalid("native query retry limits must be nonzero");
  }
  if (limits.maximum_result_batches > 65'536U)
    return invalid("native query retry batch limit is too large");
  return common::Status::ok();
}

[[nodiscard]] common::Status server_failure(const ProtocolErrorCode code) {
  switch (code) {
  case ProtocolErrorCode::kOverloaded:
  case ProtocolErrorCode::kUnknownRequest:
    return {common::StatusCode::kUnavailable, "native query server rejected the attempt"};
  case ProtocolErrorCode::kCancelled:
    return {common::StatusCode::kCancelled, "native query attempt was cancelled"};
  case ProtocolErrorCode::kUnauthorized:
    return {common::StatusCode::kUnauthenticated, "native query attempt was not authorized"};
  case ProtocolErrorCode::kMalformedFrame:
  case ProtocolErrorCode::kUnsupportedVersion:
  case ProtocolErrorCode::kInvalidState:
  case ProtocolErrorCode::kDuplicateRequest:
  case ProtocolErrorCode::kInvalidRequest:
    return invalid("native query server rejected the request");
  case ProtocolErrorCode::kExecutionFailure:
  case ProtocolErrorCode::kInternal:
    return {common::StatusCode::kInternal, "native query server execution failed"};
  }
  return {common::StatusCode::kInternal, "native query server error is unknown"};
}

} // namespace

class NativeQueryRetry::Impl {
public:
  Impl(NativeLeaderRedirectRouter owned_router, ConnectionBufferConfig configured_buffers,
       NativeQueryRetryLimits configured_limits, std::string exact_sql) noexcept
      : router(std::move(owned_router)), buffers(configured_buffers), limits(configured_limits),
        sql(std::move(exact_sql)) {}

  [[nodiscard]] common::Result<NativeClientSession> make_attempt() const {
    auto created = NativeClientSession::create(client_config(buffers));
    if (!created.has_value())
      return common::make_unexpected(created.error());
    if (const common::Status status = created->queue_handshake(); !status.is_ok())
      return common::make_unexpected(status);
    return created;
  }

  [[nodiscard]] common::Status fail(common::Status status) {
    if (state == NativeQueryRetryState::kRunning) {
      state = NativeQueryRetryState::kFailed;
      failure_status = std::move(status);
      result.reset();
      if (client.has_value())
        client->close();
    }
    return failure_status;
  }

  [[nodiscard]] NativeLeaderRoute current_route() const noexcept {
    const auto found = std::ranges::lower_bound(router.routes(), router.current_node_id(), {},
                                                &NativeLeaderRoute::node_id);
    return *found;
  }

  NativeLeaderRedirectRouter router;
  ConnectionBufferConfig buffers;
  NativeQueryRetryLimits limits;
  std::string sql;
  std::optional<NativeClientSession> client;
  std::optional<std::uint64_t> request_id;
  std::optional<NativeQueryResult> result{std::in_place};
  NativeQueryRetryState state{NativeQueryRetryState::kRunning};
  std::size_t attempts_started{};
  common::Status failure_status{common::StatusCode::kInternal, "native query retry has not failed"};
};

NativeQueryRetry::NativeQueryRetry(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

NativeQueryRetry::~NativeQueryRetry() = default;
NativeQueryRetry::NativeQueryRetry(NativeQueryRetry&&) noexcept = default;
NativeQueryRetry& NativeQueryRetry::operator=(NativeQueryRetry&&) noexcept = default;

common::Result<NativeQueryRetry> NativeQueryRetry::create(NativeQueryRetryConfig config,
                                                          std::string sql) {
  if (const common::Status status = validate_limits(config.limits); !status.is_ok())
    return common::make_unexpected(status);
  auto validated = encode_query_request(sql, config.buffers.protocol);
  if (!validated.has_value())
    return common::make_unexpected(validated.error());
  auto router = NativeLeaderRedirectRouter::create(std::move(config.routing));
  if (!router.has_value())
    return common::make_unexpected(router.error());
  try {
    auto implementation =
        std::make_unique<Impl>(std::move(*router), config.buffers, config.limits, std::move(sql));
    NativeQueryResult* result = optional_pointer(implementation->result);
    if (result == nullptr) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "native query retry result initialization failed"});
    }
    result->encoded_batches.reserve(config.limits.maximum_result_batches);
    auto attempt = implementation->make_attempt();
    if (!attempt.has_value())
      return common::make_unexpected(attempt.error());
    implementation->client.emplace(std::move(*attempt));
    implementation->attempts_started = 1U;
    return NativeQueryRetry{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("native query retry allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("native query retry limits exceed container bounds"));
  }
}

common::ByteView NativeQueryRetry::pending_write() const noexcept {
  if (!implementation_ || implementation_->state != NativeQueryRetryState::kRunning)
    return {};
  const NativeClientSession* client = optional_pointer(implementation_->client);
  return client != nullptr ? client->pending_write() : common::ByteView{};
}

common::Status NativeQueryRetry::consume_written(const std::size_t bytes) {
  if (!implementation_ || implementation_->state != NativeQueryRetryState::kRunning)
    return invalid("native query retry is not writable");
  NativeClientSession* client = optional_pointer(implementation_->client);
  return client != nullptr ? client->consume_written(bytes)
                           : invalid("native query retry has no client session");
}

common::Result<NativeQueryRetryProgress> NativeQueryRetry::receive(const common::ByteView bytes) {
  if (!implementation_ || implementation_->state != NativeQueryRetryState::kRunning)
    return common::make_unexpected(invalid("native query retry is not receiving"));
  Impl& impl = *implementation_;
  NativeClientSession* client = optional_pointer(impl.client);
  if (client == nullptr)
    return common::make_unexpected(impl.fail(invalid("native query retry has no client")));
  auto frames = client->receive(bytes);
  if (!frames.has_value())
    return common::make_unexpected(impl.fail(frames.error()));

  NativeQueryRetryProgress progress{.attempt_number = impl.attempts_started};
  try {
    for (const Frame& frame : *frames) {
      if (frame.header.message_type == MessageType::kServerHello && !impl.request_id.has_value()) {
        if ((client->negotiated_feature_bits() & kProtocolV2LeaderRedirectFeature) == 0U) {
          return common::make_unexpected(
              impl.fail(invalid("native query redirect feature was not negotiated")));
        }
        auto request = client->queue_query(impl.sql);
        if (!request.has_value())
          return common::make_unexpected(impl.fail(request.error()));
        impl.request_id = *request;
        continue;
      }
      if (!impl.request_id.has_value() || frame.header.request_id != *impl.request_id)
        continue;
      if (frame.header.message_type == MessageType::kQueryResult) {
        NativeQueryResult* result = optional_pointer(impl.result);
        if (result == nullptr)
          return common::make_unexpected(impl.fail(invalid("native query result is missing")));
        QueryResultLimits decode_limits = impl.limits.query_result;
        decode_limits.protocol.maximum_payload_size = std::min(
            decode_limits.protocol.maximum_payload_size, client->negotiated_maximum_payload_size());
        auto batch = decode_query_result_batch(frame.payload, decode_limits);
        if (!batch.has_value())
          return common::make_unexpected(impl.fail(batch.error()));
        if (result->encoded_batches.size() >= impl.limits.maximum_result_batches)
          return common::make_unexpected(
              impl.fail(exhausted("native query result batch limit exceeded")));
        if (batch->row_count() > impl.limits.maximum_result_rows - result->row_count)
          return common::make_unexpected(
              impl.fail(exhausted("native query result row limit exceeded")));
        if (frame.payload.size() > impl.limits.maximum_result_payload_bytes - result->payload_bytes)
          return common::make_unexpected(
              impl.fail(exhausted("native query result byte limit exceeded")));
        result->row_count += batch->row_count();
        result->payload_bytes += frame.payload.size();
        result->encoded_batches.emplace_back(frame.payload.begin(), frame.payload.end());
        progress.result_batches_received = result->encoded_batches.size();
        continue;
      }
      if (frame.header.message_type == MessageType::kQueryEnd) {
        impl.state = NativeQueryRetryState::kComplete;
        progress.completed = true;
        if (const NativeQueryResult* result = optional_pointer(impl.result); result != nullptr)
          progress.result_batches_received = result->encoded_batches.size();
        return progress;
      }
      if (frame.header.message_type == MessageType::kLeaderRedirect) {
        const NativeQueryResult* result = optional_pointer(impl.result);
        if (result == nullptr || !result->encoded_batches.empty())
          return common::make_unexpected(
              impl.fail(invalid("native query redirect followed result output")));
        auto redirect = decode_leader_redirect(frame.payload);
        if (!redirect.has_value())
          return common::make_unexpected(impl.fail(redirect.error()));
        auto candidate = impl.make_attempt();
        if (!candidate.has_value())
          return common::make_unexpected(impl.fail(candidate.error()));
        auto target = impl.router.accept(*redirect);
        if (!target.has_value())
          return common::make_unexpected(impl.fail(target.error()));
        impl.client.emplace(std::move(*candidate));
        impl.request_id.reset();
        ++impl.attempts_started;
        progress.reconnect_required = true;
        progress.attempt_number = impl.attempts_started;
        return progress;
      }
      if (frame.header.message_type == MessageType::kError) {
        auto error = decode_error_message(frame.payload, impl.buffers.protocol);
        return common::make_unexpected(
            impl.fail(error.has_value() ? server_failure(error->code) : error.error()));
      }
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(impl.fail(exhausted("native query result allocation failed")));
  } catch (const std::length_error&) {
    return common::make_unexpected(impl.fail(exhausted("native query result exceeds bounds")));
  }
  if (const NativeQueryResult* result = optional_pointer(impl.result); result != nullptr)
    progress.result_batches_received = result->encoded_batches.size();
  return progress;
}

NativeQueryRetryState NativeQueryRetry::state() const noexcept {
  return implementation_ ? implementation_->state : NativeQueryRetryState::kFailed;
}

NativeLeaderRoute NativeQueryRetry::current_route() const noexcept {
  return implementation_ ? implementation_->current_route() : NativeLeaderRoute{};
}

std::size_t NativeQueryRetry::attempts_started() const noexcept {
  return implementation_ ? implementation_->attempts_started : 0U;
}

const std::optional<NativeQueryResult>& NativeQueryRetry::result() const noexcept {
  static const std::optional<NativeQueryResult> empty;
  if (!implementation_ || implementation_->state != NativeQueryRetryState::kComplete)
    return empty;
  return implementation_->result;
}

const common::Status& NativeQueryRetry::failure() const {
  static const common::Status empty{common::StatusCode::kInternal, "native query retry is empty"};
  return implementation_ ? implementation_->failure_status : empty;
}

} // namespace chronos::network
