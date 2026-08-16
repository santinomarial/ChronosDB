#include "chronos/live/subscription_service.hpp"

#include "chronos/live/subscription_protocol.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <map>
#include <new>
#include <optional>
#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] network::ProtocolErrorCode wire_error(const common::StatusCode code) noexcept {
  switch (code) {
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kOutOfRange:
  case common::StatusCode::kAlreadyExists:
  case common::StatusCode::kNotSupported:
    return network::ProtocolErrorCode::kInvalidRequest;
  case common::StatusCode::kNotFound:
    return network::ProtocolErrorCode::kUnknownRequest;
  case common::StatusCode::kResourceExhausted:
    return network::ProtocolErrorCode::kOverloaded;
  case common::StatusCode::kCancelled:
    return network::ProtocolErrorCode::kCancelled;
  case common::StatusCode::kUnauthenticated:
    return network::ProtocolErrorCode::kUnauthorized;
  case common::StatusCode::kCorruption:
  case common::StatusCode::kIoError:
  case common::StatusCode::kUnavailable:
    return network::ProtocolErrorCode::kExecutionFailure;
  case common::StatusCode::kInternal:
  case common::StatusCode::kOk:
    return network::ProtocolErrorCode::kInternal;
  }
  return network::ProtocolErrorCode::kInternal;
}

} // namespace

class SubscriptionService::Impl {
public:
  struct Key {
    std::uint64_t connection_id{};
    std::uint64_t request_id{};

    friend auto operator<=>(const Key&, const Key&) = default;
  };

  enum class Phase : std::uint8_t { kSnapshot, kResumeEnd, kResumeReady, kLive };

  struct Active {
    common::Uuid subscription_id;
    std::uint64_t principal_id{};
    network::NetworkTaskProtocolContext protocol;
    Phase phase{Phase::kSnapshot};
    std::optional<MultiTabletSnapshotSubscription> snapshot;
    std::vector<std::byte> resume_token;
    std::uint64_t last_enqueued_delivery{};
  };

  explicit Impl(SubscriptionServiceConfig configured) noexcept : config(std::move(configured)) {}

  ~Impl() {
    for (const auto& [key, state] : active) {
      static_cast<void>(key);
      config.owner->abandon(state.subscription_id);
    }
  }

  [[nodiscard]] static network::NetworkTask
  task(const Key& key, const std::uint64_t principal_id,
       const network::NetworkTaskProtocolContext& protocol, const network::MessageType type,
       std::vector<std::byte> payload, const std::uint32_t flags = 0U) {
    return {.connection_id = key.connection_id,
            .principal_id = principal_id,
            .protocol = protocol,
            .frame = {.header = {.protocol_major = protocol.protocol_major,
                                 .protocol_minor = protocol.protocol_minor,
                                 .message_type = type,
                                 .flags = flags,
                                 .request_id = key.request_id},
                      .payload = std::move(payload)}};
  }

  [[nodiscard]] network::SubscriptionMessageLimits
  subscription_limits(const network::NetworkTaskProtocolContext& protocol) const noexcept {
    network::SubscriptionMessageLimits limits = config.snapshot_limits.subscription;
    limits.protocol.maximum_payload_size =
        std::min(limits.protocol.maximum_payload_size, protocol.maximum_payload_size);
    return limits;
  }

  [[nodiscard]] network::QueryResultLimits
  result_limits(const network::NetworkTaskProtocolContext& protocol) const noexcept {
    network::QueryResultLimits limits = config.snapshot_limits.result;
    limits.protocol.maximum_payload_size =
        std::min(limits.protocol.maximum_payload_size, protocol.maximum_payload_size);
    return limits;
  }

  [[nodiscard]] SnapshotSubscriptionLimits
  snapshot_limits(const network::NetworkTaskProtocolContext& protocol) const noexcept {
    SnapshotSubscriptionLimits limits = config.snapshot_limits;
    limits.subscription = subscription_limits(protocol);
    limits.result = result_limits(protocol);
    return limits;
  }

  [[nodiscard]] static network::SubscriptionProtocolContext
  subscription_context(const network::NetworkTaskProtocolContext& protocol) noexcept {
    return {.protocol_major = protocol.protocol_major,
            .protocol_minor = protocol.protocol_minor,
            .feature_bits = protocol.feature_bits};
  }

  [[nodiscard]] static bool
  valid_subscription_protocol(const network::NetworkTask& request) noexcept {
    const network::NetworkTaskProtocolContext& protocol = request.protocol;
    const bool version =
        (protocol.protocol_major == network::kProtocolMajor && protocol.protocol_minor >= 1U &&
         protocol.protocol_minor <= network::kProtocolLatestMinor) ||
        (protocol.protocol_major == network::kProtocolV2Major &&
         protocol.protocol_minor <= network::kProtocolV2LatestMinor);
    const std::uint64_t supported_features = protocol.protocol_major == network::kProtocolV2Major
                                                 ? network::kProtocolV2SupportedFeatureBits
                                                 : network::kProtocolV1SupportedFeatureBits;
    return version && protocol.maximum_payload_size != 0U &&
           protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
           (protocol.feature_bits & ~supported_features) == 0U &&
           (protocol.feature_bits & network::kProtocolV1SubscriptionFeature) != 0U &&
           request.frame.header.protocol_major == protocol.protocol_major &&
           request.frame.header.protocol_minor == protocol.protocol_minor;
  }

  [[nodiscard]] common::Status publish(network::NetworkTask response) {
    if (pending_response.has_value())
      return common::Status{common::StatusCode::kInternal,
                            "subscription service already retains a response"};
    if (config.responses->try_push_preserving(response))
      return common::Status::ok();
    try {
      pending_response.emplace(std::move(response));
      ++stats.response_backpressure;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return exhausted("subscription response retention allocation failed");
    }
  }

  [[nodiscard]] common::Status publish_error(const network::NetworkTask& request,
                                             const common::Status& cause) {
    ++stats.request_errors;
    auto payload = network::encode_error_message(wire_error(cause.code()), cause.message(),
                                                 subscription_limits(request.protocol).protocol);
    if (!payload.has_value())
      return payload.error();
    return publish(task({request.connection_id, request.frame.header.request_id},
                        request.principal_id, request.protocol, network::MessageType::kError,
                        std::move(*payload)));
  }

  [[nodiscard]] common::Result<std::vector<std::byte>>
  empty_snapshot(const network::NetworkTaskProtocolContext& protocol) const {
    try {
      std::vector<network::QueryResultColumn> columns;
      columns.reserve(config.plan->columns().size());
      for (const SnapshotSubscriptionColumn& column : config.plan->columns())
        columns.push_back({column.name, column.type, column.nullable});
      return network::encode_query_result_batch(0U, columns, {}, result_limits(protocol));
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("subscription snapshot-end allocation failed"));
    }
  }

  void erase(const Key& key) noexcept {
    active.erase(key);
    stats.active_subscriptions = active.size();
  }

  [[nodiscard]] common::Status reject_active(const Key& key, const network::NetworkTask& request,
                                             const common::Status& cause) {
    const auto found = active.find(key);
    if (found != active.end()) {
      config.owner->abandon(found->second.subscription_id);
      erase(key);
    }
    return publish_error(request, cause);
  }

  [[nodiscard]] common::Status accept_subscribe(network::NetworkTask request) {
    const Key key{request.connection_id, request.frame.header.request_id};
    if (key.connection_id == 0U || key.request_id == 0U)
      return publish_error(request, invalid("subscription network identity is invalid"));
    if (!accepting)
      return publish_error(request, common::Status{common::StatusCode::kUnavailable,
                                                   "subscription service is shutting down"});
    if (active.contains(key))
      return publish_error(request, invalid("subscription network identity is already active"));
    if (active.size() >= config.maximum_active_subscriptions)
      return publish_error(request, exhausted("subscription service capacity is exhausted"));
    const network::SubscriptionProtocolContext protocol = subscription_context(request.protocol);
    const bool has_raft_source =
        std::ranges::any_of(config.owner->source().members, [](const auto& member) {
          return member.source_kind == SubscriptionSourceKind::kRaft;
        });
    if (has_raft_source && !network::supports_source_tagged_subscription_changes(protocol))
      return publish_error(
          request, invalid("negotiated protocol cannot represent this subscription source set"));
    auto decoded = network::decode_subscription_request(request.frame.payload,
                                                        subscription_limits(request.protocol));
    if (!decoded.has_value())
      return publish_error(request, decoded.error());

    if (decoded->mode == network::SubscriptionStartMode::kNewQuery) {
      // The protocol decoder has already validated UTF-8. SQL parsing owns no pointer after return.
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      const std::string_view sql{reinterpret_cast<const char*>(decoded->body.data()),
                                 decoded->body.size()};
      auto prepared = prepare_subscription_plan(sql, config.catalog, config.plan_limits);
      if (!prepared.has_value())
        return publish_error(request, prepared.error().status());
      if (prepared->fingerprint() != config.plan->fingerprint() ||
          prepared->schema_ptr()->schema_id() != config.plan->schema_ptr()->schema_id() ||
          prepared->schema_ptr()->version() != config.plan->schema_ptr()->version())
        return publish_error(request,
                             invalid("subscription SQL does not match this durable coordinator"));
      auto snapshot = config.owner->start_snapshot(
          *config.plan, decoded->subscription_id, *config.resources, *config.storage,
          *config.publisher, *config.lineage, snapshot_limits(request.protocol));
      if (!snapshot.has_value())
        return publish_error(request, snapshot.error());
      try {
        active.emplace(key, Active{decoded->subscription_id,
                                   request.principal_id,
                                   request.protocol,
                                   Phase::kSnapshot,
                                   std::move(*snapshot),
                                   {},
                                   0U});
      } catch (const std::bad_alloc&) {
        config.owner->abandon(decoded->subscription_id);
        return publish_error(request,
                             exhausted("subscription service admission allocation failed"));
      }
      ++stats.accepted_new_subscriptions;
    } else {
      auto registration =
          config.owner->resume_subscription(decoded->subscription_id, decoded->body);
      if (!registration.has_value())
        return publish_error(request, registration.error());
      try {
        active.emplace(key, Active{decoded->subscription_id, request.principal_id, request.protocol,
                                   Phase::kResumeEnd, std::nullopt,
                                   std::move(registration->initial_resume_token), 0U});
      } catch (const std::bad_alloc&) {
        config.owner->abandon(decoded->subscription_id);
        return publish_error(request, exhausted("subscription service resume allocation failed"));
      }
      ++stats.resumed_subscriptions;
    }
    stats.active_subscriptions = active.size();
    return common::Status::ok();
  }

  [[nodiscard]] static network::SubscriptionEndReason reason_for(const SubscriptionPhase phase,
                                                                 const bool shutdown) noexcept {
    if (phase == SubscriptionPhase::kSchemaChanged)
      return network::SubscriptionEndReason::kSchemaChanged;
    if (phase == SubscriptionPhase::kOverflowed)
      return network::SubscriptionEndReason::kOverflowed;
    return shutdown ? network::SubscriptionEndReason::kServerShutdown
                    : network::SubscriptionEndReason::kCancelled;
  }

  [[nodiscard]] common::Status terminate(const Key& key,
                                         const network::SubscriptionEndReason reason) {
    const auto found = active.find(key);
    if (found == active.end())
      return common::Status{common::StatusCode::kNotFound,
                            "subscription service request is not active"};
    Active& state = found->second;
    const auto status = config.owner->status(state.subscription_id);
    if (!status.has_value()) {
      config.owner->abandon(state.subscription_id);
      erase(key);
      return status.error();
    }
    auto token = config.owner->cancel(state.subscription_id);
    if (!token.has_value()) {
      config.owner->abandon(state.subscription_id);
      erase(key);
      return token.error();
    }
    auto payload = network::encode_subscription_end(
        {.reason = reason,
         .safe_delivery_sequence = status->last_acknowledged_sequence,
         .resume_token = *token},
        subscription_limits(state.protocol));
    if (!payload.has_value()) {
      erase(key);
      return payload.error();
    }
    network::NetworkTask response =
        task(key, state.principal_id, state.protocol, network::MessageType::kSubscriptionEnd,
             std::move(*payload));
    erase(key);
    ++stats.terminal_responses;
    return publish(std::move(response));
  }

  [[nodiscard]] common::Status accept_acknowledgement(network::NetworkTask request) {
    const Key key{request.connection_id, request.frame.header.request_id};
    const auto found = active.find(key);
    if (found == active.end())
      return publish_error(request, common::Status{common::StatusCode::kNotFound,
                                                   "subscription acknowledgement is inactive"});
    if (request.protocol != found->second.protocol)
      return reject_active(key, request, invalid("subscription protocol context changed"));
    auto acknowledgement = network::decode_subscription_acknowledgement(request.frame.payload);
    if (!acknowledgement.has_value())
      return reject_active(key, request, acknowledgement.error());
    auto token = config.owner->acknowledge(found->second.subscription_id,
                                           acknowledgement->delivery_sequence);
    if (!token.has_value())
      return reject_active(key, request, token.error());
    auto payload = network::encode_subscription_checkpoint(
        {.acknowledged_delivery_sequence = acknowledgement->delivery_sequence,
         .resume_token = *token},
        subscription_limits(found->second.protocol));
    if (!payload.has_value())
      return reject_active(key, request, payload.error());
    ++stats.checkpoint_responses;
    return publish(task(key, request.principal_id, found->second.protocol,
                        network::MessageType::kSubscriptionCheckpoint, std::move(*payload)));
  }

  [[nodiscard]] common::Status accept_cancel(const network::NetworkTask& request) {
    const Key key{request.connection_id, request.frame.header.request_id};
    const auto found = active.find(key);
    if (found == active.end())
      return publish_error(request, common::Status{common::StatusCode::kNotFound,
                                                   "subscription cancellation is inactive"});
    if (request.protocol != found->second.protocol)
      return reject_active(key, request, invalid("subscription protocol context changed"));
    const auto status = config.owner->status(found->second.subscription_id);
    if (!status.has_value())
      return reject_active(key, request, status.error());
    const network::SubscriptionEndReason reason = reason_for(status->phase, false);
    const common::Status ended = terminate(key, reason);
    if (!ended.is_ok())
      return publish_error(request, ended);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status accept(network::NetworkTask request) {
    if (!valid_subscription_protocol(request))
      return invalid("subscription service task protocol context is invalid");
    switch (request.frame.header.message_type) {
    case network::MessageType::kSubscribeRequest:
      return accept_subscribe(std::move(request));
    case network::MessageType::kSubscriptionAcknowledge:
      return accept_acknowledgement(std::move(request));
    case network::MessageType::kCancel:
      return accept_cancel(request);
    default:
      return publish_error(request,
                           invalid("subscription service received an unrelated request type"));
    }
  }

  [[nodiscard]] common::Status progress(const Key& key) {
    auto found = active.find(key);
    if (found == active.end())
      return common::Status::ok();
    Active& state = found->second;
    if (shutting_down) {
      const auto status = config.owner->status(state.subscription_id);
      if (!status.has_value()) {
        config.owner->abandon(state.subscription_id);
        erase(key);
        return status.error();
      }
      return terminate(key, reason_for(status->phase, true));
    }

    const auto current_status = config.owner->status(state.subscription_id);
    if (!current_status.has_value())
      return reject_progress(key, current_status.error());
    if (current_status->phase == SubscriptionPhase::kSchemaChanged ||
        current_status->phase == SubscriptionPhase::kOverflowed)
      return terminate(key, reason_for(current_status->phase, false));

    if (state.phase == Phase::kSnapshot) {
      auto output = state.snapshot->next();
      if (!output.has_value()) {
        const common::Status failure = output.error();
        config.owner->abandon(state.subscription_id);
        const network::NetworkTask request{
            .connection_id = key.connection_id,
            .principal_id = state.principal_id,
            .protocol = state.protocol,
            .frame = {.header = {.protocol_major = state.protocol.protocol_major,
                                 .protocol_minor = state.protocol.protocol_minor,
                                 .request_id = key.request_id}}};
        erase(key);
        return publish_error(request, failure);
      }
      if (output->message_type == network::MessageType::kSubscriptionReady) {
        state.phase = Phase::kLive;
        state.snapshot.reset();
      }
      ++stats.snapshot_responses;
      return publish(task(key, state.principal_id, state.protocol, output->message_type,
                          std::move(output->payload), output->flags));
    }
    if (state.phase == Phase::kResumeEnd) {
      auto payload = empty_snapshot(state.protocol);
      if (!payload.has_value())
        return reject_progress(key, payload.error());
      state.phase = Phase::kResumeReady;
      ++stats.snapshot_responses;
      return publish(task(key, state.principal_id, state.protocol,
                          network::MessageType::kQueryResult, std::move(*payload),
                          network::kFrameFlagEndStream));
    }
    if (state.phase == Phase::kResumeReady) {
      auto payload = network::encode_subscription_ready(state.resume_token,
                                                        subscription_limits(state.protocol));
      if (!payload.has_value())
        return reject_progress(key, payload.error());
      state.phase = Phase::kLive;
      state.resume_token.clear();
      ++stats.snapshot_responses;
      return publish(task(key, state.principal_id, state.protocol,
                          network::MessageType::kSubscriptionReady, std::move(*payload)));
    }

    auto deliveries = config.owner->poll(state.subscription_id, config.maximum_live_poll_records);
    if (!deliveries.has_value())
      return reject_progress(key, deliveries.error());
    const auto delivery = std::ranges::find_if(*deliveries, [&](const DeliveryRecord& item) {
      return item.delivery_sequence > state.last_enqueued_delivery;
    });
    if (delivery == deliveries->end())
      return common::Status::ok();
    auto payload = encode_subscription_delivery(*delivery, subscription_context(state.protocol),
                                                subscription_limits(state.protocol));
    if (!payload.has_value())
      return reject_progress(key, payload.error());
    state.last_enqueued_delivery = delivery->delivery_sequence;
    ++stats.live_change_responses;
    return publish(task(key, state.principal_id, state.protocol,
                        network::MessageType::kSubscriptionChange, std::move(*payload)));
  }

  [[nodiscard]] common::Status reject_progress(const Key& key, const common::Status& cause) {
    const auto found = active.find(key);
    if (found == active.end())
      return cause;
    const network::NetworkTask request{
        .connection_id = key.connection_id,
        .principal_id = found->second.principal_id,
        .protocol = found->second.protocol,
        .frame = {.header = {.protocol_major = found->second.protocol.protocol_major,
                             .protocol_minor = found->second.protocol.protocol_minor,
                             .request_id = key.request_id}}};
    config.owner->abandon(found->second.subscription_id);
    erase(key);
    return publish_error(request, cause);
  }

  [[nodiscard]] common::Status poll_once() {
    if (pending_response.has_value()) {
      if (config.responses->try_push_preserving(*pending_response))
        pending_response.reset();
      return common::Status::ok();
    }
    if (auto request = config.requests->try_pop(); request.has_value())
      return accept(std::move(*request));
    if (active.empty())
      return common::Status::ok();
    auto next = cursor.has_value() ? active.upper_bound(*cursor) : active.begin();
    if (next == active.end())
      next = active.begin();
    const Key key = next->first;
    cursor = key;
    return progress(key);
  }

  SubscriptionServiceConfig config;
  std::map<Key, Active> active;
  std::optional<Key> cursor;
  std::optional<network::NetworkTask> pending_response;
  SubscriptionServiceMetrics stats;
  bool accepting{true};
  bool shutting_down{};
};

SubscriptionService::SubscriptionService(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SubscriptionService::~SubscriptionService() = default;
SubscriptionService::SubscriptionService(SubscriptionService&&) noexcept = default;
SubscriptionService& SubscriptionService::operator=(SubscriptionService&&) noexcept = default;

common::Result<SubscriptionService> SubscriptionService::create(SubscriptionServiceConfig config) {
  if (config.owner == nullptr || config.plan == nullptr || !config.catalog ||
      config.resources == nullptr || config.storage == nullptr || config.publisher == nullptr ||
      config.lineage == nullptr || config.requests == nullptr || config.responses == nullptr ||
      config.requests == config.responses || config.maximum_active_subscriptions == 0U ||
      config.maximum_live_poll_records == 0U || config.maximum_active_subscriptions > 1'048'576U ||
      config.maximum_live_poll_records > 1'048'576U)
    return common::make_unexpected(invalid("subscription service configuration is invalid"));
  const MultiTabletSubscriptionSource& source = config.owner->source();
  if (source.plan_fingerprint != config.plan->fingerprint() ||
      source.table_id != config.plan->schema_ptr()->table_id() ||
      source.schema_id != config.plan->schema_ptr()->schema_id() ||
      source.schema_version != config.plan->schema_ptr()->version() ||
      config.lineage->table_id() != source.table_id)
    return common::make_unexpected(
        invalid("subscription service plan, source, or schema lineage disagrees"));
  try {
    return SubscriptionService{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription service allocation failed"));
  }
}

common::Status SubscriptionService::poll_once() {
  return impl_->poll_once();
}

void SubscriptionService::begin_shutdown() noexcept {
  impl_->accepting = false;
  impl_->shutting_down = true;
}

bool SubscriptionService::drained() const noexcept {
  return impl_->active.empty() && !impl_->pending_response.has_value() &&
         impl_->config.requests->empty();
}

bool SubscriptionService::accepting() const noexcept {
  return impl_->accepting;
}

bool SubscriptionService::owns(const std::uint64_t connection_id,
                               const std::uint64_t request_id) const noexcept {
  return impl_->active.contains({connection_id, request_id});
}

SubscriptionServiceMetrics SubscriptionService::metrics() const noexcept {
  SubscriptionServiceMetrics value = impl_->stats;
  value.active_subscriptions = impl_->active.size();
  return value;
}

} // namespace chronos::live
