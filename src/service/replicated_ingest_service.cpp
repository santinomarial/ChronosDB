#include "chronos/service/replicated_ingest_service.hpp"

#include "chronos/network/messages.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

void increment(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
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

class ReplicatedIngestService::Impl {
public:
  explicit Impl(ReplicatedIngestServiceConfig configured) noexcept
      : config(std::move(configured)) {}

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll> publish(network::NetworkTask response) {
    if (pending_response.has_value())
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "replicated ingest service already retains a response"});
    if (config.responses->try_push_preserving(response))
      return ReplicatedIngestServicePoll{.response_enqueued = true};
    try {
      pending_response.emplace(std::move(response));
      increment(stats.response_backpressure);
      return ReplicatedIngestServicePoll{};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("replicated ingest response retention allocation failed"));
    }
  }

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll> reject(network::NetworkTask request,
                                                                   const common::Status& cause) {
    increment(stats.request_errors);
    network::ProtocolLimits limits = config.protocol;
    limits.maximum_payload_size =
        std::min(limits.maximum_payload_size, request.protocol.maximum_payload_size);
    auto payload = network::encode_error_message(wire_error(cause.code()), cause.message(), limits);
    if (!payload.has_value())
      return common::make_unexpected(payload.error());
    request.frame = {.header = {.protocol_major = request.protocol.protocol_major,
                                .protocol_minor = request.protocol.protocol_minor,
                                .message_type = network::MessageType::kError,
                                .request_id = request.frame.header.request_id,
                                .payload_size = static_cast<std::uint32_t>(payload->size())},
                     .payload = std::move(*payload)};
    return publish(std::move(request));
  }

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll>
  accept(network::NetworkTask request, const std::chrono::steady_clock::time_point now) {
    increment(stats.consumed_requests);
    if (request.frame.header.message_type == network::MessageType::kCancel) {
      if (config.coordinator->cancel(request.connection_id, request.frame.header.request_id))
        increment(stats.cancelled_requests);
      return ReplicatedIngestServicePoll{};
    }
    if (request.frame.header.message_type != network::MessageType::kIngestRequest)
      return reject(std::move(request),
                    invalid("replicated ingest service received an unrelated request type"));
    if (!accepting) {
      increment(stats.shutdown_rejections);
      return reject(std::move(request),
                    common::Status{common::StatusCode::kUnavailable,
                                   "replicated ingest service is shutting down"});
    }
    network::NetworkTask response_shell{
        .connection_id = request.connection_id,
        .principal_id = request.principal_id,
        .protocol = request.protocol,
        .frame = {.header = {.request_id = request.frame.header.request_id}}};
    const common::Status admitted = config.coordinator->admit(std::move(request), now);
    if (!admitted.is_ok())
      return reject(std::move(response_shell), admitted);
    increment(stats.admitted_requests);
    return ReplicatedIngestServicePoll{};
  }

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll>
  poll_once(const std::chrono::steady_clock::time_point now) {
    if (pending_response.has_value()) {
      if (!config.responses->try_push_preserving(*pending_response))
        return ReplicatedIngestServicePoll{};
      pending_response.reset();
      return ReplicatedIngestServicePoll{.response_enqueued = true};
    }
    if (auto request = config.requests->try_pop(); request.has_value()) {
      auto accepted = accept(std::move(*request), now);
      if (!accepted.has_value() || accepted->response_enqueued || pending_response.has_value())
        return accepted;
    }
    auto completed = config.coordinator->poll(now);
    if (!completed.has_value())
      return common::make_unexpected(completed.error());
    if (!completed->has_value())
      return ReplicatedIngestServicePoll{};
    return publish(std::move(**completed));
  }

  ReplicatedIngestServiceConfig config;
  std::optional<network::NetworkTask> pending_response;
  ReplicatedIngestServiceMetrics stats;
  bool accepting{true};
};

ReplicatedIngestService::ReplicatedIngestService(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplicatedIngestService::~ReplicatedIngestService() = default;
ReplicatedIngestService::ReplicatedIngestService(ReplicatedIngestService&&) noexcept = default;
ReplicatedIngestService&
ReplicatedIngestService::operator=(ReplicatedIngestService&&) noexcept = default;

common::Result<ReplicatedIngestService>
ReplicatedIngestService::create(ReplicatedIngestServiceConfig config) {
  if (config.coordinator == nullptr || config.requests == nullptr || config.responses == nullptr ||
      config.requests == config.responses || config.protocol.maximum_payload_size == 0U)
    return common::make_unexpected(invalid("replicated ingest service configuration is invalid"));
  try {
    return ReplicatedIngestService{std::make_unique<Impl>(std::move(config))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest service allocation failed"));
  }
}

common::Result<ReplicatedIngestServicePoll>
ReplicatedIngestService::poll_once(const std::chrono::steady_clock::time_point now) {
  return impl_->poll_once(now);
}

void ReplicatedIngestService::begin_shutdown() noexcept {
  impl_->accepting = false;
}

bool ReplicatedIngestService::drained() const noexcept {
  return impl_->config.requests->empty() && !impl_->pending_response.has_value() &&
         impl_->config.coordinator->metrics().pending_requests == 0U;
}

bool ReplicatedIngestService::accepting() const noexcept {
  return impl_->accepting;
}

ReplicatedIngestServiceMetrics ReplicatedIngestService::metrics() const noexcept {
  ReplicatedIngestServiceMetrics value = impl_->stats;
  value.accepting = impl_->accepting;
  value.response_retained = impl_->pending_response.has_value();
  return value;
}

} // namespace chronos::service
