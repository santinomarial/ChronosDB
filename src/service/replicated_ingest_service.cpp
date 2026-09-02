#include "chronos/service/replicated_ingest_service.hpp"

#include "chronos/network/messages.hpp"
#include "chronos/service/native_protocol_service.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

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
  explicit Impl(ReplicatedIngestServiceConfig configured) noexcept : config(configured) {}

  class ActiveQuery {
  public:
    ActiveQuery(NativeQueryDispatcher& dispatcher, network::NetworkTask request) noexcept
        : dispatcher_(&dispatcher), connection_id_(request.connection_id),
          request_id_(request.frame.header.request_id), request_(std::move(request)) {}
    ~ActiveQuery() {
      cancel();
      join();
    }
    ActiveQuery(const ActiveQuery&) = delete;
    ActiveQuery& operator=(const ActiveQuery&) = delete;

    [[nodiscard]] static common::Result<std::unique_ptr<ActiveQuery>>
    start(NativeQueryDispatcher& dispatcher, network::NetworkTask request) {
      try {
        auto owner = std::make_unique<ActiveQuery>(dispatcher, std::move(request));
        owner->thread_ = std::thread{[query = owner.get()] { query->run(); }};
        return owner;
      } catch (const std::bad_alloc&) {
        return common::make_unexpected(exhausted("native query worker allocation failed"));
      } catch (const std::system_error&) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kUnavailable, "native query worker thread could not be started"});
      }
    }

    [[nodiscard]] bool matches(const network::NetworkTask& task) const noexcept {
      return task.connection_id == connection_id_ && task.frame.header.request_id == request_id_;
    }
    void cancel() noexcept {
      suppress_response_ = true;
      cancellation_.request_cancel();
    }
    [[nodiscard]] bool complete() const noexcept {
      return complete_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool response_suppressed() const noexcept {
      return suppress_response_;
    }
    void join() noexcept {
      if (thread_.joinable())
        thread_.join();
    }
    [[nodiscard]] common::Result<NativeProtocolResponseSequence> take_result() {
      join();
      if (allocation_failure_) {
        return common::make_unexpected(exhausted("native query worker allocation failed"));
      }
      if (unexpected_failure_ || !completion_.has_value()) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kInternal, "native query worker terminated unexpectedly"});
      }
      return std::move(*completion_);
    }

  private:
    void run() noexcept {
      try {
        completion_.emplace(dispatcher_->execute_query(std::move(request_), &cancellation_));
      } catch (const std::bad_alloc&) {
        allocation_failure_ = true;
      } catch (...) {
        unexpected_failure_ = true;
      }
      complete_.store(true, std::memory_order_release);
    }

    NativeQueryDispatcher* dispatcher_{};
    std::uint64_t connection_id_{};
    std::uint64_t request_id_{};
    network::NetworkTask request_;
    NativeQueryCancellation cancellation_;
    std::optional<common::Result<NativeProtocolResponseSequence>> completion_;
    bool suppress_response_{};
    bool allocation_failure_{};
    bool unexpected_failure_{};
    std::atomic<bool> complete_;
    std::thread thread_;
  };

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

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll> publish_sequence() {
    if (next_sequence_response >= pending_sequence.size())
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "replicated native response sequence is empty"});
    network::NetworkTask response = std::move(pending_sequence[next_sequence_response++]);
    if (next_sequence_response == pending_sequence.size()) {
      std::vector<network::NetworkTask>{}.swap(pending_sequence);
      next_sequence_response = 0U;
    }
    return publish(std::move(response));
  }

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll>
  accept(network::NetworkTask request, const std::chrono::steady_clock::time_point now) {
    increment(stats.consumed_requests);
    if (request.frame.header.message_type == network::MessageType::kCancel) {
      if (active_query != nullptr && active_query->matches(request)) {
        active_query->cancel();
        increment(stats.cancelled_requests);
      } else if (config.coordinator->cancel(request.connection_id,
                                            request.frame.header.request_id)) {
        increment(stats.cancelled_requests);
      }
      return ReplicatedIngestServicePoll{};
    }
    if (request.frame.header.message_type == network::MessageType::kQueryRequest) {
      if (!accepting) {
        increment(stats.shutdown_rejections);
        return reject(std::move(request),
                      common::Status{common::StatusCode::kUnavailable,
                                     "replicated native service is shutting down"});
      }
      if (config.queries == nullptr)
        return reject(std::move(request),
                      invalid("replicated native query service is not configured"));
      if (active_query != nullptr)
        return reject(std::move(request), exhausted("replicated native query slot is occupied"));
      auto started = ActiveQuery::start(*config.queries, std::move(request));
      if (!started.has_value())
        return common::make_unexpected(started.error());
      active_query = std::move(*started);
      increment(stats.query_requests);
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
        .frame = {.header = {.request_id = request.frame.header.request_id}, .payload = {}}};
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
    if (!pending_sequence.empty())
      return publish_sequence();
    if (auto request = config.requests->try_pop(); request.has_value()) {
      auto accepted = accept(std::move(*request), now);
      if (!accepted.has_value() || accepted->response_enqueued || pending_response.has_value())
        return accepted;
    }
    if (active_query != nullptr && active_query->complete()) {
      const bool suppress = active_query->response_suppressed();
      auto completed = active_query->take_result();
      active_query.reset();
      if (suppress)
        return ReplicatedIngestServicePoll{};
      if (!completed.has_value())
        return common::make_unexpected(completed.error());
      if (completed->responses.empty())
        return common::make_unexpected(common::Status{
            common::StatusCode::kInternal, "replicated native query returned no response"});
      pending_sequence = std::move(completed->responses);
      next_sequence_response = 0U;
      return publish_sequence();
    }
    auto completed = config.coordinator->poll(now);
    if (!completed.has_value())
      return common::make_unexpected(completed.error());
    if (!completed->has_value())
      return ReplicatedIngestServicePoll{};
    auto& response = *completed;
    if (!response.has_value())
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "replicated ingest completion is absent"});
    return publish(std::move(*response));
  }

  ReplicatedIngestServiceConfig config;
  std::optional<network::NetworkTask> pending_response;
  std::vector<network::NetworkTask> pending_sequence;
  std::size_t next_sequence_response{};
  std::unique_ptr<ActiveQuery> active_query;
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
    return ReplicatedIngestService{std::make_unique<Impl>(config)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest service allocation failed"));
  }
}

common::Result<ReplicatedIngestServicePoll>
ReplicatedIngestService::poll_once(const std::chrono::steady_clock::time_point now) {
  if (!impl_)
    return common::make_unexpected(invalid("replicated ingest service was moved from"));
  return impl_->poll_once(now);
}

void ReplicatedIngestService::begin_shutdown() noexcept {
  if (impl_) {
    impl_->accepting = false;
    if (impl_->active_query != nullptr)
      impl_->active_query->cancel();
  }
}

bool ReplicatedIngestService::drained() const noexcept {
  if (!impl_)
    return true;
  return impl_->config.requests->empty() && !impl_->pending_response.has_value() &&
         impl_->pending_sequence.empty() && impl_->active_query == nullptr &&
         impl_->config.coordinator->metrics().pending_requests == 0U;
}

bool ReplicatedIngestService::accepting() const noexcept {
  return impl_ && impl_->accepting;
}

ReplicatedIngestServiceMetrics ReplicatedIngestService::metrics() const noexcept {
  if (!impl_)
    return {};
  ReplicatedIngestServiceMetrics value = impl_->stats;
  value.accepting = impl_->accepting;
  value.response_retained = impl_->pending_response.has_value() || !impl_->pending_sequence.empty();
  value.query_active = impl_->active_query != nullptr;
  return value;
}

} // namespace chronos::service
