#include "chronos/service/replicated_ingest_coordinator.hpp"

#include "chronos/network/messages.hpp"
#include "chronos/service/replicated_ingest_operation.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

void increment(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

[[nodiscard]] network::ProtocolErrorCode protocol_error(const common::StatusCode code) noexcept {
  switch (code) {
  case common::StatusCode::kCancelled:
    return network::ProtocolErrorCode::kCancelled;
  case common::StatusCode::kResourceExhausted:
    return network::ProtocolErrorCode::kOverloaded;
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kOutOfRange:
  case common::StatusCode::kNotFound:
  case common::StatusCode::kAlreadyExists:
    return network::ProtocolErrorCode::kInvalidRequest;
  case common::StatusCode::kUnauthenticated:
    return network::ProtocolErrorCode::kUnauthorized;
  case common::StatusCode::kInternal:
  case common::StatusCode::kCorruption:
    return network::ProtocolErrorCode::kInternal;
  default:
    return network::ProtocolErrorCode::kExecutionFailure;
  }
}

} // namespace

class ReplicatedIngestCoordinator::Impl {
public:
  struct Pending {
    std::uint64_t connection_id{};
    std::uint64_t principal_id{};
    std::uint64_t request_id{};
    network::NetworkTaskProtocolContext protocol;
    std::chrono::steady_clock::time_point deadline;
    ReplicatedIngestOperation operation;
  };

  Impl(raft::AsyncDurableMultiRaftRuntime& configured_runtime,
       ingest::AsyncRaftTabletApplication& configured_application,
       const ReplicatedIngestCoordinatorLimits configured_limits) noexcept
      : runtime(&configured_runtime), application(&configured_application),
        limits(configured_limits) {}

  [[nodiscard]] common::Status admit(network::NetworkTask request, const raft::GroupId& group_id,
                                     const raft::Term term,
                                     const std::chrono::steady_clock::time_point now) {
    const auto reject = [&](common::Status status) {
      increment(stats.rejected_requests);
      return status;
    };
    if (request.connection_id == 0U || request.principal_id == 0U ||
        request.frame.header.message_type != network::MessageType::kIngestRequest ||
        request.frame.header.request_id == 0U ||
        request.frame.header.protocol_major != request.protocol.protocol_major ||
        request.frame.header.protocol_minor != request.protocol.protocol_minor ||
        request.frame.header.payload_size != request.frame.payload.size() ||
        request.protocol.maximum_payload_size == 0U ||
        request.frame.payload.size() > request.protocol.maximum_payload_size ||
        request.protocol.protocol_major != network::kProtocolV2Major ||
        (request.protocol.feature_bits & network::kProtocolV2QuorumSyncFeature) == 0U)
      return reject(invalid("replicated ingest request lacks negotiated protocol authority"));
    if (pending.size() >= limits.maximum_pending_requests)
      return reject(exhausted("replicated ingest coordinator capacity is full"));
    if (std::ranges::any_of(pending, [&](const Pending& item) {
          return item.connection_id == request.connection_id &&
                 item.request_id == request.frame.header.request_id;
        }))
      return reject(invalid("replicated ingest request identity is already pending"));
    const network::IngestProtocolContext context{.protocol_major = request.protocol.protocol_major,
                                                 .protocol_minor = request.protocol.protocol_minor,
                                                 .feature_bits = request.protocol.feature_bits};
    network::ProtocolLimits protocol_limits = limits.protocol;
    protocol_limits.maximum_payload_size =
        std::min(protocol_limits.maximum_payload_size, request.protocol.maximum_payload_size);
    auto envelope = network::decode_ingest_request(request.frame.payload, context, protocol_limits);
    if (!envelope.has_value() || envelope->durability != network::DurabilityMode::kQuorumSync)
      return reject(invalid("replicated ingest requires negotiated QUORUM_SYNC bytes"));
    try {
      std::vector<std::byte> command{envelope->encoded_columnar_append.begin(),
                                     envelope->encoded_columnar_append.end()};
      auto operation = ReplicatedIngestOperation::submit(
          group_id, term, std::move(command), *runtime, *application, limits.columnar_append);
      if (!operation.has_value())
        return reject(operation.error());
      pending.push_back({.connection_id = request.connection_id,
                         .principal_id = request.principal_id,
                         .request_id = request.frame.header.request_id,
                         .protocol = request.protocol,
                         .deadline = now + limits.request_timeout,
                         .operation = std::move(*operation)});
      stats.pending_requests = pending.size();
      stats.high_water_pending_requests =
          std::max(stats.high_water_pending_requests, stats.pending_requests);
      increment(stats.admitted_requests);
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return reject(exhausted("replicated ingest coordinator allocation failed"));
    } catch (const std::length_error&) {
      return reject(exhausted("replicated ingest coordinator exceeds container limits"));
    }
  }

  [[nodiscard]] common::Result<std::optional<network::NetworkTask>>
  poll(const std::chrono::steady_clock::time_point now) {
    if (pending.empty())
      return std::optional<network::NetworkTask>{};
    for (std::size_t checked = 0U; checked < pending.size(); ++checked) {
      cursor %= pending.size();
      Pending& item = pending[cursor];
      common::Status failure;
      std::optional<ReplicatedIngestResult> completed;
      if (now >= item.deadline) {
        failure = {common::StatusCode::kCancelled, "replicated ingest request timed out"};
        increment(stats.timed_out_requests);
      } else {
        auto result = item.operation.poll();
        if (!result.has_value())
          failure = result.error();
        else if (result->has_value())
          completed = std::move(**result);
        else {
          ++cursor;
          continue;
        }
      }
      common::Result<std::vector<std::byte>> payload =
          completed.has_value() ? encode_replicated_ingest_acknowledgement(*completed)
                                : network::encode_error_message(
                                      protocol_error(failure.code()), failure.message(),
                                      {.maximum_payload_size = item.protocol.maximum_payload_size});
      if (!payload.has_value()) {
        pending.erase(pending.begin() + static_cast<std::ptrdiff_t>(cursor));
        if (cursor == pending.size())
          cursor = 0U;
        stats.pending_requests = pending.size();
        return common::make_unexpected(payload.error());
      }
      const network::MessageType type = completed.has_value()
                                            ? network::MessageType::kQuorumSyncIngestAcknowledgement
                                            : network::MessageType::kError;
      network::NetworkTask response{
          .connection_id = item.connection_id,
          .principal_id = item.principal_id,
          .protocol = item.protocol,
          .frame = {.header = {.protocol_major = item.protocol.protocol_major,
                               .protocol_minor = item.protocol.protocol_minor,
                               .message_type = type,
                               .request_id = item.request_id,
                               .payload_size = static_cast<std::uint32_t>(payload->size())},
                    .payload = std::move(*payload)}};
      pending.erase(pending.begin() + static_cast<std::ptrdiff_t>(cursor));
      if (cursor == pending.size())
        cursor = 0U;
      stats.pending_requests = pending.size();
      increment(stats.completed_requests);
      return std::optional<network::NetworkTask>{std::move(response)};
    }
    return std::optional<network::NetworkTask>{};
  }

  raft::AsyncDurableMultiRaftRuntime* runtime;
  ingest::AsyncRaftTabletApplication* application;
  ReplicatedIngestCoordinatorLimits limits;
  std::vector<Pending> pending;
  std::size_t cursor{};
  ReplicatedIngestCoordinatorMetrics stats;
};

ReplicatedIngestCoordinator::ReplicatedIngestCoordinator(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedIngestCoordinator::~ReplicatedIngestCoordinator() = default;
ReplicatedIngestCoordinator::ReplicatedIngestCoordinator(ReplicatedIngestCoordinator&&) noexcept =
    default;
ReplicatedIngestCoordinator&
ReplicatedIngestCoordinator::operator=(ReplicatedIngestCoordinator&&) noexcept = default;

common::Result<ReplicatedIngestCoordinator>
ReplicatedIngestCoordinator::create(raft::AsyncDurableMultiRaftRuntime& runtime,
                                    ingest::AsyncRaftTabletApplication& application,
                                    const ReplicatedIngestCoordinatorLimits limits) {
  if (limits.maximum_pending_requests == 0U || limits.maximum_pending_requests > 65'536U ||
      limits.request_timeout <= std::chrono::milliseconds::zero() ||
      !runtime.owns_worker_extension(application))
    return common::make_unexpected(invalid("replicated ingest coordinator limits are invalid"));
  try {
    auto impl = std::make_unique<Impl>(runtime, application, limits);
    impl->pending.reserve(limits.maximum_pending_requests);
    return ReplicatedIngestCoordinator{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated ingest coordinator exceeds limits"));
  }
}

common::Status ReplicatedIngestCoordinator::admit(network::NetworkTask request,
                                                  const raft::GroupId group_id,
                                                  const raft::Term required_leader_term,
                                                  const std::chrono::steady_clock::time_point now) {
  return impl_->admit(std::move(request), group_id, required_leader_term, now);
}

bool ReplicatedIngestCoordinator::cancel(const std::uint64_t connection_id,
                                         const std::uint64_t request_id) noexcept {
  const auto found = std::ranges::find_if(impl_->pending, [&](const Impl::Pending& item) {
    return item.connection_id == connection_id && item.request_id == request_id;
  });
  if (found == impl_->pending.end())
    return false;
  const std::size_t offset = static_cast<std::size_t>(found - impl_->pending.begin());
  impl_->pending.erase(found);
  if (impl_->cursor > offset)
    --impl_->cursor;
  if (impl_->cursor == impl_->pending.size())
    impl_->cursor = 0U;
  impl_->stats.pending_requests = impl_->pending.size();
  increment(impl_->stats.cancelled_requests);
  return true;
}

common::Result<std::optional<network::NetworkTask>>
ReplicatedIngestCoordinator::poll(const std::chrono::steady_clock::time_point now) {
  return impl_->poll(now);
}

ReplicatedIngestCoordinatorMetrics ReplicatedIngestCoordinator::metrics() const noexcept {
  return impl_->stats;
}

} // namespace chronos::service
