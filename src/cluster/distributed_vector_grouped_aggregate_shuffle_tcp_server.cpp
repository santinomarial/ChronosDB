#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_server.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status poll_error(const int error = errno) {
  return {common::StatusCode::kIoError,
          std::string("polling grouped shuffle TCP server: ") +
              std::error_code(error, std::generic_category()).message()};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      DistributedVectorGroupedAggregateShuffleTlsServer::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateShuffleTlsLimits& limits) noexcept {
  return valid_timeout(limits.handshake_timeout) && valid_timeout(limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(limits.stream);
}

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

} // namespace

class DistributedVectorGroupedAggregateShuffleTcpServer::Impl {
public:
  struct Connection {
    Connection(network::TcpSocket socket, DistributedVectorGroupedAggregateShuffleTlsServer carrier,
               const std::size_t completion_slot) noexcept
        : socket_(std::move(socket)), carrier_(std::move(carrier)),
          completion_slot_(completion_slot) {}

    // Destruction is reverse declaration order: TLS carrier before its borrowed descriptor.
    network::TcpSocket socket_;
    DistributedVectorGroupedAggregateShuffleTlsServer carrier_;
    std::size_t completion_slot_{};
  };

  Impl(DistributedVectorGroupedAggregateShuffleTcpServerConfig config,
       network::TcpListener listener, network::TlsServerContext tls_context,
       std::vector<std::unique_ptr<Connection>> connections, std::vector<pollfd> poll_descriptors,
       std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteStream>> slots,
       std::vector<std::size_t> free_slots, std::vector<std::size_t> completed_order) noexcept
      : config_(std::move(config)), listener_(std::move(listener)),
        tls_context_(std::move(tls_context)), connections_(std::move(connections)),
        poll_descriptors_(std::move(poll_descriptors)), completion_slots_(std::move(slots)),
        free_slots_(std::move(free_slots)), completed_order_(std::move(completed_order)) {}

  void remove_failed_connection(const std::size_t index) {
    free_slots_.push_back(connections_[index]->completion_slot_);
    connections_.erase(connections_.begin() + static_cast<std::ptrdiff_t>(index));
    increment_saturated(metrics_.failed_connections);
    metrics_.active_connections = connections_.size();
  }

  void retain_completed_connection(const std::size_t index) {
    const std::size_t slot = connections_[index]->completion_slot_;
    auto stream = connections_[index]->carrier_.take_complete_stream();
    if (!stream.has_value()) {
      remove_failed_connection(index);
      return;
    }
    completion_slots_[slot].emplace(std::move(*stream));
    completed_order_[(completed_head_ + completed_count_) % completed_order_.size()] = slot;
    ++completed_count_;
    connections_.erase(connections_.begin() + static_cast<std::ptrdiff_t>(index));
    increment_saturated(metrics_.completed_connections);
    metrics_.active_connections = connections_.size();
    metrics_.retained_streams = completed_count_;
  }

  void reject_connection() noexcept {
    increment_saturated(metrics_.rejected_connections);
  }

  void accept_ready(const std::chrono::steady_clock::time_point now) {
    for (std::size_t admitted = 0U; admitted < config_.maximum_accepts_per_poll; ++admitted) {
      auto next = listener_.accept_one();
      if (!next.has_value()) {
        increment_saturated(metrics_.accept_errors);
        return;
      }
      if (!next->has_value())
        return;
      if (free_slots_.empty()) {
        reject_connection();
        continue;
      }
      // The nested optional was checked above and is immediately consumed once.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      network::TcpSocket socket = std::move(next->value());
      const auto peer = socket.peer_endpoint();
      if (!peer.has_value()) {
        reject_connection();
        continue;
      }
      auto tls = network::TlsSocket::accept(tls_context_, socket.descriptor());
      if (!tls.has_value()) {
        reject_connection();
        continue;
      }
      auto carrier = DistributedVectorGroupedAggregateShuffleTlsServer::create(
          std::move(*tls), config_.resources,
          {.authenticator = config_.authenticator,
           .node_authorizer = config_.node_authorizer,
           .authority = config_.authority,
           .local_node_id = config_.local_node_id,
           .peer_ipv4_address = peer->address,
           .limits = config_.carrier_limits},
          now);
      if (!carrier.has_value()) {
        reject_connection();
        continue;
      }
      const std::size_t slot = free_slots_.back();
      try {
        connections_.emplace_back(
            std::make_unique<Connection>(std::move(socket), std::move(*carrier), slot));
      } catch (const std::bad_alloc&) {
        reject_connection();
        continue;
      }
      free_slots_.pop_back();
      increment_saturated(metrics_.accepted_connections);
      metrics_.active_connections = connections_.size();
    }
  }

  DistributedVectorGroupedAggregateShuffleTcpServerConfig config_;
  network::TcpListener listener_;
  network::TlsServerContext tls_context_;
  std::vector<std::unique_ptr<Connection>> connections_;
  std::vector<pollfd> poll_descriptors_;
  std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteStream>>
      completion_slots_;
  std::vector<std::size_t> free_slots_;
  std::vector<std::size_t> completed_order_;
  std::size_t completed_head_{};
  std::size_t completed_count_{};
  DistributedVectorGroupedAggregateShuffleTcpServerMetrics metrics_;
  bool running_{true};
};

DistributedVectorGroupedAggregateShuffleTcpServer::
    DistributedVectorGroupedAggregateShuffleTcpServer() noexcept = default;
DistributedVectorGroupedAggregateShuffleTcpServer::
    DistributedVectorGroupedAggregateShuffleTcpServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleTcpServer::
    ~DistributedVectorGroupedAggregateShuffleTcpServer() = default;
DistributedVectorGroupedAggregateShuffleTcpServer::
    DistributedVectorGroupedAggregateShuffleTcpServer(
        DistributedVectorGroupedAggregateShuffleTcpServer&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleTcpServer&
DistributedVectorGroupedAggregateShuffleTcpServer::operator=(
    DistributedVectorGroupedAggregateShuffleTcpServer&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleTcpServer>
DistributedVectorGroupedAggregateShuffleTcpServer::start(
    DistributedVectorGroupedAggregateShuffleTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.authority == nullptr || config.local_node_id == 0U ||
      config.maximum_retained_streams == 0U || config.maximum_retained_streams > 65536U ||
      config.maximum_accepts_per_poll == 0U || config.maximum_accepts_per_poll > 1024U ||
      !valid_limits(config.carrier_limits)) {
    return common::make_unexpected(invalid("grouped shuffle TCP server configuration is invalid"));
  }
  auto context = network::TlsServerContext::create(config.tls);
  if (!context.has_value())
    return common::make_unexpected(context.error());
  auto listener = network::TcpListener::bind(config.listener);
  if (!listener.has_value())
    return common::make_unexpected(listener.error());
  try {
    std::vector<std::unique_ptr<Impl::Connection>> connections;
    connections.reserve(config.maximum_retained_streams);
    std::vector<pollfd> descriptors(config.maximum_retained_streams + 1U);
    std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteStream>> slots(
        config.maximum_retained_streams);
    std::vector<std::size_t> free_slots;
    free_slots.reserve(config.maximum_retained_streams);
    for (std::size_t slot = 0U; slot < config.maximum_retained_streams; ++slot)
      free_slots.push_back(config.maximum_retained_streams - slot - 1U);
    std::vector<std::size_t> completed_order(config.maximum_retained_streams);
    return DistributedVectorGroupedAggregateShuffleTcpServer{
        std::make_unique<Impl>(std::move(config), std::move(*listener), std::move(*context),
                               std::move(connections), std::move(descriptors), std::move(slots),
                               std::move(free_slots), std::move(completed_order))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle TCP server allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle TCP server limits are too large"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_ || !implementation_->running_)
    return invalid("grouped shuffle TCP server is not running");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle TCP poll timeout is invalid");
  Impl& impl = *implementation_;
  impl.poll_descriptors_[0] = {.fd = impl.listener_.descriptor(), .events = POLLIN};
  for (std::size_t index = 0U; index < impl.connections_.size(); ++index) {
    const auto interest = impl.connections_[index]->carrier_.interest();
    short events{};
    if (interest.want_read)
      events |= POLLIN;
    if (interest.want_write)
      events |= POLLOUT;
    impl.poll_descriptors_[index + 1U] = {.fd = impl.connections_[index]->socket_.descriptor(),
                                          .events = events};
  }
  const nfds_t count = static_cast<nfds_t>(impl.connections_.size() + 1U);
  const auto before_poll = std::chrono::steady_clock::now();
  std::chrono::milliseconds bounded_wait = maximum_wait;
  for (const auto& connection : impl.connections_) {
    const auto deadline = connection->carrier_.deadline();
    if (deadline <= before_poll) {
      bounded_wait = std::chrono::milliseconds{0};
      break;
    }
    bounded_wait =
        std::min(bounded_wait,
                 std::chrono::duration_cast<std::chrono::milliseconds>(deadline - before_poll));
  }
  const int ready =
      ::poll(impl.poll_descriptors_.data(), count, static_cast<int>(bounded_wait.count()));
  if (ready < 0 && errno != EINTR)
    return poll_error();
  const auto now = std::chrono::steady_clock::now();
  for (std::size_t remaining = impl.connections_.size(); remaining > 0U; --remaining) {
    const std::size_t index = remaining - 1U;
    const short events = impl.poll_descriptors_[index + 1U].revents;
    const bool readable = (events & POLLIN) != 0;
    const bool writable = (events & POLLOUT) != 0;
    if ((events & (POLLERR | POLLHUP | POLLNVAL)) != 0 && !readable && !writable) {
      impl.remove_failed_connection(index);
      continue;
    }
    const common::Status status =
        impl.connections_[index]->carrier_.on_ready(readable, writable, now);
    const auto state = impl.connections_[index]->carrier_.state();
    if (!status.is_ok() || state == DistributedVectorGroupedAggregateShuffleTlsState::kFailed)
      impl.remove_failed_connection(index);
    else if (state == DistributedVectorGroupedAggregateShuffleTlsState::kComplete)
      impl.retain_completed_connection(index);
  }
  if (ready > 0 && (impl.poll_descriptors_[0].revents & POLLIN) != 0)
    impl.accept_ready(now);
  return common::Status::ok();
}

common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
DistributedVectorGroupedAggregateShuffleTcpServer::take_next_complete_stream() {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle TCP server is empty"));
  Impl& impl = *implementation_;
  if (impl.completed_count_ == 0U)
    return common::make_unexpected(
        unavailable("grouped shuffle TCP complete stream is unavailable"));
  const std::size_t slot = impl.completed_order_[impl.completed_head_];
  // The ring count and slot lifecycle guarantee this optional is populated exactly once.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  auto stream = std::move(*impl.completion_slots_[slot]);
  impl.completion_slots_[slot].reset();
  impl.completed_head_ = (impl.completed_head_ + 1U) % impl.completed_order_.size();
  --impl.completed_count_;
  impl.free_slots_.push_back(slot);
  impl.metrics_.retained_streams = impl.completed_count_;
  return stream;
}

common::Status DistributedVectorGroupedAggregateShuffleTcpServer::shutdown() {
  if (!implementation_ || !implementation_->running_)
    return common::Status::ok();
  Impl& impl = *implementation_;
  impl.connections_.clear();
  impl.metrics_.active_connections = 0U;
  const common::Status closed = impl.listener_.close();
  impl.running_ = false;
  return closed;
}

network::Ipv4Endpoint
DistributedVectorGroupedAggregateShuffleTcpServer::bound_endpoint() const noexcept {
  return implementation_ ? implementation_->listener_.bound_endpoint() : network::Ipv4Endpoint{};
}

DistributedVectorGroupedAggregateShuffleTcpServerMetrics
DistributedVectorGroupedAggregateShuffleTcpServer::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleTcpServerMetrics{};
}

bool DistributedVectorGroupedAggregateShuffleTcpServer::is_running() const noexcept {
  return implementation_ && implementation_->running_;
}

} // namespace chronos::cluster
