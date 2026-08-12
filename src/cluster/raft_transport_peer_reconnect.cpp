#include "chronos/cluster/raft_transport_peer_reconnect.hpp"

#include <algorithm>
#include <new>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {
[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}
[[nodiscard]] RaftTransportPeerReconnect::TimePoint
add(const RaftTransportPeerReconnect::TimePoint now,
    const std::chrono::milliseconds delay) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftTransportPeerReconnect::TimePoint::duration>(delay);
  return now > RaftTransportPeerReconnect::TimePoint::max() - duration
             ? RaftTransportPeerReconnect::TimePoint::max()
             : now + duration;
}
} // namespace

class RaftTransportPeerReconnect::Impl {
public:
  explicit Impl(RaftTransportPeerReconnectConfig configured) noexcept
      : config(configured), next_backoff(config.limits.initial_backoff) {}
  void schedule(const common::Status failure, const TimePoint now) {
    last = failure;
    next_attempt = add(now, next_backoff);
    const auto current = next_backoff.count();
    const auto maximum = config.limits.maximum_backoff.count();
    next_backoff = current > maximum / 2
                       ? config.limits.maximum_backoff
                       : std::min(next_backoff * 2, config.limits.maximum_backoff);
    state = RaftTransportPeerReconnectState::kBackoff;
  }
  RaftTransportPeerReconnectConfig config;
  std::vector<std::vector<std::byte>> retry_frames;
  std::optional<RaftTransportTcpConnector> connector;
  std::chrono::milliseconds next_backoff{};
  std::optional<TimePoint> next_attempt;
  std::size_t attempts{};
  RaftTransportPeerReconnectState state{RaftTransportPeerReconnectState::kReady};
  common::Status last{common::StatusCode::kInternal, "Raft peer reconnect has not failed"};
};

RaftTransportPeerReconnect::RaftTransportPeerReconnect(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportPeerReconnect::~RaftTransportPeerReconnect() = default;
RaftTransportPeerReconnect::RaftTransportPeerReconnect(RaftTransportPeerReconnect&&) noexcept =
    default;
RaftTransportPeerReconnect&
RaftTransportPeerReconnect::operator=(RaftTransportPeerReconnect&&) noexcept = default;

common::Result<RaftTransportPeerReconnect>
RaftTransportPeerReconnect::create(const RaftTransportPeerReconnectConfig config) {
  const common::Status valid = RaftTransportTcpConnector::validate_config(config.connector);
  const auto maximum =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  if (config.limits.initial_backoff.count() <= 0 ||
      config.limits.maximum_backoff < config.limits.initial_backoff ||
      config.limits.maximum_backoff > maximum)
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "Raft peer reconnect backoff is invalid"));
  try {
    return RaftTransportPeerReconnect{std::make_unique<Impl>(config)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft peer reconnect allocation failed"));
  }
}

common::Status RaftTransportPeerReconnect::drive(const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft peer reconnect is empty");
  Impl& impl = *implementation_;
  if (impl.state == RaftTransportPeerReconnectState::kConnecting ||
      impl.state == RaftTransportPeerReconnectState::kCarrierReady ||
      impl.state == RaftTransportPeerReconnectState::kConnected)
    return common::Status::ok();
  if (impl.next_attempt.has_value() && now < *impl.next_attempt)
    return common::Status::ok();
  auto connector =
      RaftTransportTcpConnector::begin(std::move(impl.retry_frames), impl.config.connector, now);
  if (!connector.has_value()) {
    impl.schedule(connector.error(), now);
    return connector.error();
  }
  impl.connector.emplace(std::move(*connector));
  impl.next_attempt.reset();
  ++impl.attempts;
  impl.state = RaftTransportPeerReconnectState::kConnecting;
  return common::Status::ok();
}

common::Status RaftTransportPeerReconnect::on_ready(const bool writable, const TimePoint now) {
  if (!implementation_ || implementation_->state != RaftTransportPeerReconnectState::kConnecting)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft peer reconnect has no connecting attempt");
  Impl& impl = *implementation_;
  const common::Status progress = impl.connector->on_ready(writable, now);
  if (!progress.is_ok()) {
    auto retry = impl.connector->take_retry_frames();
    if (!retry.has_value())
      return retry.error();
    impl.retry_frames = std::move(*retry);
    impl.connector.reset();
    impl.schedule(progress, now);
    return progress;
  }
  if (impl.connector->state() == RaftTransportTcpConnectorState::kCarrierReady)
    impl.state = RaftTransportPeerReconnectState::kCarrierReady;
  return common::Status::ok();
}

common::Result<RaftTransportConnectedPeer> RaftTransportPeerReconnect::take_connected_peer() {
  if (!implementation_ || implementation_->state != RaftTransportPeerReconnectState::kCarrierReady)
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "Raft peer reconnect carrier is not ready"));
  Impl& impl = *implementation_;
  auto peer = impl.connector->take_connected_peer();
  if (!peer.has_value())
    return common::make_unexpected(peer.error());
  impl.connector.reset();
  impl.next_backoff = impl.config.limits.initial_backoff;
  impl.next_attempt.reset();
  impl.state = RaftTransportPeerReconnectState::kConnected;
  return peer;
}

common::Status RaftTransportPeerReconnect::accept_failed_peer(RaftTransportFailedPeer&& failed,
                                                              const TimePoint now) {
  if (!implementation_ || implementation_->state != RaftTransportPeerReconnectState::kConnected)
    return status(common::StatusCode::kInvalidArgument, "Raft peer reconnect is not connected");
  Impl& impl = *implementation_;
  if (failed.peer_node_id != impl.config.connector.carrier.peer_node_id ||
      !failed.socket.has_value() || !failed.socket->valid())
    return status(common::StatusCode::kInvalidArgument,
                  "Raft failed peer differs from reconnect ownership");
  impl.retry_frames = std::move(failed.retry_frames);
  impl.schedule(failed.carrier.failure(), now);
  return common::Status::ok();
}

RaftTransportPeerReconnectState RaftTransportPeerReconnect::state() const noexcept {
  return implementation_ ? implementation_->state : RaftTransportPeerReconnectState::kBackoff;
}
std::optional<RaftTransportPeerReconnect::TimePoint>
RaftTransportPeerReconnect::next_attempt_not_before() const noexcept {
  return implementation_ ? implementation_->next_attempt : std::nullopt;
}
std::optional<RaftTransportPeerReconnect::TimePoint>
RaftTransportPeerReconnect::next_deadline() const noexcept {
  if (!implementation_)
    return std::nullopt;
  if (implementation_->state == RaftTransportPeerReconnectState::kConnecting &&
      implementation_->connector.has_value())
    return implementation_->connector->next_deadline();
  return implementation_->state == RaftTransportPeerReconnectState::kBackoff
             ? implementation_->next_attempt
             : std::nullopt;
}
std::size_t RaftTransportPeerReconnect::attempts_started() const noexcept {
  return implementation_ ? implementation_->attempts : 0U;
}
std::size_t RaftTransportPeerReconnect::retry_frame_count() const noexcept {
  return implementation_ ? implementation_->retry_frames.size() : 0U;
}
int RaftTransportPeerReconnect::descriptor() const noexcept {
  return implementation_ && implementation_->connector.has_value()
             ? implementation_->connector->descriptor()
             : -1;
}
bool RaftTransportPeerReconnect::wants_write() const noexcept {
  return implementation_ && implementation_->connector.has_value() &&
         implementation_->connector->wants_write();
}
const common::Status& RaftTransportPeerReconnect::last_failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft peer reconnect is empty"};
  return implementation_ ? implementation_->last : empty;
}

} // namespace chronos::cluster
