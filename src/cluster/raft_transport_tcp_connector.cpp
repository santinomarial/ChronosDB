#include "chronos/cluster/raft_transport_tcp_connector.hpp"

#include <limits>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      RaftTransportTcpConnector::TimePoint::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] RaftTransportTcpConnector::TimePoint
deadline_after(const RaftTransportTcpConnector::TimePoint now,
               const std::chrono::milliseconds timeout) noexcept {
  const auto duration =
      std::chrono::duration_cast<RaftTransportTcpConnector::TimePoint::duration>(timeout);
  return now > RaftTransportTcpConnector::TimePoint::max() - duration
             ? RaftTransportTcpConnector::TimePoint::max()
             : now + duration;
}

} // namespace

class RaftTransportTcpConnector::Impl {
public:
  Impl(network::TcpSocket owned_socket, std::vector<std::vector<std::byte>> frames,
       RaftTransportTcpConnectorConfig configured, const TimePoint now) noexcept
      : socket(std::move(owned_socket)), retry_frames(std::move(frames)), config(configured),
        deadline(deadline_after(now, config.connect_timeout)) {}

  [[nodiscard]] common::Status fail(common::Status failed) {
    if (connector_state != RaftTransportTcpConnectorState::kFailed) {
      carrier.reset();
      static_cast<void>(socket.close());
      connector_failure = std::move(failed);
      connector_state = RaftTransportTcpConnectorState::kFailed;
    }
    return connector_failure;
  }

  // Socket precedes carrier so reverse destruction releases TLS before its borrowed descriptor.
  network::TcpSocket socket;
  std::vector<std::vector<std::byte>> retry_frames;
  RaftTransportTcpConnectorConfig config;
  TimePoint deadline{};
  std::optional<RaftTransportTlsClient> carrier;
  RaftTransportTcpConnectorState connector_state{RaftTransportTcpConnectorState::kConnecting};
  common::Status connector_failure{common::StatusCode::kInternal,
                                   "Raft TCP connector has not failed"};
};

RaftTransportTcpConnector::RaftTransportTcpConnector(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RaftTransportTcpConnector::~RaftTransportTcpConnector() = default;
RaftTransportTcpConnector::RaftTransportTcpConnector(RaftTransportTcpConnector&&) noexcept =
    default;
RaftTransportTcpConnector&
RaftTransportTcpConnector::operator=(RaftTransportTcpConnector&&) noexcept = default;

common::Result<RaftTransportTcpConnector>
RaftTransportTcpConnector::begin(std::vector<std::vector<std::byte>>&& retry_frames,
                                 const RaftTransportTcpConnectorConfig config,
                                 const TimePoint now) {
  const common::Status valid = validate_config(config);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  const auto& limits = config.carrier.limits;
  if (retry_frames.size() > limits.maximum_queued_frames)
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "Raft TCP retry frame count exceeds carrier capacity"));
  try {
    std::size_t bytes{};
    for (const std::vector<std::byte>& frame : retry_frames) {
      if (frame.size() > std::numeric_limits<std::size_t>::max() - bytes)
        return common::make_unexpected(
            status(common::StatusCode::kResourceExhausted, "Raft TCP retry bytes overflow"));
      bytes += frame.size();
      auto envelope = raft::decode_raft_transport_envelope_v1(frame, limits.codec);
      if (!envelope.has_value())
        return common::make_unexpected(envelope.error());
      if (envelope->source != config.carrier.local_node_id ||
          envelope->destination != config.carrier.peer_node_id)
        return common::make_unexpected(
            status(common::StatusCode::kInvalidArgument,
                   "Raft TCP retry frame route differs from connector ownership"));
    }
    if (bytes > limits.maximum_queued_bytes)
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "Raft TCP retry bytes exceed carrier capacity"));
    auto socket = network::TcpSocket::begin_connect(config.remote_endpoint);
    if (!socket.has_value())
      return common::make_unexpected(socket.error());
    return RaftTransportTcpConnector{
        std::make_unique<Impl>(std::move(*socket), std::move(retry_frames), config, now)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "Raft TCP connector allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "Raft TCP connector exceeds container limits"));
  }
}

common::Status
RaftTransportTcpConnector::validate_config(const RaftTransportTcpConnectorConfig config) {
  const auto& limits = config.carrier.limits;
  if (config.tls_context == nullptr || config.carrier.local_node_id == 0U ||
      config.carrier.peer_node_id == 0U ||
      config.carrier.local_node_id == config.carrier.peer_node_id ||
      config.carrier.authenticator == nullptr || config.carrier.node_authorizer == nullptr ||
      config.carrier.peer_ipv4_address != config.remote_endpoint.address ||
      !valid_timeout(config.connect_timeout) || !valid_timeout(limits.handshake_timeout) ||
      !valid_timeout(limits.frame_write_timeout) || limits.maximum_queued_frames == 0U ||
      limits.maximum_queued_bytes <
          raft::kRaftTransportHeaderSize + raft::kRaftTransportTrailerSize)
    return status(common::StatusCode::kInvalidArgument,
                  "Raft TCP connector configuration is invalid");
  auto valid_codec = raft::RaftTransportFrameReader::create(limits.codec);
  return valid_codec.has_value() ? common::Status::ok() : valid_codec.error();
}

common::Status RaftTransportTcpConnector::on_ready(const bool writable, const TimePoint now) {
  if (!implementation_)
    return status(common::StatusCode::kInvalidArgument, "Raft TCP connector is empty");
  Impl& impl = *implementation_;
  if (impl.connector_state == RaftTransportTcpConnectorState::kFailed)
    return impl.connector_failure;
  if (impl.connector_state != RaftTransportTcpConnectorState::kConnecting)
    return common::Status::ok();
  if (now >= impl.deadline)
    return impl.fail(status(common::StatusCode::kUnavailable, "Raft TCP connect timed out"));
  if (!writable)
    return common::Status::ok();
  auto connected = impl.socket.finish_connect();
  if (!connected.has_value())
    return impl.fail(connected.error());
  if (*connected == network::TcpConnectState::kInProgress)
    return common::Status::ok();
  auto tls = network::TlsSocket::connect(*impl.config.tls_context, impl.socket.descriptor());
  if (!tls.has_value())
    return impl.fail(tls.error());
  auto carrier = RaftTransportTlsClient::create(std::move(*tls), impl.config.carrier, now);
  if (!carrier.has_value())
    return impl.fail(carrier.error());
  common::Status queued = carrier->try_enqueue_prevalidated_batch(impl.retry_frames, now);
  if (!queued.is_ok())
    return impl.fail(
        status(common::StatusCode::kCorruption, "Raft TCP retry batch changed after validation"));
  impl.retry_frames.clear();
  impl.carrier.emplace(std::move(*carrier));
  impl.connector_state = RaftTransportTcpConnectorState::kCarrierReady;
  return common::Status::ok();
}

common::Result<RaftTransportConnectedPeer> RaftTransportTcpConnector::take_connected_peer() {
  if (!implementation_ ||
      implementation_->connector_state != RaftTransportTcpConnectorState::kCarrierReady)
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "Raft TCP connected peer is not ready"));
  Impl& impl = *implementation_;
  RaftTransportConnectedPeer peer{std::move(impl.socket), std::move(*impl.carrier)};
  impl.carrier.reset();
  impl.connector_state = RaftTransportTcpConnectorState::kTaken;
  return peer;
}

common::Result<std::vector<std::vector<std::byte>>> RaftTransportTcpConnector::take_retry_frames() {
  if (!implementation_ ||
      implementation_->connector_state != RaftTransportTcpConnectorState::kFailed)
    return common::make_unexpected(status(common::StatusCode::kUnavailable,
                                          "Raft TCP retry frames require a failed connector"));
  return std::move(implementation_->retry_frames);
}

RaftTransportTcpConnectorState RaftTransportTcpConnector::state() const noexcept {
  return implementation_ ? implementation_->connector_state
                         : RaftTransportTcpConnectorState::kFailed;
}
bool RaftTransportTcpConnector::wants_write() const noexcept {
  return implementation_ &&
         implementation_->connector_state == RaftTransportTcpConnectorState::kConnecting;
}
int RaftTransportTcpConnector::descriptor() const noexcept {
  return implementation_ ? implementation_->socket.descriptor() : -1;
}
std::size_t RaftTransportTcpConnector::retry_frame_count() const noexcept {
  return implementation_ ? implementation_->retry_frames.size() : 0U;
}
const common::Status& RaftTransportTcpConnector::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "Raft TCP connector is empty"};
  return implementation_ ? implementation_->connector_failure : empty;
}

} // namespace chronos::cluster
