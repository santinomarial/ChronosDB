#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_POOL_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_POOL_HPP_

#include "chronos/cluster/raft_transport_receiver.hpp"
#include "chronos/cluster/raft_transport_tls_client.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct RaftTransportPeerPoolLimits {
  std::size_t maximum_peers{256U};
  raft::RaftTransportCodecLimits codec;
};
struct RaftTransportFailedPeer {
  std::optional<network::TcpSocket> socket;
  raft::NodeId peer_node_id{};
  RaftTransportTlsClient carrier;
  std::vector<std::vector<std::byte>> retry_frames;
};

// Production outbound ownership keeps the TCP descriptor alive until after its borrowing TLS
// carrier is destroyed. Tests and embeddings with a separate descriptor owner may use add_peer().
struct RaftTransportConnectedPeer {
  network::TcpSocket socket;
  RaftTransportTlsClient carrier;
};

// Single-event-loop fixed-capacity map of exact-peer outbound TLS carriers. Routing preflights all
// destinations and aggregate queue capacity. Failed removal returns complete reconnect retry bytes.
class RaftTransportPeerPool {
public:
  using TimePoint = RaftTransportTlsClient::TimePoint;
  RaftTransportPeerPool() = delete;
  ~RaftTransportPeerPool();
  RaftTransportPeerPool(const RaftTransportPeerPool&) = delete;
  RaftTransportPeerPool& operator=(const RaftTransportPeerPool&) = delete;
  RaftTransportPeerPool(RaftTransportPeerPool&&) noexcept;
  RaftTransportPeerPool& operator=(RaftTransportPeerPool&&) noexcept;
  [[nodiscard]] static common::Result<RaftTransportPeerPool>
  create(raft::NodeId local_node_id, RaftTransportPeerPoolLimits limits = {});
  [[nodiscard]] common::Status add_peer(RaftTransportTlsClient&& carrier);
  [[nodiscard]] common::Status add_connected_peer(RaftTransportConnectedPeer&& peer);
  [[nodiscard]] common::Status route_result(const raft::GroupId& group_id,
                                            const raft::DurableRaftResult& result, TimePoint now);
  [[nodiscard]] common::Status on_ready(raft::NodeId peer_node_id, bool readable, bool writable,
                                        TimePoint now);
  [[nodiscard]] common::Result<RaftTransportFailedPeer> take_failed_peer(raft::NodeId peer_node_id);
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] std::size_t peer_count() const noexcept;
  [[nodiscard]] RaftTransportTlsClient* find_peer(raft::NodeId peer_node_id) noexcept;
  [[nodiscard]] const RaftTransportTlsClient* find_peer(raft::NodeId peer_node_id) const noexcept;
  [[nodiscard]] int peer_descriptor(raft::NodeId peer_node_id) const noexcept;

private:
  class Impl;
  explicit RaftTransportPeerPool(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};
} // namespace chronos::cluster
#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_POOL_HPP_
