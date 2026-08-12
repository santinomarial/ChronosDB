#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_MANAGER_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_MANAGER_HPP_

#include "chronos/cluster/raft_transport_peer_reconnect.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct RaftTransportPeerManagerConfig {
  raft::NodeId local_node_id{};
  std::vector<RaftTransportPeerReconnectConfig> peers;
  RaftTransportPeerPoolLimits pool;
};
struct RaftTransportPeerInterest {
  raft::NodeId peer_node_id{};
  int descriptor{-1};
  bool want_read{};
  bool want_write{};
};

// Fixed-route, single-event-loop outbound Raft transport owner. Fresh results remain caller-owned
// when any destination is disconnected or full; only complete failed frames enter reconnect state.
class RaftTransportPeerManager {
public:
  using TimePoint = RaftTransportPeerReconnect::TimePoint;
  RaftTransportPeerManager() = delete;
  ~RaftTransportPeerManager();
  RaftTransportPeerManager(const RaftTransportPeerManager&) = delete;
  RaftTransportPeerManager& operator=(const RaftTransportPeerManager&) = delete;
  RaftTransportPeerManager(RaftTransportPeerManager&&) noexcept;
  RaftTransportPeerManager& operator=(RaftTransportPeerManager&&) noexcept;
  [[nodiscard]] static common::Result<RaftTransportPeerManager>
  create(RaftTransportPeerManagerConfig config);
  [[nodiscard]] common::Status drive(TimePoint now);
  [[nodiscard]] common::Status on_ready(raft::NodeId peer_node_id, bool readable, bool writable,
                                        TimePoint now);
  [[nodiscard]] common::Status on_transport_closed(raft::NodeId peer_node_id, TimePoint now);
  [[nodiscard]] common::Status route_result(const raft::GroupId& group_id,
                                            const raft::DurableRaftResult& result, TimePoint now);
  [[nodiscard]] common::Result<std::vector<RaftTransportPeerInterest>> interests() const;
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] std::size_t configured_peer_count() const noexcept;
  [[nodiscard]] std::size_t connected_peer_count() const noexcept;

private:
  class Impl;
  explicit RaftTransportPeerManager(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster
#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_PEER_MANAGER_HPP_
