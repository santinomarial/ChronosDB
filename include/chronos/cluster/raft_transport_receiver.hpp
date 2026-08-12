#ifndef CHRONOS_CLUSTER_RAFT_TRANSPORT_RECEIVER_HPP_
#define CHRONOS_CLUSTER_RAFT_TRANSPORT_RECEIVER_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/transport_codec.hpp"

#include <cstddef>
#include <utility>
#include <vector>

namespace chronos::cluster {

struct RaftTransportReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  raft::AsyncDurableMultiRaftRuntime* runtime{};
  raft::RaftTransportCodecLimits codec_limits;
};

// Owns the asynchronous durable receive after authentication, authorization, and exact route
// checks succeed. The completion releases a transition only after the runtime's local persistence
// boundary. It may outlive the receiver, but not be consumed more than once.
struct RaftTransportAdmission {
  RaftTransportAdmission(raft::GroupId admitted_group_id, raft::NodeId admitted_source_node_id,
                         raft::AsyncDurableRaftCompletion admitted_completion) noexcept
      : group_id(admitted_group_id), source_node_id(admitted_source_node_id),
        completion(std::move(admitted_completion)) {}

  raft::GroupId group_id;
  raft::NodeId source_node_id{};
  raft::AsyncDurableRaftCompletion completion;
};

// Authenticated ingress boundary for one complete Raft Transport Envelope v1 frame. Dependencies
// are borrowed and must outlive the receiver. Calls may come from multiple producers because the
// asynchronous runtime owns serialization; the authorizer supplies its own synchronization.
class RaftTransportReceiver {
public:
  RaftTransportReceiver() = delete;
  RaftTransportReceiver(const RaftTransportReceiver&) = delete;
  RaftTransportReceiver& operator=(const RaftTransportReceiver&) = delete;
  RaftTransportReceiver(RaftTransportReceiver&&) noexcept = default;
  RaftTransportReceiver& operator=(RaftTransportReceiver&&) noexcept = default;

  [[nodiscard]] static common::Result<RaftTransportReceiver>
  create(RaftTransportReceiverConfig config);

  [[nodiscard]] common::Result<RaftTransportAdmission>
  try_receive(common::ByteView frame,
              const network::PeerAuthenticationResult& authenticated_peer) const;

private:
  explicit RaftTransportReceiver(RaftTransportReceiverConfig config) noexcept;
  RaftTransportReceiverConfig config_;
};

// Encodes every outbound message from one successful durable result without consuming it. On an
// encoding error the caller retains the complete transition for explicit retry or failure policy.
[[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
encode_durable_raft_outbound_v1(const raft::GroupId& expected_group_id, raft::NodeId local_node_id,
                                const raft::DurableRaftResult& durable_result,
                                raft::RaftTransportCodecLimits limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_TRANSPORT_RECEIVER_HPP_
