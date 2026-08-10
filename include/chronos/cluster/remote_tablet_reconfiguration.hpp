#ifndef CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_
#define CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kRemoteTabletReconfigurationHeaderSize = 80U;
inline constexpr std::size_t kRemoteTabletReconfigurationTrailerSize = 4U;
inline constexpr std::size_t kMaximumRemoteTabletReconfigurationRequestSize =
    kRemoteTabletReconfigurationHeaderSize + raft::kMaximumTabletReconfigurationActionSize +
    kRemoteTabletReconfigurationTrailerSize;

struct RemoteTabletReconfigurationCodecLimits {
  std::size_t maximum_request_bytes{kMaximumRemoteTabletReconfigurationRequestSize};
  raft::TabletReconfigurationActionCodecLimits action;
};

struct RemoteTabletReconfigurationRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::Term required_leader_term{};
  raft::TabletReconfigurationAction action;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_remote_tablet_reconfiguration_request_v1(const RemoteTabletReconfigurationRequest& request,
                                                RemoteTabletReconfigurationCodecLimits limits = {});
[[nodiscard]] common::Result<RemoteTabletReconfigurationRequest>
decode_remote_tablet_reconfiguration_request_v1(common::ByteView bytes,
                                                RemoteTabletReconfigurationCodecLimits limits = {});

// Embedding-owned authorization policy connecting the network authenticator's stable principal to
// an exact cluster node identity. It must outlive the receiver and provide its own synchronization.
class ClusterNodePrincipalAuthorizer {
public:
  virtual ~ClusterNodePrincipalAuthorizer() = default;
  [[nodiscard]] virtual common::Result<bool> authorize_node(std::uint64_t principal_id,
                                                            raft::NodeId claimed_node_id) const = 0;
};

struct RemoteTabletReconfigurationReceiverConfig {
  raft::NodeId local_node_id{};
  schema::TabletId tablet_id;
  raft::GroupId tablet_group_id;
  raft::GroupId metadata_group_id;
  const ClusterNodePrincipalAuthorizer* authorizer{};
  raft::TabletReconfigurationActionLedger* action_ledger{};
  raft::AsyncDurableMultiRaftRuntime* runtime{};
  RemoteTabletReconfigurationCodecLimits codec_limits;
};

struct RemoteTabletReconfigurationAdmission {
  RemoteTabletReconfigurationAdmission(
      raft::TabletReconfigurationActionId admitted_action_id, bool action_already_prepared,
      raft::AsyncDurableRaftCompletion admitted_completion) noexcept
      : action_id(admitted_action_id), already_prepared(action_already_prepared),
        completion(std::move(admitted_completion)) {}

  raft::TabletReconfigurationActionId action_id;
  bool already_prepared{};
  raft::AsyncDurableRaftCompletion completion;
};

// One tablet's authenticated receiver. All configured dependencies are borrowed and must outlive
// it. receive() authenticates source identity before ledger mutation, verifies the target/tablet/
// group binding, durably prepares the exact action, and nonblockingly admits it behind an atomic
// current-leader-term fence. A successful completion is still local durability, not quorum commit.
class RemoteTabletReconfigurationReceiver {
public:
  RemoteTabletReconfigurationReceiver() = delete;
  ~RemoteTabletReconfigurationReceiver() = default;
  RemoteTabletReconfigurationReceiver(const RemoteTabletReconfigurationReceiver&) = delete;
  RemoteTabletReconfigurationReceiver&
  operator=(const RemoteTabletReconfigurationReceiver&) = delete;
  RemoteTabletReconfigurationReceiver(RemoteTabletReconfigurationReceiver&&) noexcept = default;
  RemoteTabletReconfigurationReceiver&
  operator=(RemoteTabletReconfigurationReceiver&&) noexcept = default;

  [[nodiscard]] static common::Result<RemoteTabletReconfigurationReceiver>
  create(RemoteTabletReconfigurationReceiverConfig config);

  [[nodiscard]] common::Result<RemoteTabletReconfigurationAdmission>
  try_receive(common::ByteView request_bytes,
              const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit RemoteTabletReconfigurationReceiver(
      RemoteTabletReconfigurationReceiverConfig config) noexcept;
  RemoteTabletReconfigurationReceiverConfig config_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_REMOTE_TABLET_RECONFIGURATION_HPP_
