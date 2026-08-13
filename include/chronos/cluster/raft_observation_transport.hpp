#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TRANSPORT_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kRaftObservationRequestHeaderSize = 80U;
inline constexpr std::size_t kRaftObservationRequestSize = 84U;
inline constexpr std::size_t kRaftObservationResponseHeaderSize = 96U;
inline constexpr std::size_t kRaftObservationPayloadHeaderSize = 72U;
inline constexpr std::size_t kRaftObservationFrameTrailerSize = 4U;

struct RaftObservationTransportLimits {
  std::size_t maximum_voters_per_set{31U};
};

struct RaftObservationRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};

  friend bool operator==(const RaftObservationRequest&, const RaftObservationRequest&) = default;
};

struct RaftObservationResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<raft::RaftGroupObservation> observation;

  friend bool operator==(const RaftObservationResponse&, const RaftObservationResponse&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_observation_request_v1(const RaftObservationRequest& request);
[[nodiscard]] common::Result<RaftObservationRequest>
decode_raft_observation_request_v1(common::ByteView bytes);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_observation_response_v1(const RaftObservationResponse& response,
                                    RaftObservationTransportLimits limits = {});
[[nodiscard]] common::Result<RaftObservationResponse>
decode_raft_observation_response_v1(common::ByteView bytes,
                                    RaftObservationTransportLimits limits = {});

// Embedding-owned, ordered local observation boundary. Implementations must obtain the observation
// through the node's single durable Raft owner and must not reconstruct it from cached scalars.
class RaftObservationService {
public:
  virtual ~RaftObservationService() = default;
  [[nodiscard]] virtual common::Result<raft::RaftGroupObservation>
  observe(const raft::GroupId& group_id) = 0;
};

struct RaftObservationReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  RaftObservationService* service{};
  RaftObservationTransportLimits limits;
};

class RaftObservationReceiver {
public:
  RaftObservationReceiver() = delete;

  [[nodiscard]] static common::Result<RaftObservationReceiver>
  create(RaftObservationReceiverConfig config);

  // Transport authentication precedes decoding. The authenticated principal is then authorized
  // for the claimed source, the target is exact-matched, and only then may the service run.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit RaftObservationReceiver(RaftObservationReceiverConfig config) noexcept;
  RaftObservationReceiverConfig config_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_OBSERVATION_TRANSPORT_HPP_
