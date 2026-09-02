#ifndef CHRONOS_CLUSTER_RAFT_OBSERVATION_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_RAFT_OBSERVATION_TRANSPORT_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/types.hpp"

#include <array>
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
inline constexpr std::size_t kMaximumRaftObservationVotersPerSet = 4096U;

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
  std::optional<raft::RaftGroupObservation> observation{std::nullopt};

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

// Validate one complete fixed header before returning its exact bounded frame length. These are
// the allocation gates for stream carriers; trailer and payload integrity still require full
// decode.
[[nodiscard]] common::Result<std::size_t>
raft_observation_request_frame_length_v1(common::ByteView header);
[[nodiscard]] common::Result<std::size_t>
raft_observation_response_frame_length_v1(common::ByteView header,
                                          RaftObservationTransportLimits limits = {});

struct RaftObservationRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<RaftObservationRequest> request{std::nullopt};
};

class RaftObservationRequestReader {
public:
  RaftObservationRequestReader() = default;
  RaftObservationRequestReader(const RaftObservationRequestReader&) = delete;
  RaftObservationRequestReader& operator=(const RaftObservationRequestReader&) = delete;
  RaftObservationRequestReader(RaftObservationRequestReader&&) = delete;
  RaftObservationRequestReader& operator=(RaftObservationRequestReader&&) = delete;

  [[nodiscard]] common::Result<RaftObservationRequestReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kRaftObservationRequestSize> frame_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

struct RaftObservationResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<RaftObservationResponse> response{std::nullopt};
};

class RaftObservationResponseReader {
public:
  RaftObservationResponseReader() = delete;
  RaftObservationResponseReader(const RaftObservationResponseReader&) = delete;
  RaftObservationResponseReader& operator=(const RaftObservationResponseReader&) = delete;
  RaftObservationResponseReader(RaftObservationResponseReader&& other) noexcept;
  RaftObservationResponseReader& operator=(RaftObservationResponseReader&& other) noexcept;

  [[nodiscard]] static common::Result<RaftObservationResponseReader>
  create(RaftObservationTransportLimits limits = {});
  [[nodiscard]] common::Result<RaftObservationResponseReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  explicit RaftObservationResponseReader(RaftObservationTransportLimits limits) noexcept;
  [[nodiscard]] common::Result<RaftObservationResponseReadStep> fail(common::Status status);
  void reset_frame() noexcept;

  RaftObservationTransportLimits limits_;
  std::array<std::byte, kRaftObservationResponseHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

// Owns one fully validated request or response and exposes only its unwritten suffix. Moving
// transfers the sole write obligation and leaves the source complete.
class RaftObservationFrameWriteCursor {
public:
  RaftObservationFrameWriteCursor() = delete;
  RaftObservationFrameWriteCursor(const RaftObservationFrameWriteCursor&) = delete;
  RaftObservationFrameWriteCursor& operator=(const RaftObservationFrameWriteCursor&) = delete;
  RaftObservationFrameWriteCursor(RaftObservationFrameWriteCursor&& other) noexcept;
  RaftObservationFrameWriteCursor& operator=(RaftObservationFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<RaftObservationFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame, RaftObservationTransportLimits limits = {});
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit RaftObservationFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

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
  RaftObservationTransportLimits limits{};
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
