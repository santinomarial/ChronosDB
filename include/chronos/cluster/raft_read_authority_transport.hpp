#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_

#include "chronos/cluster/raft_observation_transport.hpp"
#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::array<std::byte, 8U> kRaftReadAuthorityRequestMagicV1{
    std::byte{'C'}, std::byte{'H'}, std::byte{'R'}, std::byte{'R'},
    std::byte{'A'}, std::byte{'U'}, std::byte{'Q'}, std::byte{'1'}};
inline constexpr std::size_t kRaftReadAuthorityRequestHeaderSize = 80U;
inline constexpr std::size_t kRaftReadAuthorityRequestSize = 84U;
inline constexpr std::size_t kRaftReadAuthorityResponseHeaderSize = 128U;
inline constexpr std::size_t kRaftReadAuthorityFrameTrailerSize = 4U;

struct RaftReadAuthorityTransportLimits {
  RaftObservationTransportLimits observation;
};

struct RaftReadAuthority {
  raft::GroupReadBarrier barrier;
  raft::RaftGroupObservation observation;

  friend bool operator==(const RaftReadAuthority&, const RaftReadAuthority&) = default;
};

struct RaftReadAuthorityRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};

  friend bool operator==(const RaftReadAuthorityRequest&,
                         const RaftReadAuthorityRequest&) = default;
};

struct RaftReadAuthorityResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<RaftReadAuthority> authority{std::nullopt};

  friend bool operator==(const RaftReadAuthorityResponse&,
                         const RaftReadAuthorityResponse&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_read_authority_request_v1(const RaftReadAuthorityRequest& request);
[[nodiscard]] common::Result<RaftReadAuthorityRequest>
decode_raft_read_authority_request_v1(common::ByteView bytes);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_read_authority_response_v1(const RaftReadAuthorityResponse& response,
                                       RaftReadAuthorityTransportLimits limits = {});
[[nodiscard]] common::Result<RaftReadAuthorityResponse>
decode_raft_read_authority_response_v1(common::ByteView bytes,
                                       RaftReadAuthorityTransportLimits limits = {});

// Header-only allocation gates for nonblocking stream carriers. Full checksum and semantic
// validation still occurs after the exact frame has arrived.
[[nodiscard]] common::Result<std::size_t>
raft_read_authority_request_frame_length_v1(common::ByteView header);
[[nodiscard]] common::Result<std::size_t>
raft_read_authority_response_frame_length_v1(common::ByteView header,
                                             RaftReadAuthorityTransportLimits limits = {});

struct RaftReadAuthorityRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<RaftReadAuthorityRequest> request{std::nullopt};
};

class RaftReadAuthorityRequestReader {
public:
  RaftReadAuthorityRequestReader() = default;
  RaftReadAuthorityRequestReader(const RaftReadAuthorityRequestReader&) = delete;
  RaftReadAuthorityRequestReader& operator=(const RaftReadAuthorityRequestReader&) = delete;
  RaftReadAuthorityRequestReader(RaftReadAuthorityRequestReader&&) = delete;
  RaftReadAuthorityRequestReader& operator=(RaftReadAuthorityRequestReader&&) = delete;

  [[nodiscard]] common::Result<RaftReadAuthorityRequestReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kRaftReadAuthorityRequestSize> frame_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

struct RaftReadAuthorityResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<RaftReadAuthorityResponse> response{std::nullopt};
};

class RaftReadAuthorityResponseReader {
public:
  RaftReadAuthorityResponseReader() = delete;
  RaftReadAuthorityResponseReader(const RaftReadAuthorityResponseReader&) = delete;
  RaftReadAuthorityResponseReader& operator=(const RaftReadAuthorityResponseReader&) = delete;
  RaftReadAuthorityResponseReader(RaftReadAuthorityResponseReader&& other) noexcept;
  RaftReadAuthorityResponseReader& operator=(RaftReadAuthorityResponseReader&& other) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityResponseReader>
  create(RaftReadAuthorityTransportLimits limits = {});
  [[nodiscard]] common::Result<RaftReadAuthorityResponseReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  explicit RaftReadAuthorityResponseReader(RaftReadAuthorityTransportLimits limits) noexcept;
  [[nodiscard]] common::Result<RaftReadAuthorityResponseReadStep> fail(common::Status status);
  void reset_frame() noexcept;

  RaftReadAuthorityTransportLimits limits_;
  std::array<std::byte, kRaftReadAuthorityResponseHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

// Owns one canonical request or response and exposes exactly its unwritten suffix. Moving transfers
// the only write obligation and leaves the source complete.
class RaftReadAuthorityFrameWriteCursor {
public:
  RaftReadAuthorityFrameWriteCursor() = delete;
  RaftReadAuthorityFrameWriteCursor(const RaftReadAuthorityFrameWriteCursor&) = delete;
  RaftReadAuthorityFrameWriteCursor& operator=(const RaftReadAuthorityFrameWriteCursor&) = delete;
  RaftReadAuthorityFrameWriteCursor(RaftReadAuthorityFrameWriteCursor&& other) noexcept;
  RaftReadAuthorityFrameWriteCursor& operator=(RaftReadAuthorityFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<RaftReadAuthorityFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame, RaftReadAuthorityTransportLimits limits = {});
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit RaftReadAuthorityFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

class RaftReadAuthorityService {
public:
  virtual ~RaftReadAuthorityService() = default;
  [[nodiscard]] virtual common::Result<RaftReadAuthority>
  acquire(const raft::GroupId& group_id) = 0;
};

struct RaftReadAuthorityReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  RaftReadAuthorityService* service{};
  RaftReadAuthorityTransportLimits limits{};
};

class RaftReadAuthorityReceiver {
public:
  RaftReadAuthorityReceiver() = delete;

  [[nodiscard]] static common::Result<RaftReadAuthorityReceiver>
  create(RaftReadAuthorityReceiverConfig config);

  // Authentication is checked before request decoding. The authenticated principal must then be
  // authorized for the claimed source and the request must target the configured local node before
  // the service may issue a linearizable barrier.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit RaftReadAuthorityReceiver(RaftReadAuthorityReceiverConfig config) noexcept;
  RaftReadAuthorityReceiverConfig config_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_
