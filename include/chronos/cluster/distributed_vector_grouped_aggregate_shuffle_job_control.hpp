#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::cluster {

namespace distributed_vector_grouped_aggregate_shuffle_job_control_format {
inline constexpr std::array<std::byte, 8U> kRequestMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'G'}, std::byte{'J'}, std::byte{'C'}, std::byte{'1'}};
inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'G'}, std::byte{'J'}, std::byte{'R'}, std::byte{'1'}};
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 128U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength +
    distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength +
    query::distributed_vector_result_schema_format::kMaximumFrameLength + kTrailerLength;
inline constexpr std::chrono::milliseconds kMaximumExecutionTimeout{86'400'000};
inline constexpr std::size_t kResponseHeaderLength = 96U;
inline constexpr std::size_t kResponseFrameLength = kResponseHeaderLength + kTrailerLength;
} // namespace distributed_vector_grouped_aggregate_shuffle_job_control_format

namespace distributed_vector_grouped_aggregate_shuffle_job_control_v2_format {
inline constexpr std::array<std::byte, 8U> kRequestMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'G'}, std::byte{'J'}, std::byte{'C'}, std::byte{'2'}};
inline constexpr std::array<std::byte, 8U> kResponseMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'V'},
    std::byte{'G'}, std::byte{'J'}, std::byte{'R'}, std::byte{'2'}};
inline constexpr std::uint16_t kMajor = 2U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 128U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kRouteDescriptorLength = 16U;
inline constexpr std::size_t kMaximumRoutes = 4096U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + (kMaximumRoutes * kRouteDescriptorLength) + kTrailerLength;
inline constexpr std::size_t kResponseHeaderLength = 96U;
inline constexpr std::size_t kResponseFrameLength = kResponseHeaderLength + kTrailerLength;
} // namespace distributed_vector_grouped_aggregate_shuffle_job_control_v2_format

enum class DistributedVectorGroupedAggregateShuffleJobControlAction : std::uint8_t {
  kPrepare = 1,
  kSeal = 2,
  kInstallRoutes = 3,
};

struct DistributedVectorGroupedAggregateShuffleJobPrepare {
  raft::NodeId coordinator_node_id{};
  raft::NodeId target_node_id{};
  network::Ipv4Endpoint coordinator_result_endpoint;
  std::chrono::milliseconds execution_timeout{};
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  query::DistributedVectorResultSchema result_schema;
};

struct DistributedVectorGroupedAggregateShuffleJobSeal {
  common::Uuid query_id;
  raft::NodeId coordinator_node_id{};
  raft::NodeId target_node_id{};

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleJobSeal&,
                         const DistributedVectorGroupedAggregateShuffleJobSeal&) = default;
};

struct DistributedVectorGroupedAggregateShuffleJobRoute {
  raft::NodeId node_id{};
  network::Ipv4Endpoint endpoint;

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleJobRoute&,
                         const DistributedVectorGroupedAggregateShuffleJobRoute&) = default;
};

struct DistributedVectorGroupedAggregateShuffleJobInstallRoutes {
  common::Uuid query_id;
  raft::NodeId coordinator_node_id{};
  raft::NodeId target_node_id{};
  std::vector<DistributedVectorGroupedAggregateShuffleJobRoute> routes;

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleJobInstallRoutes&,
                         const DistributedVectorGroupedAggregateShuffleJobInstallRoutes&) = default;
};

using DistributedVectorGroupedAggregateShuffleJobControlRequest =
    std::variant<DistributedVectorGroupedAggregateShuffleJobPrepare,
                 DistributedVectorGroupedAggregateShuffleJobSeal,
                 DistributedVectorGroupedAggregateShuffleJobInstallRoutes>;

struct DistributedVectorGroupedAggregateShuffleJobControlResponse {
  DistributedVectorGroupedAggregateShuffleJobControlAction action{
      DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare};
  common::StatusCode status_code{common::StatusCode::kInternal};
  common::Uuid query_id;
  raft::NodeId coordinator_node_id{};
  raft::NodeId target_node_id{};
  std::optional<network::Ipv4Endpoint> reducer_shuffle_endpoint;

  friend bool
  operator==(const DistributedVectorGroupedAggregateShuffleJobControlResponse&,
             const DistributedVectorGroupedAggregateShuffleJobControlResponse&) = default;
};

struct DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumFrameLength};
  std::chrono::milliseconds maximum_execution_timeout{
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumExecutionTimeout};
  DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits authority;
  query::DistributedVectorResultSchemaDecodeLimits result_schema;
  std::size_t maximum_routes{
      distributed_vector_grouped_aggregate_shuffle_job_control_v2_format::kMaximumRoutes};
};

[[nodiscard]] common::Status
validate_distributed_vector_grouped_aggregate_shuffle_job_control_decode_limits(
    const DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits& limits) noexcept;

class EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest {
public:
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest() = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest(
      const EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&
  operator=(const EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest(
      EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&&) noexcept = default;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&
  operator=(EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
  encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(
      const DistributedVectorGroupedAggregateShuffleJobPrepare& request);
  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
  encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(
      const DistributedVectorGroupedAggregateShuffleJobSeal& request);
  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
  encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(
      const DistributedVectorGroupedAggregateShuffleJobInstallRoutes& request);
};

class EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse {
public:
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse() = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse(
      const EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&
  operator=(const EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse(
      EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&&) noexcept = default;
  EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&
  operator=(EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
  encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
      const DistributedVectorGroupedAggregateShuffleJobControlResponse& response);
  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
  encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(
      const DistributedVectorGroupedAggregateShuffleJobControlResponse& response);
};

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(
    const DistributedVectorGroupedAggregateShuffleJobPrepare& request);

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(
    const DistributedVectorGroupedAggregateShuffleJobSeal& request);

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(
    const DistributedVectorGroupedAggregateShuffleJobInstallRoutes& request);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
    common::ByteView bytes,
    DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
    common::ByteView bytes,
    DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(
    const DistributedVectorGroupedAggregateShuffleJobControlResponse& response);

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlResponse>
encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2(
    const DistributedVectorGroupedAggregateShuffleJobControlResponse& response);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1_exact(
    common::ByteView bytes);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v2_exact(
    common::ByteView bytes);

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_
