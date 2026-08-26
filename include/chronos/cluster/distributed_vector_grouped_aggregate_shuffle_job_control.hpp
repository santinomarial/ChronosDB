#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace chronos::cluster {

namespace distributed_vector_grouped_aggregate_shuffle_job_control_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 128U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength +
    distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength +
    query::distributed_vector_result_schema_format::kMaximumFrameLength + kTrailerLength;
inline constexpr std::chrono::milliseconds kMaximumExecutionTimeout{86'400'000};
} // namespace distributed_vector_grouped_aggregate_shuffle_job_control_format

enum class DistributedVectorGroupedAggregateShuffleJobControlAction : std::uint8_t {
  kPrepare = 1,
  kSeal = 2,
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

using DistributedVectorGroupedAggregateShuffleJobControlRequest =
    std::variant<DistributedVectorGroupedAggregateShuffleJobPrepare,
                 DistributedVectorGroupedAggregateShuffleJobSeal>;

struct DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumFrameLength};
  std::chrono::milliseconds maximum_execution_timeout{
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kMaximumExecutionTimeout};
  DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits authority;
  query::DistributedVectorResultSchemaDecodeLimits result_schema;
};

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
};

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(
    const DistributedVectorGroupedAggregateShuffleJobPrepare& request);

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleJobControlRequest>
encode_distributed_vector_grouped_aggregate_shuffle_job_seal_v1(
    const DistributedVectorGroupedAggregateShuffleJobSeal& request);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleJobControlRequest>
decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
    common::ByteView bytes,
    DistributedVectorGroupedAggregateShuffleJobControlDecodeLimits limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_JOB_CONTROL_HPP_
