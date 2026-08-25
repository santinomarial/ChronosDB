#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_

#include "chronos/cluster/distributed_vector_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/resource_context.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

// Grouped sufficient-state requests reuse the exact Fragment-v2 CHDVREQ2 carrier. This distinct
// response envelope prevents row, ungrouped-state, and grouped-state payload confusion.
inline constexpr std::size_t kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize = 112U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size =
    kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
    query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength +
    kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;

struct DistributedVectorGroupedAggregateQueryResponseV2 {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<query::DistributedVectorGroupedAggregateExchangeMessage> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_query_response_v2(
    const DistributedVectorGroupedAggregateQueryResponseV2& response,
    std::span<const query::VectorGroupKeyDefinition> expected_keys,
    std::span<const query::VectorAggregateDefinition> expected_aggregates);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryResponseV2>
decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
    common::ByteView bytes, std::span<const query::VectorGroupKeyDefinition> expected_keys,
    std::span<const query::VectorAggregateDefinition> expected_aggregates,
    const query::QueryResourceContext& resources,
    query::DistributedVectorGroupedAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateQueryResponseV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateQueryResponseV2> response;
};

// One connection owner retains the complete grouped authority and query resource context. Only a
// checksummed fixed header may drive exact frame allocation; every failure remains sticky.
class DistributedVectorGroupedAggregateQueryResponseV2Reader {
public:
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      std::vector<query::VectorGroupKeyDefinition>&& expected_keys,
      std::vector<query::VectorAggregateDefinition>&& expected_aggregates,
      query::QueryResourceContext resources,
      std::size_t maximum_frame_length =
          kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size,
      query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits = {}) noexcept;
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      const DistributedVectorGroupedAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader&
  operator=(const DistributedVectorGroupedAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      DistributedVectorGroupedAggregateQueryResponseV2Reader&&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader&
  operator=(DistributedVectorGroupedAggregateQueryResponseV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryResponseV2ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::vector<query::VectorGroupKeyDefinition> expected_keys_;
  std::vector<query::VectorAggregateDefinition> expected_aggregates_;
  query::QueryResourceContext resources_;
  std::size_t maximum_frame_length_{};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits_;
  std::array<std::byte, kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// Typed construction validates the complete grouped authority before any byte becomes writable.
class DistributedVectorGroupedAggregateQueryResponseV2WriteCursor {
public:
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor() = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      const DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&
  operator=(const DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&
  operator=(DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryResponseV2WriteCursor>
  create(const DistributedVectorGroupedAggregateQueryResponseV2& response,
         std::span<const query::VectorGroupKeyDefinition> expected_keys,
         std::span<const query::VectorAggregateDefinition> expected_aggregates);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
