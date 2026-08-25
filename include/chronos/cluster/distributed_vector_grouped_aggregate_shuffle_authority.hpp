#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::uint16_t kDistributedVectorGroupedAggregateShuffleHashVersionV1 = 1U;
inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleAuthorityBytes =
    std::size_t{2U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleAuthorityBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleSources = 65'536U;

struct DistributedVectorGroupedAggregateShuffleSource {
  schema::TabletId tablet_id;
  raft::NodeId node_id{};

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleSource&,
                         const DistributedVectorGroupedAggregateShuffleSource&) = default;
};

struct DistributedVectorGroupedAggregateShuffleDestination {
  std::uint32_t partition_id{};
  raft::NodeId node_id{};

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleDestination&,
                         const DistributedVectorGroupedAggregateShuffleDestination&) = default;
};

struct DistributedVectorGroupedAggregateShuffleEdge {
  schema::TabletId tablet_id;
  std::uint32_t partition_id{};
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  std::uint16_t hash_version{};
};

struct DistributedVectorGroupedAggregateShuffleAuthorityLimits {
  std::size_t maximum_sources{kMaximumDistributedVectorGroupedAggregateShuffleSources};
  std::uint32_t maximum_partitions{query::kMaximumDistributedVectorGroupedAggregatePartitions};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleAuthorityBytes};
};

// One immutable, single-thread-affine authority for a complete grouped shuffle. Source order is
// the deterministic reducer merge order. Destinations are canonical and contiguous by partition
// ID. Local source/destination pairs remain valid authority but must bypass a network carrier.
class DistributedVectorGroupedAggregateShuffleAuthority {
public:
  DistributedVectorGroupedAggregateShuffleAuthority() = delete;
  DistributedVectorGroupedAggregateShuffleAuthority(
      const DistributedVectorGroupedAggregateShuffleAuthority&) = delete;
  DistributedVectorGroupedAggregateShuffleAuthority&
  operator=(const DistributedVectorGroupedAggregateShuffleAuthority&) = delete;
  DistributedVectorGroupedAggregateShuffleAuthority(
      DistributedVectorGroupedAggregateShuffleAuthority&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleAuthority&
  operator=(DistributedVectorGroupedAggregateShuffleAuthority&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleAuthority>
  create(common::Uuid query_id, std::vector<DistributedVectorGroupedAggregateShuffleSource> sources,
         std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations,
         std::vector<query::VectorGroupKeyDefinition> keys,
         std::vector<query::VectorAggregateDefinition> aggregates,
         DistributedVectorGroupedAggregateShuffleAuthorityLimits limits = {});

  // Derives plan-order sources and a canonical one-partition-per-distinct-serving-node destination
  // set from one proof-bound mutable fragment vector. The fragments and grouped definitions are
  // copied; no catalog or caller storage is borrowed.
  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleAuthority>
  create_from_mutable_fragments(
      std::span<const query::DistributedMutableVectorFragment> fragments,
      std::span<const query::VectorGroupKeyDefinition> keys,
      std::span<const query::VectorAggregateDefinition> aggregates,
      DistributedVectorGroupedAggregateShuffleAuthorityLimits limits = {});

  [[nodiscard]] common::Result<raft::NodeId> source_node(const schema::TabletId& tablet_id) const;
  [[nodiscard]] common::Result<raft::NodeId> destination_node(std::uint32_t partition_id) const;
  [[nodiscard]] common::Status
  validate_edge(const DistributedVectorGroupedAggregateShuffleEdge& edge) const;

  [[nodiscard]] const common::Uuid& query_id() const noexcept;
  [[nodiscard]] std::uint16_t hash_version() const noexcept;
  [[nodiscard]] std::uint32_t partition_count() const noexcept;
  [[nodiscard]] std::span<const DistributedVectorGroupedAggregateShuffleSource>
  sources() const noexcept;
  [[nodiscard]] std::span<const DistributedVectorGroupedAggregateShuffleDestination>
  destinations() const noexcept;
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> key_definitions() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition>
  aggregate_definitions() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleAuthority(
      common::Uuid query_id, std::vector<DistributedVectorGroupedAggregateShuffleSource> sources,
      std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations,
      std::vector<query::VectorGroupKeyDefinition> keys,
      std::vector<query::VectorAggregateDefinition> aggregates,
      std::map<schema::TabletId, raft::NodeId> source_nodes, std::uint16_t hash_version,
      std::size_t retained_configuration_bytes) noexcept;

  common::Uuid query_id_;
  std::vector<DistributedVectorGroupedAggregateShuffleSource> sources_;
  std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations_;
  std::vector<query::VectorGroupKeyDefinition> keys_;
  std::vector<query::VectorAggregateDefinition> aggregates_;
  std::map<schema::TabletId, raft::NodeId> source_nodes_;
  std::uint16_t hash_version_{};
  std::size_t retained_configuration_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_HPP_
