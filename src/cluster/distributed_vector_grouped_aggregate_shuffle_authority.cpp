#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"

#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status not_found(const char* message) {
  return {common::StatusCode::kNotFound, message};
}

[[nodiscard]] std::optional<std::size_t>
estimated_configuration_bytes(const std::size_t sources, const std::size_t destinations,
                              const std::size_t keys, const std::size_t aggregates) noexcept {
  constexpr std::size_t kOwnerBytes = 512U;
  constexpr std::size_t kSourceBytes = 128U;
  constexpr std::size_t kDestinationBytes = 64U;
  constexpr std::size_t kKeyBytes = 64U;
  constexpr std::size_t kAggregateBytes = 96U;
  std::optional<std::size_t> total{kOwnerBytes};
  const auto add_product = [&](const std::size_t count,
                               const std::size_t width) -> std::optional<std::size_t> {
    const auto bytes = common::checked_multiply(count, width);
    if (!bytes.has_value() || !total.has_value())
      return std::nullopt;
    total = common::checked_add(*total, *bytes);
    return total;
  };
  if (!add_product(sources, kSourceBytes).has_value() ||
      !add_product(destinations, kDestinationBytes).has_value() ||
      !add_product(keys, kKeyBytes).has_value() ||
      !add_product(aggregates, kAggregateBytes).has_value()) {
    return std::nullopt;
  }
  return total;
}

} // namespace

DistributedVectorGroupedAggregateShuffleAuthority::
    DistributedVectorGroupedAggregateShuffleAuthority(
        common::Uuid query_id, std::vector<DistributedVectorGroupedAggregateShuffleSource> sources,
        std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations,
        std::vector<query::VectorGroupKeyDefinition> keys,
        std::vector<query::VectorAggregateDefinition> aggregates,
        std::map<schema::TabletId, raft::NodeId> source_nodes, const std::uint16_t hash_version,
        const std::size_t retained_configuration_bytes) noexcept
    : query_id_(query_id), sources_(std::move(sources)), destinations_(std::move(destinations)),
      keys_(std::move(keys)), aggregates_(std::move(aggregates)),
      source_nodes_(std::move(source_nodes)), hash_version_(hash_version),
      retained_configuration_bytes_(retained_configuration_bytes) {}

common::Result<DistributedVectorGroupedAggregateShuffleAuthority>
DistributedVectorGroupedAggregateShuffleAuthority::create(
    const common::Uuid query_id,
    std::vector<DistributedVectorGroupedAggregateShuffleSource> sources,
    std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations,
    std::vector<query::VectorGroupKeyDefinition> keys,
    std::vector<query::VectorAggregateDefinition> aggregates,
    const DistributedVectorGroupedAggregateShuffleAuthorityLimits limits) {
  if (query_id.is_nil() || sources.empty() || destinations.empty() ||
      limits.maximum_sources == 0U ||
      limits.maximum_sources > kMaximumDistributedVectorGroupedAggregateShuffleSources ||
      limits.maximum_partitions == 0U ||
      limits.maximum_partitions > query::kMaximumDistributedVectorGroupedAggregatePartitions ||
      limits.maximum_retained_configuration_bytes == 0U ||
      limits.maximum_retained_configuration_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleAuthorityBytes) {
    return common::make_unexpected(invalid("grouped shuffle authority identity or limits invalid"));
  }
  if (sources.size() > limits.maximum_sources || destinations.size() > limits.maximum_partitions) {
    return common::make_unexpected(exhausted("grouped shuffle authority count limit exceeded"));
  }
  const common::Status grouped =
      query::validate_distributed_vector_grouped_aggregate_authority(keys, aggregates);
  if (!grouped.is_ok())
    return common::make_unexpected(grouped);
  const auto configuration = estimated_configuration_bytes(sources.size(), destinations.size(),
                                                           keys.size(), aggregates.size());
  if (!configuration.has_value()) {
    return common::make_unexpected(
        exhausted("grouped shuffle authority configuration size overflowed"));
  }
  if (*configuration > limits.maximum_retained_configuration_bytes) {
    return common::make_unexpected(
        exhausted("grouped shuffle authority configuration byte limit exceeded"));
  }
  try {
    std::map<schema::TabletId, raft::NodeId> source_nodes;
    for (const auto& source : sources) {
      if (source.tablet_id.uuid().is_nil() || source.node_id == 0U ||
          !source_nodes.emplace(source.tablet_id, source.node_id).second) {
        return common::make_unexpected(
            invalid("grouped shuffle source authority is invalid or duplicated"));
      }
    }
    for (std::size_t index = 0U; index < destinations.size(); ++index) {
      if (destinations[index].partition_id != index || destinations[index].node_id == 0U) {
        return common::make_unexpected(
            invalid("grouped shuffle destination authority is noncanonical"));
      }
    }
    return DistributedVectorGroupedAggregateShuffleAuthority{
        query_id,
        std::move(sources),
        std::move(destinations),
        std::move(keys),
        std::move(aggregates),
        std::move(source_nodes),
        kDistributedVectorGroupedAggregateShuffleHashVersionV1,
        *configuration};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle authority exceeds container limits"));
  }
}

common::Result<raft::NodeId> DistributedVectorGroupedAggregateShuffleAuthority::source_node(
    const schema::TabletId& tablet_id) const {
  const auto found = source_nodes_.find(tablet_id);
  if (found == source_nodes_.end())
    return common::make_unexpected(not_found("grouped shuffle source tablet is not authorized"));
  return found->second;
}

common::Result<raft::NodeId> DistributedVectorGroupedAggregateShuffleAuthority::destination_node(
    const std::uint32_t partition_id) const {
  if (partition_id >= destinations_.size()) {
    return common::make_unexpected(
        not_found("grouped shuffle destination partition is not authorized"));
  }
  return destinations_[partition_id].node_id;
}

common::Status DistributedVectorGroupedAggregateShuffleAuthority::validate_edge(
    const DistributedVectorGroupedAggregateShuffleEdge& edge) const {
  if (edge.hash_version != hash_version_) {
    return invalid("grouped shuffle edge hash version differs from authority");
  }
  const auto source = source_node(edge.tablet_id);
  if (!source.has_value())
    return source.error();
  const auto destination = destination_node(edge.partition_id);
  if (!destination.has_value())
    return destination.error();
  if (*source != edge.source_node_id || *destination != edge.target_node_id)
    return invalid("grouped shuffle edge node identity differs from authority");
  return common::Status::ok();
}

const common::Uuid& DistributedVectorGroupedAggregateShuffleAuthority::query_id() const noexcept {
  return query_id_;
}

std::uint16_t DistributedVectorGroupedAggregateShuffleAuthority::hash_version() const noexcept {
  return hash_version_;
}

std::uint32_t DistributedVectorGroupedAggregateShuffleAuthority::partition_count() const noexcept {
  return static_cast<std::uint32_t>(destinations_.size());
}

std::span<const DistributedVectorGroupedAggregateShuffleSource>
DistributedVectorGroupedAggregateShuffleAuthority::sources() const noexcept {
  return sources_;
}

std::span<const DistributedVectorGroupedAggregateShuffleDestination>
DistributedVectorGroupedAggregateShuffleAuthority::destinations() const noexcept {
  return destinations_;
}

std::span<const query::VectorGroupKeyDefinition>
DistributedVectorGroupedAggregateShuffleAuthority::key_definitions() const noexcept {
  return keys_;
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorGroupedAggregateShuffleAuthority::aggregate_definitions() const noexcept {
  return aggregates_;
}

std::size_t
DistributedVectorGroupedAggregateShuffleAuthority::retained_configuration_bytes() const noexcept {
  return retained_configuration_bytes_;
}

} // namespace chronos::cluster
