#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_execution.hpp"

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

class DistributedVectorGroupedAggregateShuffleResultExecution::Impl {
public:
  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       query::QueryResourceContext resources,
       std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations,
       std::vector<std::size_t> partition_destinations)
      : authority_(authority), resources_(std::move(resources)),
        destinations_(std::move(destinations)),
        partition_destinations_(std::move(partition_destinations)) {
    metrics_.total_partitions = partition_destinations_.size();
  }

  [[nodiscard]] common::Result<query::PhysicalOperatorStep> fail(common::Status failure) {
    failure_ = std::move(failure);
    failed_ = true;
    return common::make_unexpected(failure_);
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  query::QueryResourceContext resources_;
  std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations_;
  std::vector<std::size_t> partition_destinations_;
  std::size_t partition_ordinal_{};
  DistributedVectorGroupedAggregateShuffleResultExecutionMetrics metrics_;
  common::Status failure_{common::StatusCode::kInternal,
                          "grouped shuffle result execution has not failed"};
  bool failed_{};
  bool complete_{};
};

DistributedVectorGroupedAggregateShuffleResultExecution::
    DistributedVectorGroupedAggregateShuffleResultExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleResultExecution::
    ~DistributedVectorGroupedAggregateShuffleResultExecution() = default;
DistributedVectorGroupedAggregateShuffleResultExecution::
    DistributedVectorGroupedAggregateShuffleResultExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleResultExecution::
    DistributedVectorGroupedAggregateShuffleResultExecution(
        DistributedVectorGroupedAggregateShuffleResultExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleResultExecution&
DistributedVectorGroupedAggregateShuffleResultExecution::operator=(
    DistributedVectorGroupedAggregateShuffleResultExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleResultExecution>
DistributedVectorGroupedAggregateShuffleResultExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations,
    const std::size_t maximum_working_memory_bytes) {
  if (destinations.empty() || maximum_working_memory_bytes == 0U ||
      maximum_working_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleResultWorkingMemoryBytes) {
    return common::make_unexpected(invalid("grouped shuffle result configuration is invalid"));
  }
  auto resources = query::QueryResourceContext::create(maximum_working_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());
  try {
    std::map<raft::NodeId, std::size_t> destination_indexes;
    for (std::size_t index = 0U; index < destinations.size(); ++index) {
      const auto& destination = destinations[index];
      const auto metrics = destination.metrics();
      if (destination.authority() != std::addressof(authority) ||
          destination.state() !=
              DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete ||
          metrics.output_chunks != 0U || metrics.completed_output_partitions != 0U ||
          !destination_indexes.emplace(destination.local_node_id(), index).second) {
        return common::make_unexpected(
            invalid("grouped shuffle result destination is unsealed, consumed, or duplicated"));
      }
    }
    std::vector<std::size_t> partition_destinations;
    std::set<raft::NodeId> expected_nodes;
    partition_destinations.reserve(authority.destinations().size());
    for (const auto& partition : authority.destinations()) {
      const auto destination = destination_indexes.find(partition.node_id);
      if (destination == destination_indexes.end()) {
        return common::make_unexpected(
            invalid("grouped shuffle result is missing a destination node"));
      }
      expected_nodes.emplace(partition.node_id);
      partition_destinations.push_back(destination->second);
    }
    if (expected_nodes.size() != destinations.size()) {
      return common::make_unexpected(invalid("grouped shuffle result has an extra destination"));
    }
    return DistributedVectorGroupedAggregateShuffleResultExecution{
        std::make_unique<Impl>(authority, std::move(*resources), std::move(destinations),
                               std::move(partition_destinations))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result exceeds limits"));
  }
}

common::Result<query::PhysicalOperatorStep>
DistributedVectorGroupedAggregateShuffleResultExecution::next() {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle result execution is empty"));
  Impl& impl = *implementation_;
  if (impl.failed_)
    return common::make_unexpected(impl.failure_);
  if (impl.complete_)
    return query::PhysicalOperatorStep::end();
  while (impl.partition_ordinal_ < impl.partition_destinations_.size()) {
    auto& destination = impl.destinations_[impl.partition_destinations_[impl.partition_ordinal_]];
    auto step = destination.next(static_cast<std::uint32_t>(impl.partition_ordinal_));
    if (!step.has_value())
      return impl.fail(step.error());
    if (step->kind() == query::PhysicalOperatorStepKind::kChunk) {
      ++impl.metrics_.emitted_chunks;
      return step;
    }
    ++impl.partition_ordinal_;
    ++impl.metrics_.completed_partitions;
  }
  impl.complete_ = true;
  return query::PhysicalOperatorStep::end();
}

std::span<const query::VectorGroupKeyDefinition>
DistributedVectorGroupedAggregateShuffleResultExecution::key_definitions() const noexcept {
  return implementation_ ? implementation_->authority_.get().key_definitions()
                         : std::span<const query::VectorGroupKeyDefinition>{};
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorGroupedAggregateShuffleResultExecution::aggregate_definitions() const noexcept {
  return implementation_ ? implementation_->authority_.get().aggregate_definitions()
                         : std::span<const query::VectorAggregateDefinition>{};
}

std::optional<query::QueryResourceContext>
DistributedVectorGroupedAggregateShuffleResultExecution::output_resources() const noexcept {
  return implementation_ ? std::optional<query::QueryResourceContext>{implementation_->resources_}
                         : std::nullopt;
}

DistributedVectorGroupedAggregateShuffleResultExecutionMetrics
DistributedVectorGroupedAggregateShuffleResultExecution::metrics() const noexcept {
  return implementation_ ? implementation_->metrics_
                         : DistributedVectorGroupedAggregateShuffleResultExecutionMetrics{};
}

const common::Status&
DistributedVectorGroupedAggregateShuffleResultExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "grouped shuffle result execution is empty"};
  if (!implementation_)
    return empty;
  return implementation_->failure_;
}

} // namespace chronos::cluster
