#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_collector.hpp"

#include <cstddef>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status conflict(const char* message) {
  return {common::StatusCode::kAlreadyExists, message};
}

[[nodiscard]] bool
same_stream(const DistributedVectorGroupedAggregateShuffleCompleteResultStream& left,
            const DistributedVectorGroupedAggregateShuffleCompleteResultStream& right) noexcept {
  return left.query_id == right.query_id && left.source_node_id == right.source_node_id &&
         left.target_node_id == right.target_node_id && left.partition_id == right.partition_id &&
         left.frame_count == right.frame_count && left.encoded_bytes == right.encoded_bytes &&
         left.encoded_result_batches == right.encoded_result_batches;
}

} // namespace

DistributedVectorGroupedAggregateShuffleResultCollector::
    DistributedVectorGroupedAggregateShuffleResultCollector(
        const DistributedVectorGroupedAggregateShuffleAuthority& authority,
        const query::DistributedVectorResultSchema& result_schema,
        const raft::NodeId coordinator_node_id,
        const DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits,
        std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
            streams) noexcept
    : authority_(authority), result_schema_(result_schema),
      coordinator_node_id_(coordinator_node_id), limits_(limits), streams_(std::move(streams)) {
  metrics_.total_partitions = streams_.size();
}

common::Result<DistributedVectorGroupedAggregateShuffleResultCollector>
DistributedVectorGroupedAggregateShuffleResultCollector::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const raft::NodeId coordinator_node_id,
    const DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits) {
  if (coordinator_node_id == 0U ||
      !validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(limits.stream) ||
      limits.maximum_total_encoded_bytes < limits.stream.maximum_encoded_bytes ||
      limits.maximum_total_encoded_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleResultCollectorBytes ||
      !query::validate_distributed_vector_result_schema_value(result_schema).is_ok()) {
    return common::make_unexpected(
        invalid("grouped shuffle result collector configuration is invalid"));
  }
  for (const auto& destination : authority.destinations()) {
    if (destination.node_id == coordinator_node_id) {
      return common::make_unexpected(
          invalid("grouped shuffle result collector coordinator is a reducer"));
    }
  }
  try {
    std::vector<std::optional<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
        streams(authority.partition_count());
    return DistributedVectorGroupedAggregateShuffleResultCollector{
        authority, result_schema, coordinator_node_id, limits, std::move(streams)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle result collector allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle result collector exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleResultCollector::accept_stream(
    DistributedVectorGroupedAggregateShuffleCompleteResultStream stream) {
  return accept_stream_preserving(stream);
}

common::Status DistributedVectorGroupedAggregateShuffleResultCollector::accept_stream_preserving(
    DistributedVectorGroupedAggregateShuffleCompleteResultStream& stream) {
  if (state_ == DistributedVectorGroupedAggregateShuffleResultCollectorState::kTaken)
    return invalid("grouped shuffle result collector has been taken");
  if (stream.query_id != authority_.get().query_id() ||
      stream.target_node_id != coordinator_node_id_ || stream.partition_id >= streams_.size()) {
    return invalid("grouped shuffle result stream identity is invalid");
  }
  auto source = authority_.get().destination_node(stream.partition_id);
  if (!source.has_value() || *source != stream.source_node_id)
    return invalid("grouped shuffle result stream source is invalid");
  auto canonical = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
      authority_.get(), result_schema_.get(), stream.partition_id, stream.source_node_id,
      coordinator_node_id_, stream.encoded_result_batches, limits_.stream);
  if (!canonical.has_value())
    return canonical.error();
  if (canonical->frame_count() != stream.frame_count ||
      canonical->encoded_bytes() != stream.encoded_bytes) {
    return invalid("grouped shuffle result stream extent is not canonical");
  }

  auto& retained = streams_[stream.partition_id];
  if (retained.has_value()) {
    if (!same_stream(*retained, stream))
      return conflict("grouped shuffle result retry conflicts with retained partition");
    if (metrics_.duplicate_streams != std::numeric_limits<std::size_t>::max())
      ++metrics_.duplicate_streams;
    return common::Status::ok();
  }
  if (stream.encoded_bytes >
      limits_.maximum_total_encoded_bytes - metrics_.retained_encoded_bytes) {
    return exhausted("grouped shuffle result collector bytes are exhausted");
  }
  metrics_.retained_encoded_bytes += stream.encoded_bytes;
  retained.emplace(std::move(stream));
  ++metrics_.accepted_partitions;
  if (metrics_.accepted_partitions == metrics_.total_partitions)
    state_ = DistributedVectorGroupedAggregateShuffleResultCollectorState::kComplete;
  return common::Status::ok();
}

common::Result<std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream>>
DistributedVectorGroupedAggregateShuffleResultCollector::take_complete_streams() {
  if (state_ != DistributedVectorGroupedAggregateShuffleResultCollectorState::kComplete) {
    return common::make_unexpected(
        unavailable("complete grouped shuffle result collection is unavailable"));
  }
  try {
    std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> result;
    result.reserve(streams_.size());
    for (auto& stream : streams_) {
      // Complete state proves every partition slot is populated exactly once.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      result.push_back(std::move(*stream));
      stream.reset();
    }
    metrics_.retained_encoded_bytes = 0U;
    state_ = DistributedVectorGroupedAggregateShuffleResultCollectorState::kTaken;
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result collection publication failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle result collection publication exceeds limits"));
  }
}

DistributedVectorGroupedAggregateShuffleResultCollectorState
DistributedVectorGroupedAggregateShuffleResultCollector::state() const noexcept {
  return state_;
}

bool DistributedVectorGroupedAggregateShuffleResultCollector::ready() const noexcept {
  return state_ == DistributedVectorGroupedAggregateShuffleResultCollectorState::kComplete;
}

bool DistributedVectorGroupedAggregateShuffleResultCollector::contains_partition(
    const std::uint32_t partition_id) const noexcept {
  return partition_id < streams_.size() && streams_[partition_id].has_value();
}

raft::NodeId
DistributedVectorGroupedAggregateShuffleResultCollector::coordinator_node_id() const noexcept {
  return coordinator_node_id_;
}

DistributedVectorGroupedAggregateShuffleResultCollectorMetrics
DistributedVectorGroupedAggregateShuffleResultCollector::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::cluster
