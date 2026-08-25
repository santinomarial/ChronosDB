#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"

#include "chronos/common/checked_math.hpp"

#include <cstddef>
#include <new>
#include <optional>
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

[[nodiscard]] common::Result<std::size_t>
outer_extent(const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
                 messages) {
  std::size_t total{};
  for (const auto& message : messages) {
    auto frame = common::checked_add(kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize,
                                     message.bytes().size());
    if (frame.has_value()) {
      frame =
          common::checked_add(*frame, kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize);
    }
    const auto next =
        frame.has_value() ? common::checked_add(total, *frame) : std::optional<std::size_t>{};
    if (!next.has_value())
      return common::make_unexpected(exhausted("grouped shuffle source extent overflowed"));
    total = *next;
  }
  return total;
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
decode_local_stream(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const DistributedVectorGroupedAggregateShuffleEdge edge,
    const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
    const query::QueryResourceContext& resources,
    const DistributedVectorGroupedAggregateShuffleStreamLimits& limits) {
  if (messages.empty() || messages.size() > limits.maximum_frames)
    return common::make_unexpected(invalid("local grouped shuffle source stream is empty"));
  auto extent = outer_extent(messages);
  if (!extent.has_value())
    return common::make_unexpected(extent.error());
  if (*extent > limits.maximum_encoded_bytes) {
    return common::make_unexpected(
        exhausted("local grouped shuffle source stream exceeds byte limit"));
  }
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> decoded;
  decoded.reserve(messages.size());
  for (const auto& encoded : messages) {
    auto message = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        encoded.bytes(), authority.key_definitions(), authority.aggregate_definitions(), resources,
        limits.payload);
    if (!message.has_value())
      return common::make_unexpected(message.error());
    const auto& position = message->position();
    if (position.query_id != authority.query_id() || position.tablet_id != edge.tablet_id) {
      return common::make_unexpected(
          invalid("local grouped shuffle source identity differs from authority"));
    }
    if (!position.empty) {
      auto hash =
          query::canonical_vector_group_key_hash_v1(authority.key_definitions(), message->keys());
      if (!hash.has_value())
        return common::make_unexpected(hash.error());
      if (*hash % authority.partition_count() != edge.partition_id) {
        return common::make_unexpected(
            invalid("local grouped shuffle source key is routed incorrectly"));
      }
    }
    decoded.push_back(std::move(*message));
  }
  return DistributedVectorGroupedAggregateShuffleCompleteStream{
      .edge = edge, .messages = std::move(decoded), .encoded_bytes = *extent};
}

} // namespace

DistributedVectorGroupedAggregateShuffleSourcePlan::
    DistributedVectorGroupedAggregateShuffleSourcePlan(
        schema::TabletId tablet_id, const raft::NodeId source_node_id,
        std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream> local_streams,
        std::vector<DistributedVectorGroupedAggregateShuffleRetry> remote_retries,
        const DistributedVectorGroupedAggregateShuffleSourcePlanMetrics metrics) noexcept
    : tablet_id_(tablet_id), source_node_id_(source_node_id),
      local_streams_(std::move(local_streams)), remote_retries_(std::move(remote_retries)),
      metrics_(metrics) {}

common::Result<DistributedVectorGroupedAggregateShuffleSourcePlan>
DistributedVectorGroupedAggregateShuffleSourcePlan::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const schema::TabletId& tablet_id,
    const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> input,
    const query::QueryResourceContext& resources,
    const DistributedVectorGroupedAggregateShuffleSourcePlanLimits limits) {
  if (limits.maximum_total_outer_encoded_bytes == 0U ||
      limits.maximum_total_outer_encoded_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes) {
    return common::make_unexpected(invalid("grouped shuffle source plan limits are invalid"));
  }
  auto source_node = authority.source_node(tablet_id);
  if (!source_node.has_value())
    return common::make_unexpected(source_node.error());
  try {
    auto partitioner = query::DistributedVectorGroupedAggregatePartitioner::create(
        {authority.key_definitions().begin(), authority.key_definitions().end()},
        {authority.aggregate_definitions().begin(), authority.aggregate_definitions().end()},
        resources, authority.partition_count(), limits.partitioner);
    if (!partitioner.has_value())
      return common::make_unexpected(partitioner.error());
    auto partitions = partitioner->partition(input);
    if (!partitions.has_value())
      return common::make_unexpected(partitions.error());

    std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream> local_streams;
    std::vector<DistributedVectorGroupedAggregateShuffleRetry> remote_retries;
    local_streams.reserve(partitions->size());
    remote_retries.reserve(partitions->size());
    DistributedVectorGroupedAggregateShuffleSourcePlanMetrics metrics;
    for (auto& partition : *partitions) {
      auto destination = authority.destination_node(partition.partition_id);
      if (!destination.has_value())
        return common::make_unexpected(destination.error());
      const DistributedVectorGroupedAggregateShuffleEdge edge{
          .tablet_id = tablet_id,
          .partition_id = partition.partition_id,
          .source_node_id = *source_node,
          .target_node_id = *destination,
          .hash_version = authority.hash_version()};
      const common::Status edge_status = authority.validate_edge(edge);
      if (!edge_status.is_ok())
        return common::make_unexpected(edge_status);
      auto extent = outer_extent(partition.messages);
      if (!extent.has_value())
        return common::make_unexpected(extent.error());
      const auto nested =
          common::checked_add(metrics.nested_encoded_bytes, partition.encoded_bytes);
      const auto outer = common::checked_add(metrics.outer_encoded_bytes, *extent);
      if (!nested.has_value() || !outer.has_value()) {
        return common::make_unexpected(
            exhausted("grouped shuffle source total byte extent overflowed"));
      }
      metrics.nested_encoded_bytes = *nested;
      metrics.outer_encoded_bytes = *outer;
      if (metrics.outer_encoded_bytes > limits.maximum_total_outer_encoded_bytes) {
        return common::make_unexpected(
            exhausted("grouped shuffle source total outer byte limit exceeded"));
      }

      if (*destination == *source_node) {
        auto local = decode_local_stream(authority, edge, partition.messages, resources,
                                         limits.retry.stream);
        if (!local.has_value())
          return common::make_unexpected(local.error());
        local_streams.push_back(std::move(*local));
        ++metrics.local_edges;
      } else {
        auto remote = DistributedVectorGroupedAggregateShuffleRetry::create(
            authority, edge, std::move(partition.messages), resources, limits.retry);
        if (!remote.has_value())
          return common::make_unexpected(remote.error());
        remote_retries.push_back(std::move(*remote));
        ++metrics.remote_edges;
      }
    }
    return DistributedVectorGroupedAggregateShuffleSourcePlan{
        tablet_id, *source_node, std::move(local_streams), std::move(remote_retries), metrics};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle source plan allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle source plan exceeds limits"));
  }
}

const schema::TabletId&
DistributedVectorGroupedAggregateShuffleSourcePlan::tablet_id() const noexcept {
  return tablet_id_;
}

raft::NodeId DistributedVectorGroupedAggregateShuffleSourcePlan::source_node_id() const noexcept {
  return source_node_id_;
}

std::span<const DistributedVectorGroupedAggregateShuffleCompleteStream>
DistributedVectorGroupedAggregateShuffleSourcePlan::local_streams() const noexcept {
  return local_streams_;
}

std::span<const DistributedVectorGroupedAggregateShuffleRetry>
DistributedVectorGroupedAggregateShuffleSourcePlan::remote_retries() const noexcept {
  return remote_retries_;
}

std::vector<DistributedVectorGroupedAggregateShuffleCompleteStream>
DistributedVectorGroupedAggregateShuffleSourcePlan::take_local_streams() noexcept {
  return std::exchange(local_streams_, {});
}

std::vector<DistributedVectorGroupedAggregateShuffleRetry>
DistributedVectorGroupedAggregateShuffleSourcePlan::take_remote_retries() noexcept {
  return std::exchange(remote_retries_, {});
}

DistributedVectorGroupedAggregateShuffleSourcePlanMetrics
DistributedVectorGroupedAggregateShuffleSourcePlan::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::cluster
