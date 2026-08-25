#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"

#include "chronos/common/checked_math.hpp"

#include <cstddef>
#include <functional>
#include <map>
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

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

struct CanonicalStreamExtent {
  std::size_t outer_bytes{};
  std::size_t nested_bytes{};
  std::size_t additional_nested_bytes{};
};

[[nodiscard]] common::Result<CanonicalStreamExtent>
canonical_extent(const DistributedVectorGroupedAggregateShuffleCompleteStream& stream,
                 const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                 const std::size_t accepted_prefix) {
  std::size_t outer_bytes{};
  std::size_t nested_bytes{};
  std::size_t additional_nested_bytes{};
  for (std::size_t ordinal = 0U; ordinal < stream.messages.size(); ++ordinal) {
    const auto& message = stream.messages[ordinal];
    const auto& position = message.position();
    const bool valid_empty = position.empty && stream.messages.size() == 1U &&
                             position.group_count == 0U && position.group_ordinal == 0U &&
                             position.sequence == 1U && position.terminal;
    const bool valid_groups = !position.empty && position.group_count == stream.messages.size() &&
                              position.group_ordinal == ordinal &&
                              position.sequence == ordinal + 1U &&
                              position.terminal == (ordinal + 1U == stream.messages.size());
    if (position.query_id != authority.query_id() || position.tablet_id != stream.edge.tablet_id ||
        (!valid_empty && !valid_groups)) {
      return common::make_unexpected(invalid("grouped shuffle reducer stream is not complete"));
    }
    if (!position.empty) {
      auto hash =
          query::canonical_vector_group_key_hash_v1(authority.key_definitions(), message.keys());
      if (!hash.has_value())
        return common::make_unexpected(hash.error());
      if (*hash % authority.partition_count() != stream.edge.partition_id) {
        return common::make_unexpected(
            invalid("grouped shuffle reducer stream contains a misrouted key"));
      }
    }
    auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        message, authority.key_definitions(), authority.aggregate_definitions());
    if (!nested.has_value())
      return common::make_unexpected(nested.error());
    const auto next_nested = common::checked_add(nested_bytes, nested->bytes().size());
    if (!next_nested.has_value())
      return common::make_unexpected(exhausted("grouped shuffle reducer nested bytes overflowed"));
    nested_bytes = *next_nested;
    if (ordinal >= accepted_prefix) {
      const auto next_additional =
          common::checked_add(additional_nested_bytes, nested->bytes().size());
      if (!next_additional.has_value()) {
        return common::make_unexpected(
            exhausted("grouped shuffle reducer additional nested bytes overflowed"));
      }
      additional_nested_bytes = *next_additional;
    }
    const auto nested_and_trailer = common::checked_add(
        nested->bytes().size(), kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize);
    const auto frame_bytes =
        nested_and_trailer.has_value()
            ? common::checked_add(kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize,
                                  *nested_and_trailer)
            : std::optional<std::size_t>{};
    const auto next_outer = frame_bytes.has_value() ? common::checked_add(outer_bytes, *frame_bytes)
                                                    : std::optional<std::size_t>{};
    if (!next_outer.has_value())
      return common::make_unexpected(exhausted("grouped shuffle reducer outer bytes overflowed"));
    outer_bytes = *next_outer;
  }
  return CanonicalStreamExtent{.outer_bytes = outer_bytes,
                               .nested_bytes = nested_bytes,
                               .additional_nested_bytes = additional_nested_bytes};
}

} // namespace

DistributedVectorGroupedAggregateShuffleReducer::DistributedVectorGroupedAggregateShuffleReducer(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const std::uint32_t partition_id,
    const DistributedVectorGroupedAggregateShuffleReducerLimits limits,
    const raft::NodeId local_node_id, std::map<schema::TabletId, std::size_t> source_indices,
    std::vector<SourceProgress> sources,
    query::DistributedVectorGroupedAggregateCoordinator coordinator) noexcept
    : authority_(authority), partition_id_(partition_id), local_node_id_(local_node_id),
      limits_(limits), source_indices_(std::move(source_indices)), sources_(std::move(sources)),
      coordinator_(std::move(coordinator)) {}

common::Result<DistributedVectorGroupedAggregateShuffleReducer>
DistributedVectorGroupedAggregateShuffleReducer::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const std::uint32_t partition_id, const raft::NodeId local_node_id,
    const DistributedVectorGroupedAggregateShuffleReducerLimits limits) {
  auto destination = authority.destination_node(partition_id);
  if (!destination.has_value())
    return common::make_unexpected(destination.error());
  if (local_node_id == 0U || *destination != local_node_id ||
      limits.maximum_source_stream_bytes == 0U ||
      limits.maximum_source_stream_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleStreamBytes ||
      limits.maximum_total_stream_bytes < limits.maximum_source_stream_bytes ||
      limits.maximum_total_stream_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleReducerBytes) {
    return common::make_unexpected(invalid("grouped shuffle reducer configuration is invalid"));
  }
  try {
    std::vector<schema::TabletId> tablets;
    tablets.reserve(authority.sources().size());
    std::map<schema::TabletId, std::size_t> source_indices;
    for (std::size_t index = 0U; index < authority.sources().size(); ++index) {
      tablets.push_back(authority.sources()[index].tablet_id);
      source_indices.emplace(authority.sources()[index].tablet_id, index);
    }
    std::vector<query::VectorGroupKeyDefinition> keys{authority.key_definitions().begin(),
                                                      authority.key_definitions().end()};
    std::vector<query::VectorAggregateDefinition> definitions{
        authority.aggregate_definitions().begin(), authority.aggregate_definitions().end()};
    auto coordinator = query::DistributedVectorGroupedAggregateCoordinator::create(
        authority.query_id(), std::move(tablets), std::move(keys), std::move(definitions),
        limits.coordinator);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    std::vector<SourceProgress> sources(authority.sources().size());
    return DistributedVectorGroupedAggregateShuffleReducer{authority,
                                                           partition_id,
                                                           limits,
                                                           local_node_id,
                                                           std::move(source_indices),
                                                           std::move(sources),
                                                           std::move(*coordinator)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped shuffle reducer allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle reducer exceeds container limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleReducer::accept_stream(
    const DistributedVectorGroupedAggregateShuffleCompleteStream& stream) {
  if (ready_)
    return invalid("grouped shuffle reducer input is sealed");
  common::Status edge_status = authority_.get().validate_edge(stream.edge);
  if (!edge_status.is_ok())
    return edge_status;
  if (stream.edge.partition_id != partition_id_ || stream.edge.target_node_id != local_node_id_ ||
      stream.messages.empty()) {
    return invalid("grouped shuffle stream does not belong to this reducer");
  }
  const auto source = source_indices_.find(stream.edge.tablet_id);
  if (source == source_indices_.end())
    return invalid("grouped shuffle reducer source is unknown");
  if (stream.messages.size() > limits_.coordinator.messages.maximum_messages_per_fragment)
    return exhausted("grouped shuffle source exceeds reducer message limit");
  SourceProgress& progress = sources_[source->second];
  if (progress.accepted_prefix > stream.messages.size()) {
    return {common::StatusCode::kAlreadyExists,
            "grouped shuffle retry is shorter than its retained prefix"};
  }
  auto extent = canonical_extent(stream, authority_.get(), progress.accepted_prefix);
  if (!extent.has_value())
    return extent.error();
  if (extent->outer_bytes != stream.encoded_bytes ||
      extent->outer_bytes > limits_.maximum_source_stream_bytes) {
    return invalid("grouped shuffle source extent is not canonical or exceeds its limit");
  }

  const bool first_attempt = !progress.encoded_bytes.has_value();
  if (first_attempt) {
    if (extent->outer_bytes > limits_.maximum_total_stream_bytes - metrics_.retained_stream_bytes) {
      return exhausted("grouped shuffle reducer total stream bytes are exhausted");
    }
    progress.encoded_bytes = extent->outer_bytes;
    progress.message_count = stream.messages.size();
    metrics_.retained_stream_bytes += extent->outer_bytes;
  } else if (*progress.encoded_bytes != extent->outer_bytes ||
             *progress.message_count != stream.messages.size()) {
    return {common::StatusCode::kAlreadyExists,
            "grouped shuffle retry extent conflicts with retained source"};
  }

  const std::size_t additional_messages = stream.messages.size() - progress.accepted_prefix;
  if (additional_messages > limits_.coordinator.messages.maximum_total_messages -
                                coordinator_.retained_message_count() ||
      extent->additional_nested_bytes >
          limits_.coordinator.maximum_total_encoded_bytes - coordinator_.retained_encoded_bytes()) {
    return exhausted("grouped shuffle reducer coordinator retention is exhausted");
  }
  for (std::size_t ordinal = 0U; ordinal < stream.messages.size(); ++ordinal) {
    const common::Status accepted = coordinator_.accept(stream.messages[ordinal]);
    if (!accepted.is_ok())
      return accepted;
    if (ordinal >= progress.accepted_prefix)
      ++progress.accepted_prefix;
  }
  if (progress.complete) {
    ++metrics_.duplicate_streams;
    return common::Status::ok();
  }
  progress.complete = true;
  ++metrics_.accepted_sources;
  return common::Status::ok();
}

common::Status DistributedVectorGroupedAggregateShuffleReducer::finish() {
  if (ready_)
    return invalid("grouped shuffle reducer is already finished");
  if (metrics_.accepted_sources != sources_.size())
    return unavailable("grouped shuffle reducer is missing source terminals");
  const common::Status finished = coordinator_.finish();
  if (finished.is_ok())
    ready_ = true;
  return finished;
}

common::Result<query::PhysicalOperatorStep>
DistributedVectorGroupedAggregateShuffleReducer::next() {
  if (!ready_)
    return common::make_unexpected(invalid("grouped shuffle reducer is not ready for output"));
  return coordinator_.next();
}

std::uint32_t DistributedVectorGroupedAggregateShuffleReducer::partition_id() const noexcept {
  return partition_id_;
}

raft::NodeId DistributedVectorGroupedAggregateShuffleReducer::local_node_id() const noexcept {
  return local_node_id_;
}

bool DistributedVectorGroupedAggregateShuffleReducer::ready() const noexcept {
  return ready_;
}

DistributedVectorGroupedAggregateShuffleReducerMetrics
DistributedVectorGroupedAggregateShuffleReducer::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::cluster
