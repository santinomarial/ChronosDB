#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool
valid_decode_limits(const DistributedVectorGroupedAggregateExchangeDecodeLimits& limits) noexcept {
  return limits.maximum_frame_length >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_frame_length <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength &&
         limits.maximum_key_payload_bytes > 0U &&
         limits.maximum_key_payload_bytes <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumKeyPayloadBytes &&
         limits.maximum_groups > 0U &&
         limits.maximum_groups <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_group_keys > 0U &&
         limits.maximum_group_keys <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroupKeys &&
         limits.maximum_aggregates <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumAggregates &&
         limits.state.maximum_frame_length >=
             distributed_vector_aggregate_state_format::kMinimumFrameLength &&
         limits.state.maximum_frame_length <=
             distributed_vector_aggregate_state_format::kMaximumFrameLength &&
         limits.state.maximum_variable_extremum_bytes > 0U &&
         limits.state.maximum_variable_extremum_bytes <=
             distributed_vector_aggregate_state_format::kMaximumExtremumBytes;
}

[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregatePartitionerLimits& limits) noexcept {
  return limits.maximum_partitions > 0U &&
         limits.maximum_partitions <= kMaximumDistributedVectorGroupedAggregatePartitions &&
         limits.maximum_input_groups > 0U &&
         limits.maximum_input_groups <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_groups_per_partition > 0U &&
         limits.maximum_groups_per_partition <=
             distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups &&
         limits.maximum_input_encoded_bytes >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_input_encoded_bytes <= kMaximumDistributedVectorGroupedPartitionBytes &&
         limits.maximum_partition_encoded_bytes >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_partition_encoded_bytes <= kMaximumDistributedVectorGroupedPartitionBytes &&
         limits.maximum_total_output_encoded_bytes >=
             distributed_vector_grouped_aggregate_exchange_format::kMinimumFrameLength &&
         limits.maximum_total_output_encoded_bytes <=
             kMaximumDistributedVectorGroupedPartitionBytes &&
         valid_decode_limits(limits.decode);
}

[[nodiscard]] common::Result<std::size_t>
checked_add_bytes(const std::size_t current, const std::size_t added, const char* message) {
  const auto sum = common::checked_add(current, added);
  if (!sum.has_value())
    return common::make_unexpected(exhausted(message));
  return *sum;
}

} // namespace

DistributedVectorGroupedAggregatePartitioner::DistributedVectorGroupedAggregatePartitioner(
    std::vector<VectorGroupKeyDefinition> keys, std::vector<VectorAggregateDefinition> aggregates,
    QueryResourceContext resources, const std::uint32_t partition_count,
    const DistributedVectorGroupedAggregatePartitionerLimits limits) noexcept
    : keys_(std::move(keys)), aggregates_(std::move(aggregates)), resources_(std::move(resources)),
      partition_count_(partition_count), limits_(limits) {}

common::Result<DistributedVectorGroupedAggregatePartitioner>
DistributedVectorGroupedAggregatePartitioner::create(
    std::vector<VectorGroupKeyDefinition> keys, std::vector<VectorAggregateDefinition> aggregates,
    QueryResourceContext resources, const std::uint32_t partition_count,
    const DistributedVectorGroupedAggregatePartitionerLimits limits) {
  if (!valid_limits(limits) || partition_count == 0U ||
      partition_count > limits.maximum_partitions || keys.empty() ||
      keys.size() > limits.decode.maximum_group_keys ||
      aggregates.size() > limits.decode.maximum_aggregates) {
    return common::make_unexpected(invalid("grouped partitioner authority or limits are invalid"));
  }
  const common::Status authority = validate_distributed_vector_grouped_aggregate_authority(
      keys, aggregates, limits.decode.maximum_group_keys, limits.decode.maximum_aggregates);
  if (!authority.is_ok())
    return common::make_unexpected(authority);
  return DistributedVectorGroupedAggregatePartitioner{
      std::move(keys), std::move(aggregates), std::move(resources), partition_count, limits};
}

common::Result<std::vector<DistributedVectorGroupedAggregatePartitionStream>>
DistributedVectorGroupedAggregatePartitioner::partition(
    const std::span<const EncodedDistributedVectorGroupedAggregateExchangeMessage> input) const {
  if (input.empty() || input.size() > limits_.maximum_input_groups)
    return common::make_unexpected(invalid("grouped partition input stream size is invalid"));
  try {
    std::size_t input_bytes{};
    std::vector<DistributedVectorGroupedAggregateExchangeMessage> decoded;
    decoded.reserve(input.size());
    for (const auto& encoded : input) {
      auto total = checked_add_bytes(input_bytes, encoded.bytes().size(),
                                     "grouped partition input byte count overflowed");
      if (!total.has_value())
        return common::make_unexpected(total.error());
      input_bytes = *total;
      if (input_bytes > limits_.maximum_input_encoded_bytes)
        return common::make_unexpected(exhausted("grouped partition input byte limit exceeded"));
      auto message = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          encoded.bytes(), keys_, aggregates_, resources_, limits_.decode);
      if (!message.has_value())
        return common::make_unexpected(message.error());
      decoded.push_back(std::move(*message));
    }

    const auto& first = decoded.front().position();
    if (first.empty) {
      if (decoded.size() != 1U)
        return common::make_unexpected(invalid("grouped empty input stream is not singular"));
    } else if (first.group_count != decoded.size() ||
               decoded.size() > limits_.maximum_input_groups) {
      return common::make_unexpected(invalid("grouped partition input stream is incomplete"));
    }
    for (std::size_t index = 0U; index < decoded.size(); ++index) {
      const auto& position = decoded[index].position();
      if (position.query_id != first.query_id || position.tablet_id != first.tablet_id)
        return common::make_unexpected(invalid("grouped partition input identity changed"));
      if (first.empty)
        continue;
      if (position.empty || position.group_count != decoded.size() ||
          position.group_ordinal != index || position.sequence != index + 1U ||
          position.terminal != (index + 1U == decoded.size())) {
        return common::make_unexpected(invalid("grouped partition input sequence is invalid"));
      }
    }

    std::vector<std::vector<std::size_t>> assignments(partition_count_);
    if (!first.empty) {
      for (std::size_t index = 0U; index < decoded.size(); ++index) {
        auto hash = canonical_vector_group_key_hash_v1(keys_, decoded[index].keys());
        if (!hash.has_value())
          return common::make_unexpected(hash.error());
        const std::size_t partition = static_cast<std::size_t>(*hash % partition_count_);
        if (assignments[partition].size() >= limits_.maximum_groups_per_partition) {
          return common::make_unexpected(exhausted("grouped partition skew group limit exceeded"));
        }
        assignments[partition].push_back(index);
      }
    }

    std::vector<DistributedVectorGroupedAggregatePartitionStream> output;
    output.reserve(partition_count_);
    std::size_t total_output_bytes{};
    for (std::uint32_t partition = 0U; partition < partition_count_; ++partition) {
      DistributedVectorGroupedAggregatePartitionStream stream{
          .partition_id = partition, .messages = {}, .encoded_bytes = 0U};
      const std::vector<std::size_t>& indexes = assignments[partition];
      stream.messages.reserve(std::max<std::size_t>(indexes.size(), 1U));
      if (indexes.empty()) {
        auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
            {.query_id = first.query_id,
             .tablet_id = first.tablet_id,
             .sequence = 1U,
             .group_ordinal = 0U,
             .group_count = 0U,
             .terminal = true,
             .empty = true},
            {}, {}, keys_, aggregates_);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        stream.encoded_bytes = encoded->bytes().size();
        stream.messages.push_back(std::move(*encoded));
      } else {
        const std::uint32_t group_count = static_cast<std::uint32_t>(indexes.size());
        for (std::uint32_t ordinal = 0U; ordinal < group_count; ++ordinal) {
          const auto& message = decoded[indexes[ordinal]];
          auto encoded = encode_distributed_vector_grouped_aggregate_exchange_message(
              {.query_id = first.query_id,
               .tablet_id = first.tablet_id,
               .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
               .group_ordinal = ordinal,
               .group_count = group_count,
               .terminal = ordinal + 1U == group_count,
               .empty = false},
              message.keys(), message.states(), keys_, aggregates_);
          if (!encoded.has_value())
            return common::make_unexpected(encoded.error());
          auto partition_bytes =
              checked_add_bytes(stream.encoded_bytes, encoded->bytes().size(),
                                "grouped partition output byte count overflowed");
          if (!partition_bytes.has_value())
            return common::make_unexpected(partition_bytes.error());
          stream.encoded_bytes = *partition_bytes;
          if (stream.encoded_bytes > limits_.maximum_partition_encoded_bytes) {
            return common::make_unexpected(
                exhausted("grouped partition output byte limit exceeded"));
          }
          stream.messages.push_back(std::move(*encoded));
        }
      }
      if (stream.encoded_bytes > limits_.maximum_partition_encoded_bytes) {
        return common::make_unexpected(exhausted("grouped partition output byte limit exceeded"));
      }
      auto output_bytes = checked_add_bytes(total_output_bytes, stream.encoded_bytes,
                                            "grouped partition total byte count overflowed");
      if (!output_bytes.has_value())
        return common::make_unexpected(output_bytes.error());
      total_output_bytes = *output_bytes;
      if (total_output_bytes > limits_.maximum_total_output_encoded_bytes) {
        return common::make_unexpected(
            exhausted("grouped partition total output byte limit exceeded"));
      }
      output.push_back(std::move(stream));
    }
    return output;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped partition allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped partition exceeds container limits"));
  }
}

std::uint32_t DistributedVectorGroupedAggregatePartitioner::partition_count() const noexcept {
  return partition_count_;
}

std::span<const VectorGroupKeyDefinition>
DistributedVectorGroupedAggregatePartitioner::key_definitions() const noexcept {
  return keys_;
}

std::span<const VectorAggregateDefinition>
DistributedVectorGroupedAggregatePartitioner::aggregate_definitions() const noexcept {
  return aggregates_;
}

} // namespace chronos::query
