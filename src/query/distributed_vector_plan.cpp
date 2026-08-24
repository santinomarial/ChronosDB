#include "chronos/query/distributed_vector_plan.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bitset>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'P'}, std::byte{'L'},
                                                  std::byte{'N'}, std::byte{'1'}};
inline constexpr std::uint8_t kLimitPresent = 1U;
inline constexpr std::size_t kHeaderCrcOffset = 44U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] bool valid_operation(const VectorAggregateOperation operation) noexcept {
  switch (operation) {
  case VectorAggregateOperation::kCountStar:
  case VectorAggregateOperation::kCount:
  case VectorAggregateOperation::kSum:
  case VectorAggregateOperation::kAverage:
  case VectorAggregateOperation::kMinimum:
  case VectorAggregateOperation::kMaximum:
  case VectorAggregateOperation::kVariancePopulation:
  case VectorAggregateOperation::kVarianceSample:
    return true;
  }
  return false;
}

[[nodiscard]] common::Status validate_intent(const DistributedVectorPlanIntent& intent,
                                             const std::uint32_t maximum_input_columns,
                                             const std::uint32_t maximum_output_columns) {
  if (maximum_input_columns == 0U ||
      maximum_input_columns > distributed_vector_plan_format::kMaximumInputColumns ||
      maximum_output_columns == 0U ||
      maximum_output_columns > distributed_vector_plan_format::kMaximumOutputColumns) {
    return invalid("distributed vector plan column limits are invalid");
  }
  if (intent.row_output_indices.size() > distributed_vector_plan_format::kMaximumRowOutputs ||
      intent.visible_row_output_indices.size() >
          distributed_vector_plan_format::kMaximumVisibleRowOutputs ||
      intent.group_key_input_indices.size() > distributed_vector_plan_format::kMaximumGroupKeys ||
      intent.aggregates.size() > distributed_vector_plan_format::kMaximumAggregates ||
      intent.order_keys.size() > distributed_vector_plan_format::kMaximumOrderKeys) {
    return invalid("distributed vector plan collection exceeds its hard limit");
  }

  std::size_t output_columns{};
  switch (intent.mode) {
  case DistributedVectorPlanMode::kRows:
    if (intent.row_output_indices.empty() || !intent.group_key_input_indices.empty() ||
        !intent.aggregates.empty()) {
      return invalid("distributed vector row plan shape is invalid");
    }
    output_columns = intent.row_output_indices.size();
    break;
  case DistributedVectorPlanMode::kUngroupedAggregate:
    if (!intent.row_output_indices.empty() || !intent.visible_row_output_indices.empty() ||
        !intent.group_key_input_indices.empty() || intent.aggregates.empty()) {
      return invalid("distributed vector ungrouped aggregate plan shape is invalid");
    }
    output_columns = intent.aggregates.size();
    break;
  case DistributedVectorPlanMode::kGroupedAggregate:
    if (!intent.row_output_indices.empty() || !intent.visible_row_output_indices.empty() ||
        intent.group_key_input_indices.empty())
      return invalid("distributed vector grouped aggregate plan shape is invalid");
    output_columns = intent.group_key_input_indices.size() + intent.aggregates.size();
    break;
  default:
    return invalid("distributed vector plan mode is invalid");
  }
  if (output_columns == 0U || output_columns > maximum_output_columns)
    return invalid("distributed vector plan output width is invalid");

  for (const std::uint32_t index : intent.row_output_indices) {
    if (index >= maximum_input_columns)
      return invalid("distributed vector row output index is invalid");
  }
  std::bitset<distributed_vector_plan_format::kMaximumRowOutputs> seen_visible;
  for (const std::uint32_t index : intent.visible_row_output_indices) {
    if (index >= intent.row_output_indices.size() || seen_visible[index])
      return invalid("distributed vector visible row output is invalid or duplicated");
    seen_visible.set(index);
  }
  std::bitset<distributed_vector_plan_format::kMaximumInputColumns> seen_groups;
  for (const std::uint32_t index : intent.group_key_input_indices) {
    if (index >= maximum_input_columns || seen_groups[index])
      return invalid("distributed vector group-key index is invalid or duplicated");
    seen_groups.set(index);
  }
  for (const DistributedVectorAggregateIntent& aggregate : intent.aggregates) {
    if (!valid_operation(aggregate.operation) ||
        (aggregate.operation == VectorAggregateOperation::kCountStar) !=
            !aggregate.input_index.has_value() ||
        (aggregate.input_index.has_value() && *aggregate.input_index >= maximum_input_columns)) {
      return invalid("distributed vector aggregate intent is invalid");
    }
  }
  std::bitset<distributed_vector_plan_format::kMaximumOutputColumns> seen_order;
  for (const DistributedVectorOrderKey& key : intent.order_keys) {
    if (key.output_index >= output_columns || seen_order[key.output_index] ||
        (key.direction != PhysicalSortDirection::kAscending &&
         key.direction != PhysicalSortDirection::kDescending) ||
        (key.null_placement != ScalarNullPlacement::kFirst &&
         key.null_placement != ScalarNullPlacement::kLast)) {
      return invalid("distributed vector order key is invalid or duplicated");
    }
    seen_order.set(key.output_index);
  }
  return common::Status::ok();
}

[[nodiscard]] std::size_t encoded_length(const DistributedVectorPlanIntent& intent) noexcept {
  return distributed_vector_plan_format::kHeaderLength + intent.row_output_indices.size() * 4U +
         intent.visible_row_output_indices.size() * 4U +
         intent.group_key_input_indices.size() * 4U + intent.aggregates.size() * 8U +
         intent.order_keys.size() * 8U + (intent.limit.has_value() ? 8U : 0U) +
         distributed_vector_plan_format::kTrailerLength;
}

} // namespace

EncodedDistributedVectorPlanIntent::EncodedDistributedVectorPlanIntent(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorPlanIntent::bytes() const noexcept {
  return bytes_;
}

common::Status validate_distributed_vector_plan_intent(const DistributedVectorPlanIntent& intent,
                                                       const std::uint32_t input_columns,
                                                       const std::uint32_t maximum_output_columns) {
  return validate_intent(intent, input_columns, maximum_output_columns);
}

common::Result<EncodedDistributedVectorPlanIntent>
encode_distributed_vector_plan_intent(const DistributedVectorPlanIntent& intent) {
  const common::Status validation =
      validate_intent(intent, distributed_vector_plan_format::kMaximumInputColumns,
                      distributed_vector_plan_format::kMaximumOutputColumns);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  try {
    std::vector<std::byte> bytes(encoded_length(intent));
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_plan_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(intent.visible_row_output_indices.empty()
                                       ? distributed_vector_plan_format::kLegacyMinor
                                       : distributed_vector_plan_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_plan_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(bytes.size());
    if (status.is_ok())
      status = writer.write_u8(static_cast<std::uint8_t>(intent.mode));
    if (status.is_ok())
      status = writer.write_u8(intent.limit.has_value() ? kLimitPresent : 0U);
    if (status.is_ok())
      status =
          writer.write_u16_le(static_cast<std::uint16_t>(intent.visible_row_output_indices.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(intent.row_output_indices.size()));
    if (status.is_ok())
      status =
          writer.write_u32_le(static_cast<std::uint32_t>(intent.group_key_input_indices.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(intent.aggregates.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(intent.order_keys.size()));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    for (const std::uint32_t index : intent.row_output_indices) {
      if (status.is_ok())
        status = writer.write_u32_le(index);
    }
    for (const std::uint32_t index : intent.visible_row_output_indices) {
      if (status.is_ok())
        status = writer.write_u32_le(index);
    }
    for (const std::uint32_t index : intent.group_key_input_indices) {
      if (status.is_ok())
        status = writer.write_u32_le(index);
    }
    for (const DistributedVectorAggregateIntent& aggregate : intent.aggregates) {
      if (status.is_ok())
        status = writer.write_u8(static_cast<std::uint8_t>(aggregate.operation));
      if (status.is_ok())
        status = writer.write_u8(aggregate.input_index.has_value() ? 1U : 0U);
      if (status.is_ok())
        status = writer.zero_fill(2U);
      if (status.is_ok())
        status = writer.write_u32_le(aggregate.input_index.value_or(0U));
    }
    for (const DistributedVectorOrderKey& key : intent.order_keys) {
      if (status.is_ok())
        status = writer.write_u32_le(key.output_index);
      if (status.is_ok())
        status = writer.write_u8(static_cast<std::uint8_t>(key.direction));
      if (status.is_ok())
        status = writer.write_u8(static_cast<std::uint8_t>(key.null_placement));
      if (status.is_ok())
        status = writer.zero_fill(2U);
    }
    if (status.is_ok() && intent.limit.has_value())
      status = writer.write_u64_le(*intent.limit);
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "distributed vector plan layout failed"});
    }
    return EncodedDistributedVectorPlanIntent{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed vector plan allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector plan exceeds container limits"});
  }
}

common::Result<DistributedVectorPlanIntent>
decode_distributed_vector_plan_intent_exact(const common::ByteView bytes,
                                            const DistributedVectorPlanDecodeLimits limits) {
  if (limits.maximum_input_columns == 0U ||
      limits.maximum_input_columns > distributed_vector_plan_format::kMaximumInputColumns ||
      limits.maximum_output_columns == 0U ||
      limits.maximum_output_columns > distributed_vector_plan_format::kMaximumOutputColumns ||
      limits.maximum_row_outputs == 0U ||
      limits.maximum_row_outputs > distributed_vector_plan_format::kMaximumRowOutputs ||
      limits.maximum_visible_row_outputs >
          distributed_vector_plan_format::kMaximumVisibleRowOutputs ||
      limits.maximum_group_keys == 0U ||
      limits.maximum_group_keys > distributed_vector_plan_format::kMaximumGroupKeys ||
      limits.maximum_aggregates == 0U ||
      limits.maximum_aggregates > distributed_vector_plan_format::kMaximumAggregates ||
      limits.maximum_order_keys > distributed_vector_plan_format::kMaximumOrderKeys) {
    return common::make_unexpected(invalid("distributed vector plan decode limits are invalid"));
  }
  if (bytes.size() < distributed_vector_plan_format::kHeaderLength +
                         distributed_vector_plan_format::kTrailerLength ||
      bytes.size() > distributed_vector_plan_format::kMaximumFrameLength)
    return common::make_unexpected(corruption("distributed vector plan frame length is invalid"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed vector plan magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto header_crc = header_crc_reader.read_u32_le();
  if (!header_crc.has_value() || *header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corruption("distributed vector plan header checksum is invalid"));

  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto mode = reader.read_u8();
  const auto flags = reader.read_u8();
  const auto visible_count = reader.read_u16_le();
  const auto row_count = reader.read_u32_le();
  const auto group_count = reader.read_u32_le();
  const auto aggregate_count = reader.read_u32_le();
  const auto order_count = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !mode.has_value() || !flags.has_value() ||
      !visible_count.has_value() || !row_count.has_value() || !group_count.has_value() ||
      !aggregate_count.has_value() || !order_count.has_value()) {
    return common::make_unexpected(corruption("distributed vector plan header is truncated"));
  }
  if (*major != distributed_vector_plan_format::kMajor ||
      (*minor != distributed_vector_plan_format::kLegacyMinor &&
       *minor != distributed_vector_plan_format::kMinor)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed vector plan version is unsupported"});
  }
  if (*header_length != distributed_vector_plan_format::kHeaderLength ||
      *frame_length != bytes.size() || (*flags & ~kLimitPresent) != 0U ||
      (*minor == distributed_vector_plan_format::kLegacyMinor) != (*visible_count == 0U) ||
      *row_count > distributed_vector_plan_format::kMaximumRowOutputs ||
      *visible_count > distributed_vector_plan_format::kMaximumVisibleRowOutputs ||
      *group_count > distributed_vector_plan_format::kMaximumGroupKeys ||
      *aggregate_count > distributed_vector_plan_format::kMaximumAggregates ||
      *order_count > distributed_vector_plan_format::kMaximumOrderKeys) {
    return common::make_unexpected(corruption("distributed vector plan header is noncanonical"));
  }
  if (*row_count > limits.maximum_row_outputs ||
      *visible_count > limits.maximum_visible_row_outputs ||
      *group_count > limits.maximum_group_keys || *aggregate_count > limits.maximum_aggregates ||
      *order_count > limits.maximum_order_keys) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed vector plan exceeds caller limits"});
  }
  const std::size_t expected_length =
      distributed_vector_plan_format::kHeaderLength + static_cast<std::size_t>(*row_count) * 4U +
      static_cast<std::size_t>(*visible_count) * 4U + static_cast<std::size_t>(*group_count) * 4U +
      static_cast<std::size_t>(*aggregate_count) * 8U +
      static_cast<std::size_t>(*order_count) * 8U + ((*flags & kLimitPresent) != 0U ? 8U : 0U) +
      distributed_vector_plan_format::kTrailerLength;
  if (bytes.size() != expected_length)
    return common::make_unexpected(corruption("distributed vector plan encoded length is invalid"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto frame_crc = trailer.read_u32_le();
  if (!frame_crc.has_value() || *frame_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed vector plan checksum is invalid"));

  try {
    DistributedVectorPlanIntent intent{.mode = static_cast<DistributedVectorPlanMode>(*mode),
                                       .row_output_indices = {},
                                       .visible_row_output_indices = {},
                                       .group_key_input_indices = {},
                                       .aggregates = {},
                                       .order_keys = {},
                                       .limit = std::nullopt};
    intent.row_output_indices.reserve(*row_count);
    intent.visible_row_output_indices.reserve(*visible_count);
    intent.group_key_input_indices.reserve(*group_count);
    intent.aggregates.reserve(*aggregate_count);
    intent.order_keys.reserve(*order_count);
    for (std::uint32_t index = 0U; index < *row_count; ++index) {
      const auto value = reader.read_u32_le();
      if (!value.has_value())
        return common::make_unexpected(corruption("distributed vector row output is truncated"));
      intent.row_output_indices.push_back(*value);
    }
    for (std::uint32_t index = 0U; index < *visible_count; ++index) {
      const auto value = reader.read_u32_le();
      if (!value.has_value()) {
        return common::make_unexpected(
            corruption("distributed vector visible row output is truncated"));
      }
      intent.visible_row_output_indices.push_back(*value);
    }
    for (std::uint32_t index = 0U; index < *group_count; ++index) {
      const auto value = reader.read_u32_le();
      if (!value.has_value())
        return common::make_unexpected(corruption("distributed vector group key is truncated"));
      intent.group_key_input_indices.push_back(*value);
    }
    for (std::uint32_t index = 0U; index < *aggregate_count; ++index) {
      const auto operation = reader.read_u8();
      const auto input_present = reader.read_u8();
      const auto aggregate_reserved = reader.read_exact(2U);
      const auto input = reader.read_u32_le();
      if (!operation.has_value() || !input_present.has_value() || !aggregate_reserved.has_value() ||
          !input.has_value() || *input_present > 1U ||
          !std::ranges::all_of(*aggregate_reserved,
                               [](const std::byte value) { return value == std::byte{}; }) ||
          (*input_present == 0U && *input != 0U)) {
        return common::make_unexpected(
            corruption("distributed vector aggregate descriptor is noncanonical"));
      }
      intent.aggregates.push_back({.operation = static_cast<VectorAggregateOperation>(*operation),
                                   .input_index = *input_present != 0U
                                                      ? std::optional<std::uint32_t>{*input}
                                                      : std::nullopt});
    }
    for (std::uint32_t index = 0U; index < *order_count; ++index) {
      const auto output = reader.read_u32_le();
      const auto direction = reader.read_u8();
      const auto null_placement = reader.read_u8();
      const auto order_reserved = reader.read_exact(2U);
      if (!output.has_value() || !direction.has_value() || !null_placement.has_value() ||
          !order_reserved.has_value() ||
          !std::ranges::all_of(*order_reserved,
                               [](const std::byte value) { return value == std::byte{}; })) {
        return common::make_unexpected(
            corruption("distributed vector order descriptor is noncanonical"));
      }
      intent.order_keys.push_back(
          {.output_index = *output,
           .direction = static_cast<PhysicalSortDirection>(*direction),
           .null_placement = static_cast<ScalarNullPlacement>(*null_placement)});
    }
    if ((*flags & kLimitPresent) != 0U) {
      const auto limit = reader.read_u64_le();
      if (!limit.has_value())
        return common::make_unexpected(corruption("distributed vector limit is truncated"));
      intent.limit = *limit;
    }
    if (reader.remaining() != distributed_vector_plan_format::kTrailerLength)
      return common::make_unexpected(corruption("distributed vector plan has trailing payload"));
    const common::Status validation =
        validate_intent(intent, distributed_vector_plan_format::kMaximumInputColumns,
                        distributed_vector_plan_format::kMaximumOutputColumns);
    if (!validation.is_ok())
      return common::make_unexpected(corruption("distributed vector plan semantics are invalid"));
    const auto exceeds_input_limit = [&](const std::uint32_t input_index) {
      return input_index >= limits.maximum_input_columns;
    };
    if (std::ranges::any_of(intent.row_output_indices, exceeds_input_limit) ||
        std::ranges::any_of(intent.group_key_input_indices, exceeds_input_limit) ||
        std::ranges::any_of(intent.aggregates, [&](const DistributedVectorAggregateIntent& value) {
          return value.input_index.has_value() && exceeds_input_limit(*value.input_index);
        })) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "distributed vector plan input index exceeds caller column limit"});
    }
    const std::size_t output_columns =
        intent.mode == DistributedVectorPlanMode::kRows
            ? intent.row_output_indices.size()
            : intent.group_key_input_indices.size() + intent.aggregates.size();
    if (output_columns > limits.maximum_output_columns) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "distributed vector plan output width exceeds caller limit"});
    }
    return intent;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector plan decode allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector plan exceeds container limits"});
  }
}

} // namespace chronos::query
