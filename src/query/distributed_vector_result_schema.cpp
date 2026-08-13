#include "chronos/query/distributed_vector_result_schema.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <array>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::query {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                  std::byte{'V'}, std::byte{'R'}, std::byte{'S'},
                                                  std::byte{'C'}, std::byte{'1'}};
inline constexpr std::size_t kHeaderCrcOffset = 32U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

struct ColumnValidationLimits {
  std::uint32_t maximum_columns;
  std::uint32_t maximum_name_length;
};

[[nodiscard]] common::Status
validate_columns(const std::span<const DistributedVectorResultColumn> columns,
                 const ColumnValidationLimits limits) {
  if (columns.empty() || columns.size() > limits.maximum_columns)
    return invalid("distributed vector result schema width is invalid");
  for (const DistributedVectorResultColumn& column : columns) {
    const common::ByteView name =
        std::as_bytes(std::span<const char>{column.name.data(), column.name.size()});
    if (name.empty() || name.size() > limits.maximum_name_length || !schema::is_valid_utf8(name))
      return invalid("distributed vector result column name is invalid");
    const auto type = schema::LogicalType::create(column.type.kind(), column.type.parameter_0(),
                                                  column.type.parameter_1());
    if (!type.has_value() || *type != column.type)
      return invalid("distributed vector result column type is invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::vector<PhysicalColumnShape>>
output_shapes(const DistributedVectorPlanIntent& intent,
              const std::span<const PhysicalColumnShape> projected_inputs) {
  if (projected_inputs.size() > distributed_vector_plan_format::kMaximumInputColumns)
    return common::make_unexpected(invalid("distributed vector result input width is invalid"));
  const common::Status plan_status = validate_distributed_vector_plan_intent(
      intent, static_cast<std::uint32_t>(projected_inputs.size()));
  if (!plan_status.is_ok())
    return common::make_unexpected(plan_status);
  try {
    std::vector<PhysicalColumnShape> shapes;
    const std::size_t width =
        intent.mode == DistributedVectorPlanMode::kRows
            ? intent.row_output_indices.size()
            : intent.group_key_input_indices.size() + intent.aggregates.size();
    shapes.reserve(width);
    if (intent.mode == DistributedVectorPlanMode::kRows) {
      for (const std::uint32_t index : intent.row_output_indices)
        shapes.push_back(projected_inputs[index]);
      return shapes;
    }
    for (const std::uint32_t index : intent.group_key_input_indices)
      shapes.push_back(projected_inputs[index]);
    for (const DistributedVectorAggregateIntent& aggregate : intent.aggregates) {
      std::optional<VectorAggregateInput> input;
      if (aggregate.input_index.has_value()) {
        const PhysicalColumnShape shape = projected_inputs[*aggregate.input_index];
        input = VectorAggregateInput{*aggregate.input_index, shape.type, shape.nullable};
      }
      const auto shape = vector_aggregate_output_shape({aggregate.operation, input});
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      shapes.push_back({shape->type, shape->nullable});
    }
    return shapes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector result shape allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector result shape exceeds limits"));
  }
}

} // namespace

EncodedDistributedVectorResultSchema::EncodedDistributedVectorResultSchema(
    std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedDistributedVectorResultSchema::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedDistributedVectorResultSchema>
encode_distributed_vector_result_schema(const DistributedVectorResultSchema& value) {
  const common::Status validation = validate_columns(
      value.columns,
      {.maximum_columns = distributed_vector_result_schema_format::kMaximumColumns,
       .maximum_name_length = distributed_vector_result_schema_format::kMaximumNameLength});
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  std::size_t descriptor_bytes{};
  for (const DistributedVectorResultColumn& column : value.columns) {
    const auto next = common::checked_add(
        descriptor_bytes,
        distributed_vector_result_schema_format::kDescriptorFixedLength + column.name.size());
    if (!next.has_value())
      return common::make_unexpected(exhausted("distributed vector result schema size overflowed"));
    descriptor_bytes = *next;
  }
  const auto frame_length =
      common::checked_add(distributed_vector_result_schema_format::kHeaderLength +
                              distributed_vector_result_schema_format::kTrailerLength,
                          descriptor_bytes);
  if (!frame_length.has_value() ||
      *frame_length > distributed_vector_result_schema_format::kMaximumFrameLength)
    return common::make_unexpected(exhausted("distributed vector result schema is too large"));
  try {
    std::vector<std::byte> bytes(*frame_length);
    common::ByteWriter writer{bytes};
    common::Status status = writer.write_exact(kMagic);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_result_schema_format::kMajor);
    if (status.is_ok())
      status = writer.write_u16_le(distributed_vector_result_schema_format::kMinor);
    if (status.is_ok())
      status = writer.write_u32_le(distributed_vector_result_schema_format::kHeaderLength);
    if (status.is_ok())
      status = writer.write_u64_le(*frame_length);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(value.columns.size()));
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(descriptor_bytes));
    if (status.is_ok())
      status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (status.is_ok())
      status = writer.zero_fill(12U);
    for (const DistributedVectorResultColumn& column : value.columns) {
      const common::ByteView name =
          std::as_bytes(std::span<const char>{column.name.data(), column.name.size()});
      if (status.is_ok())
        status = writer.write_u16_le(column.type.code());
      if (status.is_ok())
        status = writer.write_u16_le(column.type.parameter_0());
      if (status.is_ok())
        status = writer.write_u16_le(column.type.parameter_1());
      if (status.is_ok())
        status = writer.write_u8(column.nullable ? 1U : 0U);
      if (status.is_ok())
        status = writer.zero_fill(1U);
      if (status.is_ok())
        status = writer.write_u32_le(static_cast<std::uint32_t>(name.size()));
      if (status.is_ok())
        status = writer.zero_fill(4U);
      if (status.is_ok())
        status = writer.write_exact(name);
    }
    if (status.is_ok())
      status =
          writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("distributed vector result schema layout failed"));
    return EncodedDistributedVectorResultSchema{std::move(bytes)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector result schema allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector result schema exceeds limits"));
  }
}

common::Result<DistributedVectorResultSchema> decode_distributed_vector_result_schema_exact(
    const common::ByteView bytes, const DistributedVectorResultSchemaDecodeLimits limits) {
  if (limits.maximum_frame_length <
          distributed_vector_result_schema_format::kHeaderLength +
              distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
              distributed_vector_result_schema_format::kTrailerLength ||
      limits.maximum_frame_length > distributed_vector_result_schema_format::kMaximumFrameLength ||
      limits.maximum_columns == 0U ||
      limits.maximum_columns > distributed_vector_result_schema_format::kMaximumColumns ||
      limits.maximum_name_length == 0U ||
      limits.maximum_name_length > distributed_vector_result_schema_format::kMaximumNameLength)
    return common::make_unexpected(invalid("distributed vector result schema limits are invalid"));
  if (bytes.size() < distributed_vector_result_schema_format::kHeaderLength +
                         distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
                         distributed_vector_result_schema_format::kTrailerLength ||
      bytes.size() > distributed_vector_result_schema_format::kMaximumFrameLength)
    return common::make_unexpected(
        corruption("distributed vector result schema length is invalid"));
  if (bytes.size() > limits.maximum_frame_length)
    return common::make_unexpected(
        exhausted("distributed vector result schema exceeds the caller frame limit"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("distributed vector result schema magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset)))
    return common::make_unexpected(
        corruption("distributed vector result schema header checksum differs"));
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto frame_length = reader.read_u64_le();
  const auto column_count = reader.read_u32_le();
  const auto descriptor_length = reader.read_u32_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(12U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !frame_length.has_value() || !column_count.has_value() || !descriptor_length.has_value() ||
      !reserved.has_value())
    return common::make_unexpected(
        corruption("distributed vector result schema header is truncated"));
  if (*major != distributed_vector_result_schema_format::kMajor ||
      *minor != distributed_vector_result_schema_format::kMinor)
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed vector result schema version is unsupported"});
  const auto minimum_descriptors = common::checked_multiply(
      static_cast<std::size_t>(*column_count),
      distributed_vector_result_schema_format::kDescriptorFixedLength + 1U);
  if (*header_length != distributed_vector_result_schema_format::kHeaderLength ||
      *frame_length != bytes.size() || *column_count == 0U ||
      *column_count > distributed_vector_result_schema_format::kMaximumColumns ||
      !minimum_descriptors.has_value() || *descriptor_length < *minimum_descriptors ||
      *descriptor_length != bytes.size() - distributed_vector_result_schema_format::kHeaderLength -
                                distributed_vector_result_schema_format::kTrailerLength ||
      !std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{}; }))
    return common::make_unexpected(
        corruption("distributed vector result schema header is invalid"));
  if (*column_count > limits.maximum_columns)
    return common::make_unexpected(
        exhausted("distributed vector result schema exceeds the caller column limit"));
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("distributed vector result schema checksum differs"));
  try {
    DistributedVectorResultSchema result;
    result.columns.reserve(*column_count);
    for (std::uint32_t index = 0U; index < *column_count; ++index) {
      const auto code = reader.read_u16_le();
      const auto parameter_0 = reader.read_u16_le();
      const auto parameter_1 = reader.read_u16_le();
      const auto nullable = reader.read_u8();
      const auto small_reserved = reader.read_u8();
      const auto name_length = reader.read_u32_le();
      const auto descriptor_reserved = reader.read_u32_le();
      if (!code.has_value() || !parameter_0.has_value() || !parameter_1.has_value() ||
          !nullable.has_value() || !small_reserved.has_value() || !name_length.has_value() ||
          !descriptor_reserved.has_value() || *nullable > 1U || *small_reserved != 0U ||
          *descriptor_reserved != 0U || *name_length == 0U ||
          *name_length > distributed_vector_result_schema_format::kMaximumNameLength)
        return common::make_unexpected(
            corruption("distributed vector result descriptor is invalid"));
      if (*name_length > limits.maximum_name_length)
        return common::make_unexpected(
            exhausted("distributed vector result name exceeds the caller limit"));
      const auto kind = schema::logical_type_kind_from_code(*code);
      if (!kind.has_value())
        return common::make_unexpected(corruption("distributed vector result type is unassigned"));
      const auto type = schema::LogicalType::create(*kind, *parameter_0, *parameter_1);
      const auto name = reader.read_exact(*name_length);
      if (!type.has_value() || !name.has_value() || !schema::is_valid_utf8(*name))
        return common::make_unexpected(
            corruption("distributed vector result descriptor is invalid"));
      // Character bytes may be inspected through char by the C++ aliasing rules.
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
      const std::string owned_name{reinterpret_cast<const char*>(name->data()), name->size()};
      result.columns.push_back({owned_name, *type, *nullable == 1U});
    }
    if (reader.remaining() != distributed_vector_result_schema_format::kTrailerLength)
      return common::make_unexpected(
          corruption("distributed vector result descriptors are noncanonical"));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed vector result schema allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed vector result schema exceeds limits"));
  }
}

common::Status validate_distributed_vector_result_schema_value(
    const DistributedVectorResultSchema& result_schema) {
  return validate_columns(
      result_schema.columns,
      {.maximum_columns = distributed_vector_result_schema_format::kMaximumColumns,
       .maximum_name_length = distributed_vector_result_schema_format::kMaximumNameLength});
}

common::Status validate_distributed_vector_result_schema(
    const DistributedVectorPlanIntent& intent,
    const std::span<const PhysicalColumnShape> projected_inputs,
    const DistributedVectorResultSchema& result_schema) {
  common::Status columns = validate_columns(
      result_schema.columns,
      {.maximum_columns = distributed_vector_result_schema_format::kMaximumColumns,
       .maximum_name_length = distributed_vector_result_schema_format::kMaximumNameLength});
  if (!columns.is_ok())
    return columns;
  const auto expected = output_shapes(intent, projected_inputs);
  if (!expected.has_value())
    return expected.error();
  if (expected->size() != result_schema.columns.size())
    return invalid("distributed vector result schema width differs from the plan");
  for (std::size_t index = 0U; index < expected->size(); ++index) {
    if ((*expected)[index].type != result_schema.columns[index].type ||
        (*expected)[index].nullable != result_schema.columns[index].nullable)
      return invalid("distributed vector result schema shape differs from the plan");
  }
  return common::Status::ok();
}

} // namespace chronos::query
