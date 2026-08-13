#include "chronos/cluster/distributed_vector_aggregate_finalization_v2.hpp"

#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/query/value.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

inline constexpr std::size_t kConservativeOwnedAllocationOverheadBytes = std::size_t{8U} * 1024U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] bool
valid_limits(const DistributedVectorAggregateFinalizationLimitsV2& limits) noexcept {
  return limits.maximum_working_bytes > 0U &&
         limits.maximum_working_bytes <= kMaximumDistributedVectorAggregateFinalizationBytesV2 &&
         limits.maximum_output_encoded_bytes > 0U &&
         limits.maximum_output_encoded_bytes <=
             kMaximumDistributedVectorAggregateFinalizationBytesV2 &&
         limits.output_batch.protocol.maximum_payload_size > 0U &&
         limits.output_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.output_batch.maximum_rows > 0U && limits.output_batch.maximum_columns > 0U &&
         limits.output_batch.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.output_batch.maximum_column_name_bytes > 0U &&
         limits.output_batch.maximum_column_name_bytes <= 65'536U;
}

[[nodiscard]] common::Result<std::size_t> scalar_size(const query::ScalarValue& value,
                                                      const schema::LogicalTypeKind kind) {
  if (value.is_null())
    return std::size_t{};
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kBool:
    return std::get_if<bool>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{1U}
               : common::make_unexpected(invalid("vector aggregate Boolean storage is invalid"));
  case LogicalTypeKind::kInt8:
    return std::get_if<std::int64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{1U}
               : common::make_unexpected(invalid("vector aggregate signed storage is invalid"));
  case LogicalTypeKind::kInt16:
    return std::get_if<std::int64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{2U}
               : common::make_unexpected(invalid("vector aggregate signed storage is invalid"));
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate:
    return std::get_if<std::int64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{4U}
               : common::make_unexpected(invalid("vector aggregate signed storage is invalid"));
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
    return std::get_if<std::int64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{8U}
               : common::make_unexpected(invalid("vector aggregate signed storage is invalid"));
  case LogicalTypeKind::kUInt8:
    return std::get_if<std::uint64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{1U}
               : common::make_unexpected(invalid("vector aggregate unsigned storage is invalid"));
  case LogicalTypeKind::kUInt16:
    return std::get_if<std::uint64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{2U}
               : common::make_unexpected(invalid("vector aggregate unsigned storage is invalid"));
  case LogicalTypeKind::kUInt32:
    return std::get_if<std::uint64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{4U}
               : common::make_unexpected(invalid("vector aggregate unsigned storage is invalid"));
  case LogicalTypeKind::kUInt64:
    return std::get_if<std::uint64_t>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{8U}
               : common::make_unexpected(invalid("vector aggregate unsigned storage is invalid"));
  case LogicalTypeKind::kFloat32:
    return std::get_if<float>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{4U}
               : common::make_unexpected(invalid("vector aggregate FLOAT32 storage is invalid"));
  case LogicalTypeKind::kFloat64:
    return std::get_if<double>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{8U}
               : common::make_unexpected(invalid("vector aggregate FLOAT64 storage is invalid"));
  case LogicalTypeKind::kDecimal:
    return std::get_if<query::Decimal128Value>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{16U}
               : common::make_unexpected(invalid("vector aggregate DECIMAL storage is invalid"));
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString: {
    const auto* text = std::get_if<std::string>(&value.storage());
    return text != nullptr
               ? common::Result<std::size_t>{text->size()}
               : common::make_unexpected(invalid("vector aggregate text storage is invalid"));
  }
  case LogicalTypeKind::kBinary: {
    const auto* bytes = std::get_if<std::vector<std::byte>>(&value.storage());
    return bytes != nullptr
               ? common::Result<std::size_t>{bytes->size()}
               : common::make_unexpected(invalid("vector aggregate binary storage is invalid"));
  }
  case LogicalTypeKind::kUuid:
    return std::get_if<common::Uuid>(&value.storage()) != nullptr
               ? common::Result<std::size_t>{common::Uuid::kSize}
               : common::make_unexpected(invalid("vector aggregate UUID storage is invalid"));
  }
  return common::make_unexpected(invalid("vector aggregate scalar type is invalid"));
}

template <typename Value>
[[nodiscard]] const Value* storage(const query::ScalarValue& value, const char* message,
                                   common::Status& status) {
  const Value* result = std::get_if<Value>(&value.storage());
  if (result == nullptr)
    status = invalid(message);
  return result;
}

[[nodiscard]] common::Result<void> encode_scalar(const query::ScalarValue& value,
                                                 const common::MutableByteView bytes) {
  if (value.is_null())
    return bytes.empty() ? common::Result<void>{}
                         : common::make_unexpected(
                               invalid("vector aggregate NULL scalar has payload storage"));
  common::ByteWriter writer{bytes};
  common::Status status = common::Status::ok();
  using schema::LogicalTypeKind;
  // Scalar shape validation at the public boundary supplies this exact type.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  switch (value.type().value().kind()) {
  case LogicalTypeKind::kBool: {
    const bool* scalar =
        storage<bool>(value, "vector aggregate Boolean storage is invalid", status);
    if (status.is_ok())
      status = writer.write_u8(*scalar ? 1U : 0U);
    break;
  }
  case LogicalTypeKind::kInt8: {
    const std::int64_t* scalar =
        storage<std::int64_t>(value, "vector aggregate signed storage is invalid", status);
    if (status.is_ok())
      status = writer.write_i8(static_cast<std::int8_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kInt16: {
    const std::int64_t* scalar =
        storage<std::int64_t>(value, "vector aggregate signed storage is invalid", status);
    if (status.is_ok())
      status = writer.write_i16_le(static_cast<std::int16_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate: {
    const std::int64_t* scalar =
        storage<std::int64_t>(value, "vector aggregate signed storage is invalid", status);
    if (status.is_ok())
      status = writer.write_i32_le(static_cast<std::int32_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs: {
    const std::int64_t* scalar =
        storage<std::int64_t>(value, "vector aggregate signed storage is invalid", status);
    if (status.is_ok())
      status = writer.write_i64_le(*scalar);
    break;
  }
  case LogicalTypeKind::kUInt8: {
    const std::uint64_t* scalar =
        storage<std::uint64_t>(value, "vector aggregate unsigned storage is invalid", status);
    if (status.is_ok())
      status = writer.write_u8(static_cast<std::uint8_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kUInt16: {
    const std::uint64_t* scalar =
        storage<std::uint64_t>(value, "vector aggregate unsigned storage is invalid", status);
    if (status.is_ok())
      status = writer.write_u16_le(static_cast<std::uint16_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kUInt32: {
    const std::uint64_t* scalar =
        storage<std::uint64_t>(value, "vector aggregate unsigned storage is invalid", status);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(*scalar));
    break;
  }
  case LogicalTypeKind::kUInt64: {
    const std::uint64_t* scalar =
        storage<std::uint64_t>(value, "vector aggregate unsigned storage is invalid", status);
    if (status.is_ok())
      status = writer.write_u64_le(*scalar);
    break;
  }
  case LogicalTypeKind::kFloat32: {
    const float* scalar =
        storage<float>(value, "vector aggregate FLOAT32 storage is invalid", status);
    if (status.is_ok())
      status = writer.write_float32_le(*scalar);
    break;
  }
  case LogicalTypeKind::kFloat64: {
    const double* scalar =
        storage<double>(value, "vector aggregate FLOAT64 storage is invalid", status);
    if (status.is_ok())
      status = writer.write_float64_le(*scalar);
    break;
  }
  case LogicalTypeKind::kDecimal: {
    const query::Decimal128Value* scalar = storage<query::Decimal128Value>(
        value, "vector aggregate DECIMAL storage is invalid", status);
    if (status.is_ok())
      status = writer.write_exact(scalar->coefficient);
    break;
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString: {
    const std::string* scalar =
        storage<std::string>(value, "vector aggregate text storage is invalid", status);
    if (status.is_ok())
      status = writer.write_exact(std::as_bytes(std::span{scalar->data(), scalar->size()}));
    break;
  }
  case LogicalTypeKind::kBinary: {
    const std::vector<std::byte>* scalar = storage<std::vector<std::byte>>(
        value, "vector aggregate binary storage is invalid", status);
    if (status.is_ok())
      status = writer.write_exact(*scalar);
    break;
  }
  case LogicalTypeKind::kUuid: {
    const common::Uuid* scalar =
        storage<common::Uuid>(value, "vector aggregate UUID storage is invalid", status);
    if (status.is_ok())
      status = writer.write_exact(scalar->bytes());
    break;
  }
  }
  if (!status.is_ok() || !writer.full())
    return common::make_unexpected(status.is_ok() ? internal("vector aggregate scalar size changed")
                                                  : status);
  return {};
}

[[nodiscard]] common::Result<std::size_t>
descriptor_size(const query::DistributedVectorResultSchema& schema) {
  std::size_t total = network::kQueryResultEnvelopeSize;
  for (const query::DistributedVectorResultColumn& column : schema.columns) {
    const auto descriptor =
        common::checked_add(network::kQueryResultColumnEnvelopeSize, column.name.size());
    const auto next =
        descriptor.has_value() ? common::checked_add(total, *descriptor) : std::nullopt;
    if (!next.has_value())
      return common::make_unexpected(exhausted("vector aggregate descriptor size overflowed"));
    total = *next;
  }
  return total;
}

} // namespace

common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_v2(
    const query::DistributedVectorPlanIntent& plan,
    query::DistributedVectorAggregateQueryResultV2&& input,
    const DistributedVectorAggregateFinalizationLimitsV2 limits) {
  try {
    if (!valid_limits(limits))
      return common::make_unexpected(invalid("vector aggregate finalization limits are invalid"));
    if (plan.mode != query::DistributedVectorPlanMode::kUngroupedAggregate)
      return common::make_unexpected(
          invalid("vector aggregate finalization requires an ungrouped plan"));
    const common::Status schema_status =
        query::validate_distributed_vector_result_schema_value(input.result_schema);
    if (!schema_status.is_ok())
      return common::make_unexpected(schema_status);
    if (input.definitions.empty() || input.definitions.size() != plan.aggregates.size() ||
        input.definitions.size() != input.result_schema.columns.size() ||
        input.definitions.size() != input.values.size()) {
      return common::make_unexpected(invalid("vector aggregate finalization widths differ"));
    }
    const common::Status plan_status = query::validate_distributed_vector_plan_intent(
        plan, query::distributed_vector_plan_format::kMaximumInputColumns,
        static_cast<std::uint32_t>(input.definitions.size()));
    if (!plan_status.is_ok())
      return common::make_unexpected(plan_status);
    if (input.definitions.size() > limits.output_batch.maximum_columns)
      return common::make_unexpected(
          exhausted("vector aggregate output column limit is exhausted"));

    std::vector<std::size_t> scalar_sizes;
    scalar_sizes.reserve(input.values.size());
    auto planned_size = descriptor_size(input.result_schema);
    if (!planned_size.has_value())
      return common::make_unexpected(planned_size.error());
    const bool emits_row = plan.limit.value_or(1U) != 0U;
    std::size_t retained_scalar_bytes{};
    for (std::size_t ordinal = 0U; ordinal < input.definitions.size(); ++ordinal) {
      const query::DistributedVectorAggregateIntent& intent = plan.aggregates[ordinal];
      const query::VectorAggregateDefinition& definition = input.definitions[ordinal];
      std::optional<std::uint32_t> definition_input_index;
      if (definition.input.has_value()) {
        if (definition.input->column_ordinal >
            query::distributed_vector_aggregate_state_format::kMaximumInputColumnOrdinal) {
          return common::make_unexpected(
              invalid("vector aggregate definition input ordinal exceeds the wire limit"));
        }
        definition_input_index = static_cast<std::uint32_t>(definition.input->column_ordinal);
      }
      if (intent.operation != definition.operation ||
          intent.input_index != definition_input_index) {
        return common::make_unexpected(invalid("vector aggregate plan and definitions differ"));
      }
      const auto shape = query::vector_aggregate_output_shape(definition);
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      const query::DistributedVectorResultColumn& column = input.result_schema.columns[ordinal];
      const query::ScalarValue& value = input.values[ordinal];
      if (shape->type != column.type || shape->nullable != column.nullable ||
          value.type() != std::optional<schema::LogicalType>{column.type} ||
          (value.is_null() && !column.nullable) ||
          column.name.size() > limits.output_batch.maximum_column_name_bytes) {
        return common::make_unexpected(invalid("vector aggregate finalized scalar shape differs"));
      }
      auto size = scalar_size(value, column.type.kind());
      if (!size.has_value())
        return common::make_unexpected(size.error());
      scalar_sizes.push_back(*size);
      if (emits_row) {
        const auto next_scalar_bytes = common::checked_add(retained_scalar_bytes, *size);
        const auto cell_size = common::checked_add(std::size_t{4U}, *size);
        const auto next =
            cell_size.has_value() ? common::checked_add(*planned_size, *cell_size) : std::nullopt;
        if (!next_scalar_bytes.has_value() || !next.has_value())
          return common::make_unexpected(exhausted("vector aggregate output size overflowed"));
        retained_scalar_bytes = *next_scalar_bytes;
        *planned_size = *next;
      }
    }
    if (*planned_size > limits.output_batch.protocol.maximum_payload_size ||
        *planned_size > limits.maximum_output_encoded_bytes) {
      return common::make_unexpected(exhausted("vector aggregate output byte limit is exhausted"));
    }
    auto collection_bytes = common::checked_multiply(
        input.values.size(), (sizeof(network::QueryResultCell) +
                              sizeof(network::QueryResultColumn) + sizeof(std::size_t)) *
                                 2U);
    const auto collections_and_output = collection_bytes.has_value()
                                            ? common::checked_add(*collection_bytes, *planned_size)
                                            : std::nullopt;
    const auto with_scalar_bytes =
        collections_and_output.has_value()
            ? common::checked_add(*collections_and_output, retained_scalar_bytes)
            : std::nullopt;
    const auto working_bytes =
        with_scalar_bytes.has_value()
            ? common::checked_add(*with_scalar_bytes, kConservativeOwnedAllocationOverheadBytes)
            : std::nullopt;
    if (!working_bytes.has_value() || *working_bytes > limits.maximum_working_bytes)
      return common::make_unexpected(
          exhausted("vector aggregate working-memory limit is exhausted"));

    std::vector<network::QueryResultColumn> columns;
    columns.reserve(input.result_schema.columns.size());
    for (const query::DistributedVectorResultColumn& column : input.result_schema.columns)
      columns.push_back({.name = column.name, .type = column.type, .nullable = column.nullable});
    std::vector<std::byte> owned_cell_bytes;
    std::vector<network::QueryResultCell> cells;
    if (emits_row) {
      owned_cell_bytes.resize(retained_scalar_bytes);
      cells.reserve(input.values.size());
      std::size_t offset{};
      for (std::size_t ordinal = 0U; ordinal < input.values.size(); ++ordinal) {
        const common::MutableByteView destination =
            common::MutableByteView{owned_cell_bytes}.subspan(offset, scalar_sizes[ordinal]);
        auto encoded = encode_scalar(input.values[ordinal], destination);
        if (!encoded.has_value())
          return common::make_unexpected(encoded.error());
        cells.push_back({.is_null = input.values[ordinal].is_null(), .value = destination});
        offset += scalar_sizes[ordinal];
      }
      if (offset != owned_cell_bytes.size())
        return common::make_unexpected(internal("vector aggregate scalar offsets changed"));
    }
    auto encoded = network::encode_query_result_batch(emits_row ? 1U : 0U, columns, cells,
                                                      limits.output_batch);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    if (encoded->size() != *planned_size)
      return common::make_unexpected(internal("vector aggregate output sizing changed"));
    return DistributedVectorAggregateFinalizedResultV2{.result_schema =
                                                           std::move(input.result_schema),
                                                       .encoded_batch = std::move(*encoded),
                                                       .row_count = emits_row ? 1U : 0U,
                                                       .encoded_bytes = *planned_size};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector aggregate finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector aggregate finalization exceeds limits"));
  }
}

} // namespace chronos::cluster
