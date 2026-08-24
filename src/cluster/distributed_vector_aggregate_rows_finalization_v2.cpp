#include "chronos/cluster/distributed_vector_aggregate_rows_finalization_v2.hpp"

#include "../query/vector_expression_internal.hpp"
#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/query/distributed_vector_aggregate_state.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

struct BatchShape {
  std::uint32_t rows{};
  std::uint32_t columns{};
};

struct CanonicalCellScratch {
  std::array<std::byte, 1U> validity{};
  std::array<std::byte, 8U> offsets{};
  std::array<std::byte, 16U> fixed_values{};
};

struct ProductTerm {
  std::size_t count{};
  std::size_t width{};
};

[[nodiscard]] std::size_t tablet_insertion_index(const std::span<const schema::TabletId> tablets,
                                                 const schema::TabletId tablet) noexcept {
  std::size_t first{};
  std::size_t count = tablets.size();
  while (count != 0U) {
    const std::size_t step = count / 2U;
    const std::size_t middle = first + step;
    if (tablets[middle] < tablet) {
      first = middle + 1U;
      count -= step + 1U;
    } else {
      count = step;
    }
  }
  return first;
}

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool
valid_limits(const DistributedVectorAggregateRowsFinalizationLimitsV2& limits) noexcept {
  return limits.maximum_input_rows > 0U &&
         limits.maximum_input_rows <= kMaximumDistributedVectorAggregateRowsInputRowsV2 &&
         limits.maximum_input_messages > 0U &&
         limits.maximum_input_messages <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_input_encoded_bytes > 0U &&
         limits.maximum_input_encoded_bytes <= kMaximumDistributedVectorAggregateRowsBytesV2 &&
         limits.maximum_working_bytes > 0U &&
         limits.maximum_working_bytes <= kMaximumDistributedVectorAggregateRowsBytesV2 &&
         limits.maximum_query_memory_bytes > 0U &&
         limits.maximum_query_memory_bytes <=
             query::kMaximumDistributedVectorAggregateCoordinatorMemoryBytesV2 &&
         limits.maximum_variable_extremum_bytes > 0U &&
         limits.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes &&
         limits.input_batch.protocol.maximum_payload_size > 0U &&
         limits.input_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.input_batch.maximum_rows > 0U && limits.input_batch.maximum_columns > 0U &&
         limits.input_batch.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.input_batch.maximum_column_name_bytes > 0U &&
         limits.input_batch.maximum_column_name_bytes <=
             query::distributed_vector_result_schema_format::kMaximumNameLength;
}

[[nodiscard]] common::Result<BatchShape> batch_shape(const common::ByteView bytes) {
  if (bytes.size() < network::kQueryResultEnvelopeSize)
    return common::make_unexpected(corruption("aggregate row batch header is truncated"));
  common::ByteReader reader{bytes.first(network::kQueryResultEnvelopeSize)};
  const auto format = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto rows = reader.read_u32_le();
  const auto columns = reader.read_u32_le();
  const auto descriptor_bytes = reader.read_u32_le();
  if (!format.has_value() || !flags.has_value() || !rows.has_value() || !columns.has_value() ||
      !descriptor_bytes.has_value() || *format != network::kMessagePayloadFormat || *flags != 0U ||
      *columns == 0U || *descriptor_bytes > bytes.size() - network::kQueryResultEnvelopeSize) {
    return common::make_unexpected(corruption("aggregate row batch header is invalid"));
  }
  return BatchShape{.rows = *rows, .columns = *columns};
}

[[nodiscard]] bool
descriptors_match(const network::QueryResultBatchView& batch,
                  const query::DistributedVectorResultSchema& expected) noexcept {
  const auto columns = batch.columns();
  if (columns.size() != expected.columns.size())
    return false;
  for (std::size_t index = 0U; index < columns.size(); ++index) {
    if (columns[index].name != expected.columns[index].name ||
        columns[index].type != expected.columns[index].type ||
        columns[index].nullable != expected.columns[index].nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] common::Result<columnar::ColumnCellView>
canonical_cell_view(const schema::LogicalType type, const bool nullable,
                    const network::QueryResultCell& cell, CanonicalCellScratch& scratch) {
  if (cell.is_null && !nullable)
    return common::make_unexpected(corruption("aggregate row has NULL in a nonnullable column"));
  if (cell.is_null && !cell.value.empty())
    return common::make_unexpected(corruption("aggregate row NULL carries value bytes"));
  scratch.validity[0] = cell.is_null ? std::byte{} : std::byte{1U};
  columnar::ColumnVectorBufferView buffers;
  if (nullable)
    buffers.validity = scratch.validity;
  if (type.is_variable_width()) {
    if (cell.value.size() > std::numeric_limits<std::uint32_t>::max())
      return common::make_unexpected(corruption("aggregate row variable cell is too large"));
    common::ByteWriter writer{scratch.offsets};
    common::Status status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(cell.value.size()));
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(corruption("aggregate row offsets could not be built"));
    buffers.offsets = scratch.offsets;
    buffers.values = cell.value;
  } else {
    const std::size_t width = fixed_width(type.kind());
    if (width == 0U || width > scratch.fixed_values.size() ||
        (!cell.is_null && cell.value.size() != width)) {
      return common::make_unexpected(corruption("aggregate row fixed cell width is invalid"));
    }
    for (std::byte& value : scratch.fixed_values)
      value = std::byte{};
    if (!cell.is_null) {
      for (std::size_t index = 0U; index < cell.value.size(); ++index)
        scratch.fixed_values[index] = cell.value[index];
    }
    buffers.values = common::ByteView{scratch.fixed_values}.first(width);
  }
  auto physical = columnar::PhysicalColumnView::create(
      {.type = type, .nullable = nullable, .row_count = 1U, .null_count = cell.is_null ? 1U : 0U},
      buffers);
  if (!physical.has_value())
    return common::make_unexpected(corruption("aggregate row cell is not canonical"));
  auto view = physical->cell(0U);
  if (!view.has_value())
    return common::make_unexpected(corruption("aggregate row cell could not be inspected"));
  return *view;
}

[[nodiscard]] common::Result<std::size_t> add_product(const std::size_t current,
                                                      const ProductTerm term) {
  const auto bytes = common::checked_multiply(term.count, term.width);
  const auto total = bytes.has_value() ? common::checked_add(current, *bytes) : std::nullopt;
  if (!total.has_value())
    return common::make_unexpected(exhausted("aggregate row working size overflows"));
  return *total;
}

[[nodiscard]] common::Status
validate_predicate_sources(const query::VectorExpression& predicate,
                           const query::DistributedVectorResultSchema& input_schema) {
  if (predicate.result_shape().type.kind() != schema::LogicalTypeKind::kBool)
    return invalid("aggregate row predicate result is not Boolean");
  for (const query::VectorExpressionInstruction& instruction : predicate.instructions()) {
    const auto* source = std::get_if<query::VectorInputExpression>(&instruction);
    if (source == nullptr)
      continue;
    if (source->input_column_ordinal >= input_schema.columns.size())
      return invalid("aggregate row predicate source is out of bounds");
    const auto& input = input_schema.columns[source->input_column_ordinal];
    if (source->type != input.type || source->nullable != input.nullable)
      return invalid("aggregate row predicate source shape differs from its input");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<bool>
predicate_matches(const query::VectorExpression& predicate,
                  const std::span<const query::detail::CanonicalVectorExpressionCell> input) {
  auto value = query::detail::evaluate_canonical_vector_expression_row(predicate, input);
  if (!value.has_value())
    return common::make_unexpected(value.error());
  if (value->is_null())
    return false;
  const auto* boolean = std::get_if<bool>(&value->storage());
  if (boolean == nullptr)
    return common::make_unexpected(corruption("aggregate row predicate produced non-Boolean"));
  return *boolean;
}

} // namespace

static common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_impl_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const query::VectorExpression* const predicate,
    const DistributedVectorAggregateRowsFinalizationLimitsV2 limits) {
  try {
    if (!valid_limits(limits))
      return common::make_unexpected(invalid("aggregate row finalization limits are invalid"));
    const auto& input_schema = input.result.result_schema;
    if (input.plan.mode != query::DistributedVectorPlanMode::kRows ||
        input.plan.row_output_indices.size() != input_schema.columns.size() ||
        !input.plan.visible_row_output_indices.empty() ||
        !input.plan.group_key_input_indices.empty() || !input.plan.aggregates.empty() ||
        !input.plan.order_keys.empty() || input.plan.limit.has_value()) {
      return common::make_unexpected(
          invalid("aggregate input requires an unlimited identity row plan"));
    }
    for (std::size_t index = 0U; index < input.plan.row_output_indices.size(); ++index) {
      if (input.plan.row_output_indices[index] != index)
        return common::make_unexpected(invalid("aggregate input row projection is not identity"));
    }
    const common::Status input_schema_status =
        query::validate_distributed_vector_result_schema_value(input_schema);
    if (!input_schema_status.is_ok())
      return common::make_unexpected(input_schema_status);
    if (predicate != nullptr) {
      const common::Status predicate_status = validate_predicate_sources(*predicate, input_schema);
      if (!predicate_status.is_ok())
        return common::make_unexpected(predicate_status);
    }
    const common::Status row_plan_status = query::validate_distributed_vector_plan_intent(
        input.plan, static_cast<std::uint32_t>(input_schema.columns.size()),
        static_cast<std::uint32_t>(input_schema.columns.size()));
    if (!row_plan_status.is_ok())
      return common::make_unexpected(row_plan_status);
    const common::Status aggregate_plan_status = query::validate_distributed_vector_plan_intent(
        aggregate_plan, static_cast<std::uint32_t>(input_schema.columns.size()),
        static_cast<std::uint32_t>(aggregate_result_schema.columns.size()));
    if (!aggregate_plan_status.is_ok())
      return common::make_unexpected(aggregate_plan_status);
    if (aggregate_plan.mode != query::DistributedVectorPlanMode::kUngroupedAggregate)
      return common::make_unexpected(invalid("aggregate output plan is not ungrouped"));
    if (input.result.messages.empty())
      return common::make_unexpected(invalid("aggregate input has no tablet streams"));
    if (input.result.messages.size() > limits.maximum_input_messages)
      return common::make_unexpected(exhausted("aggregate input message limit is exhausted"));

    auto configuration_working =
        add_product(0U, {.count = input_schema.columns.size(),
                         .width = sizeof(query::PhysicalColumnShape) * 2U});
    if (!configuration_working.has_value())
      return common::make_unexpected(configuration_working.error());
    if (predicate != nullptr) {
      configuration_working =
          add_product(*configuration_working,
                      {.count = predicate->retained_configuration_bytes(), .width = 2U});
      if (!configuration_working.has_value())
        return common::make_unexpected(configuration_working.error());
      configuration_working =
          add_product(*configuration_working,
                      {.count = input_schema.columns.size(),
                       .width = sizeof(query::detail::CanonicalVectorExpressionCell) * 2U});
      if (!configuration_working.has_value())
        return common::make_unexpected(configuration_working.error());
    }
    configuration_working = add_product(
        *configuration_working,
        {.count = aggregate_plan.aggregates.size(),
         .width = (sizeof(query::VectorAggregateDefinition) +
                   sizeof(query::MergeableVectorAggregateState) + sizeof(query::ScalarValue)) *
                  2U});
    if (!configuration_working.has_value())
      return common::make_unexpected(configuration_working.error());
    configuration_working =
        add_product(*configuration_working, {.count = input.result.messages.size(),
                                             .width = sizeof(schema::TabletId) * 2U});
    if (!configuration_working.has_value() ||
        *configuration_working > limits.maximum_working_bytes) {
      return common::make_unexpected(exhausted("aggregate row working-memory limit is exhausted"));
    }

    std::vector<query::PhysicalColumnShape> input_shapes;
    input_shapes.reserve(input_schema.columns.size());
    for (const auto& column : input_schema.columns)
      input_shapes.push_back({.type = column.type, .nullable = column.nullable});
    const common::Status aggregate_schema_status = query::validate_distributed_vector_result_schema(
        aggregate_plan, input_shapes, aggregate_result_schema);
    if (!aggregate_schema_status.is_ok())
      return common::make_unexpected(aggregate_schema_status);
    std::vector<query::VectorAggregateDefinition> definitions;
    definitions.reserve(aggregate_plan.aggregates.size());
    for (const query::DistributedVectorAggregateIntent& aggregate : aggregate_plan.aggregates) {
      std::optional<query::VectorAggregateInput> aggregate_input;
      if (aggregate.input_index.has_value()) {
        const auto& column = input_schema.columns[*aggregate.input_index];
        aggregate_input = query::VectorAggregateInput{.column_ordinal = *aggregate.input_index,
                                                      .type = column.type,
                                                      .nullable = column.nullable};
      }
      definitions.push_back({.operation = aggregate.operation, .input = aggregate_input});
    }

    std::optional<schema::TabletId> current_tablet;
    common::Uuid query_id;
    std::uint64_t expected_sequence = 1U;
    bool current_terminal = false;
    std::vector<schema::TabletId> tablets;
    tablets.reserve(input.result.messages.size());
    std::uint64_t total_rows{};
    std::size_t input_encoded_bytes{};
    std::size_t maximum_batch_working{};
    constexpr std::size_t kExchangeOverhead =
        distributed_vector_result_exchange_v2_format::kHeaderLength +
        distributed_vector_result_exchange_v2_format::kTrailerLength;
    for (const DistributedVectorResultExchangeMessage& message : input.result.messages) {
      if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
        return common::make_unexpected(invalid("aggregate input identity is invalid"));
      if (query_id.is_nil())
        query_id = message.query_id;
      else if (message.query_id != query_id)
        return common::make_unexpected(invalid("aggregate input mixes query identities"));
      if (!current_tablet.has_value() || message.tablet_id != *current_tablet) {
        if (current_tablet.has_value() && !current_terminal)
          return common::make_unexpected(invalid("aggregate tablet stream is not terminal"));
        current_tablet = message.tablet_id;
        const std::size_t tablet_position = tablet_insertion_index(tablets, message.tablet_id);
        if (tablet_position != tablets.size() && tablets[tablet_position] == message.tablet_id)
          return common::make_unexpected(invalid("aggregate tablet stream is duplicated"));
        tablets.insert(tablets.begin() + static_cast<std::ptrdiff_t>(tablet_position),
                       message.tablet_id);
        expected_sequence = 1U;
      } else if (current_terminal) {
        return common::make_unexpected(invalid("aggregate tablet continues after terminal"));
      }
      if (message.sequence != expected_sequence)
        return common::make_unexpected(invalid("aggregate tablet sequence is not contiguous"));
      if (expected_sequence == std::numeric_limits<std::uint64_t>::max()) {
        if (!message.terminal)
          return common::make_unexpected(exhausted("aggregate tablet sequence is exhausted"));
      } else {
        ++expected_sequence;
      }
      current_terminal = message.terminal;
      if (message.encoded_result_batch.empty() && (!message.terminal || message.sequence != 1U))
        return common::make_unexpected(invalid("aggregate empty tablet stream is noncanonical"));
      const auto frame_size =
          common::checked_add(kExchangeOverhead, message.encoded_result_batch.size());
      const auto next_bytes = frame_size.has_value()
                                  ? common::checked_add(input_encoded_bytes, *frame_size)
                                  : std::nullopt;
      if (!next_bytes.has_value() || *next_bytes > limits.maximum_input_encoded_bytes)
        return common::make_unexpected(exhausted("aggregate input byte limit is exhausted"));
      input_encoded_bytes = *next_bytes;
      if (message.encoded_result_batch.empty())
        continue;
      auto shape = batch_shape(message.encoded_result_batch);
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      if (shape->columns != input_schema.columns.size())
        return common::make_unexpected(corruption("aggregate input batch width differs"));
      if (shape->rows > limits.input_batch.maximum_rows ||
          shape->columns > limits.input_batch.maximum_columns) {
        return common::make_unexpected(exhausted("aggregate input batch shape exceeds limits"));
      }
      if (shape->rows > limits.maximum_input_rows - total_rows)
        return common::make_unexpected(exhausted("aggregate input row limit is exhausted"));
      total_rows += shape->rows;
      const auto cells = common::checked_multiply(static_cast<std::size_t>(shape->rows),
                                                  static_cast<std::size_t>(shape->columns));
      if (!cells.has_value())
        return common::make_unexpected(exhausted("aggregate input cell count overflows"));
      auto batch_working =
          add_product(0U, {.count = *cells, .width = sizeof(network::QueryResultCell) * 2U});
      if (!batch_working.has_value())
        return common::make_unexpected(batch_working.error());
      batch_working =
          add_product(*batch_working,
                      {.count = shape->columns, .width = sizeof(network::QueryResultColumn) * 2U});
      if (!batch_working.has_value())
        return common::make_unexpected(batch_working.error());
      if (*batch_working > maximum_batch_working)
        maximum_batch_working = *batch_working;
    }
    if (!current_terminal)
      return common::make_unexpected(invalid("aggregate final tablet stream is not terminal"));

    const auto working = common::checked_add(*configuration_working, maximum_batch_working);
    if (!working.has_value() || *working > limits.maximum_working_bytes)
      return common::make_unexpected(exhausted("aggregate row working-memory limit is exhausted"));

    auto resources = query::QueryResourceContext::create(limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::vector<query::MergeableVectorAggregateState> states;
    states.reserve(definitions.size());
    for (const query::VectorAggregateDefinition& definition : definitions) {
      auto state = query::MergeableVectorAggregateState::create(
          definition, limits.maximum_variable_extremum_bytes);
      if (!state.has_value())
        return common::make_unexpected(state.error());
      states.push_back(std::move(*state));
    }
    std::vector<query::detail::CanonicalVectorExpressionCell> canonical_row;
    if (predicate != nullptr)
      canonical_row.resize(input_schema.columns.size());

    for (const DistributedVectorResultExchangeMessage& message : input.result.messages) {
      if (message.encoded_result_batch.empty())
        continue;
      auto batch =
          network::decode_query_result_batch(message.encoded_result_batch, limits.input_batch);
      if (!batch.has_value())
        return common::make_unexpected(batch.error());
      if (!descriptors_match(*batch, input_schema))
        return common::make_unexpected(corruption("aggregate input descriptors differ"));
      for (std::uint32_t row = 0U; row < batch->row_count(); ++row) {
        if (predicate != nullptr) {
          for (std::size_t column = 0U; column < canonical_row.size(); ++column) {
            const network::QueryResultCell* cell = batch->cell(row, column);
            if (cell == nullptr)
              return common::make_unexpected(corruption("aggregate predicate cell disappeared"));
            canonical_row[column] = {.is_null = cell->is_null, .bytes = cell->value};
          }
          auto matches = predicate_matches(*predicate, canonical_row);
          if (!matches.has_value())
            return common::make_unexpected(matches.error());
          if (!*matches)
            continue;
        }
        for (std::size_t ordinal = 0U; ordinal < states.size(); ++ordinal) {
          if (definitions[ordinal].operation == query::VectorAggregateOperation::kCountStar) {
            auto accumulated = states[ordinal].accumulate_count_star();
            if (!accumulated.has_value())
              return common::make_unexpected(accumulated.error());
            continue;
          }
          const std::size_t input_index = definitions[ordinal].input->column_ordinal;
          const network::QueryResultCell* raw = batch->cell(row, input_index);
          if (raw == nullptr)
            return common::make_unexpected(corruption("aggregate input cell disappeared"));
          CanonicalCellScratch scratch;
          auto cell = canonical_cell_view(definitions[ordinal].input->type,
                                          definitions[ordinal].input->nullable, *raw, scratch);
          if (!cell.has_value())
            return common::make_unexpected(cell.error());
          auto accumulated = states[ordinal].accumulate_cell(*cell, *resources);
          if (!accumulated.has_value())
            return common::make_unexpected(accumulated.error());
        }
      }
    }

    std::vector<query::ScalarValue> values;
    values.reserve(states.size());
    for (query::MergeableVectorAggregateState& state : states) {
      auto value = std::move(state).take_result();
      if (!value.has_value())
        return common::make_unexpected(value.error());
      values.push_back(std::move(*value));
    }
    query::DistributedVectorAggregateQueryResultV2 aggregate_result{
        .definitions = std::move(definitions),
        .result_schema = std::move(aggregate_result_schema),
        .values = std::move(values),
        .retained_encoded_bytes = input_encoded_bytes};
    return finalize_distributed_vector_aggregate_v2(aggregate_plan, std::move(aggregate_result),
                                                    limits.output);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate row finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate row finalization exceeds limits"));
  }
}

common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const DistributedVectorAggregateRowsFinalizationLimitsV2 limits) {
  return finalize_distributed_vector_aggregate_rows_impl_v2(
      std::move(input), aggregate_plan, std::move(aggregate_result_schema), nullptr, limits);
}

common::Result<DistributedVectorAggregateFinalizedResultV2>
finalize_distributed_vector_aggregate_rows_with_predicate_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorPlanIntent& aggregate_plan,
    query::DistributedVectorResultSchema&& aggregate_result_schema,
    const query::VectorExpression& predicate,
    const DistributedVectorAggregateRowsFinalizationLimitsV2 limits) {
  return finalize_distributed_vector_aggregate_rows_impl_v2(
      std::move(input), aggregate_plan, std::move(aggregate_result_schema), &predicate, limits);
}

} // namespace chronos::cluster
