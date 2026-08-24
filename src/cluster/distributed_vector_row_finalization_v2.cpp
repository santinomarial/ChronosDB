#include "chronos/cluster/distributed_vector_row_finalization_v2.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/value.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
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

struct RowReference {
  std::size_t batch_index{};
  std::uint32_t row{};
};

struct BatchRange {
  std::size_t begin{};
  std::size_t end{};
  std::size_t encoded_size{};
};

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] bool valid_limits(const DistributedVectorRowFinalizationLimitsV2& limits) noexcept {
  return limits.maximum_input_rows > 0U &&
         limits.maximum_input_rows <= kMaximumDistributedVectorRowFinalizationRowsV2 &&
         limits.maximum_input_messages > 0U &&
         limits.maximum_input_messages <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_input_encoded_bytes > 0U &&
         limits.maximum_input_encoded_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.maximum_working_bytes > 0U &&
         limits.maximum_working_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.maximum_output_batches > 0U &&
         limits.maximum_output_batches <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_output_encoded_bytes > 0U &&
         limits.maximum_output_encoded_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.output_batch.protocol.maximum_payload_size > 0U &&
         limits.output_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.output_batch.maximum_rows > 0U && limits.output_batch.maximum_columns > 0U &&
         limits.output_batch.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.output_batch.maximum_column_name_bytes > 0U &&
         limits.output_batch.maximum_column_name_bytes <= 65'536U;
}

[[nodiscard]] common::Result<BatchShape> batch_shape(const common::ByteView bytes) {
  if (bytes.size() < network::kQueryResultEnvelopeSize)
    return common::make_unexpected(corruption("vector row result batch header is truncated"));
  common::ByteReader reader{bytes.first(network::kQueryResultEnvelopeSize)};
  const auto format = reader.read_u16_le();
  const auto flags = reader.read_u16_le();
  const auto rows = reader.read_u32_le();
  const auto columns = reader.read_u32_le();
  const auto descriptor_bytes = reader.read_u32_le();
  if (!format.has_value() || !flags.has_value() || !rows.has_value() || !columns.has_value() ||
      !descriptor_bytes.has_value() || *format != network::kMessagePayloadFormat || *flags != 0U ||
      *columns == 0U || *descriptor_bytes > bytes.size() - network::kQueryResultEnvelopeSize) {
    return common::make_unexpected(corruption("vector row result batch header is invalid"));
  }
  return BatchShape{.rows = *rows, .columns = *columns};
}

[[nodiscard]] bool
descriptors_match(const network::QueryResultBatchView& batch,
                  const query::DistributedVectorResultSchema& expected_schema) noexcept {
  const auto columns = batch.columns();
  if (columns.size() != expected_schema.columns.size())
    return false;
  for (std::size_t index = 0U; index < columns.size(); ++index) {
    const auto& expected = expected_schema.columns[index];
    if (columns[index].name != expected.name || columns[index].type != expected.type ||
        columns[index].nullable != expected.nullable) {
      return false;
    }
  }
  return true;
}

// Accumulator, element count, and element width have intrinsically distinct arithmetic roles.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<std::size_t>
add_product(const std::size_t current, const std::size_t count, const std::size_t element_size) {
  const auto bytes = common::checked_multiply(count, element_size);
  if (!bytes.has_value())
    return common::make_unexpected(exhausted("vector row finalization state size overflowed"));
  const auto total = common::checked_add(current, *bytes);
  if (!total.has_value())
    return common::make_unexpected(exhausted("vector row finalization state size overflowed"));
  return *total;
}
// NOLINTEND(bugprone-easily-swappable-parameters)

[[nodiscard]] common::Result<std::size_t>
working_bytes(const std::size_t messages, const std::size_t batches, const std::size_t columns,
              const std::size_t cells, const std::size_t rows, const bool sorts,
              const std::size_t maximum_output_batches) {
  std::size_t total{};
  auto next = add_product(total, messages, sizeof(schema::TabletId) * 2U);
  if (!next.has_value())
    return next;
  total = *next;
  next = add_product(total, batches, sizeof(network::QueryResultBatchView) * 2U);
  if (!next.has_value())
    return next;
  total = *next;
  next = add_product(total, columns, sizeof(network::QueryResultColumn) * 2U);
  if (!next.has_value())
    return next;
  total = *next;
  // Decoded-cell ownership plus one output-batch staging vector, both with conservative capacity
  // headroom. Neither path allocates per row.
  next = add_product(total, cells, sizeof(network::QueryResultCell) * 4U);
  if (!next.has_value())
    return next;
  total = *next;
  next = add_product(total, rows, sizeof(RowReference) * (sorts ? 4U : 2U));
  if (!next.has_value())
    return next;
  total = *next;
  return add_product(total, maximum_output_batches, sizeof(BatchRange) * 2U);
}

[[nodiscard]] common::Result<std::size_t>
visibility_working_bytes(const query::DistributedVectorPlanIntent& plan,
                         const query::DistributedVectorResultSchema& schema,
                         const query::DistributedVectorRowCoordinatorProjection* projection) {
  const std::size_t visible_columns =
      projection != nullptr
          ? projection->outputs.size()
          : (plan.visible_row_output_indices.empty() ? plan.row_output_indices.size()
                                                     : plan.visible_row_output_indices.size());
  auto total = add_product(0U, visible_columns, sizeof(std::uint32_t) * 2U);
  if (!total.has_value())
    return total;
  total = add_product(*total, visible_columns, sizeof(query::DistributedVectorResultColumn) * 2U);
  if (!total.has_value())
    return total;
  if (projection != nullptr) {
    for (std::size_t position = 0U; position < visible_columns; ++position) {
      auto next = add_product(*total, projection->result_schema.columns[position].name.size(), 2U);
      if (!next.has_value())
        return next;
      total = next;
      if (const auto* constant = std::get_if<query::DistributedVectorRowConstantOutput>(
              &projection->outputs[position]);
          constant != nullptr) {
        next = add_product(*total, constant->canonical_value.size(), 2U);
        if (!next.has_value())
          return next;
        total = next;
      }
    }
    return total;
  }
  for (std::size_t position = 0U; position < visible_columns; ++position) {
    const std::size_t index = plan.visible_row_output_indices.empty()
                                  ? position
                                  : plan.visible_row_output_indices[position];
    if (index >= schema.columns.size())
      return common::make_unexpected(internal("visible vector row output escaped its schema"));
    auto next = add_product(*total, schema.columns[index].name.size(), 2U);
    if (!next.has_value())
      return next;
    total = next;
  }
  return total;
}

[[nodiscard]] common::Status
validate_projection(const query::DistributedVectorRowCoordinatorProjection& projection,
                    const query::DistributedVectorResultSchema& worker_schema) {
  if (projection.outputs.empty() ||
      projection.outputs.size() != projection.result_schema.columns.size())
    return invalid("vector row coordinator projection shape is invalid");
  common::Status schema_status =
      query::validate_distributed_vector_result_schema_value(projection.result_schema);
  if (!schema_status.is_ok())
    return schema_status;
  for (std::size_t index = 0U; index < projection.outputs.size(); ++index) {
    const auto& descriptor = projection.result_schema.columns[index];
    if (const auto* source =
            std::get_if<query::DistributedVectorRowSourceOutput>(&projection.outputs[index]);
        source != nullptr) {
      if (source->worker_output_index >= worker_schema.columns.size())
        return invalid("vector row coordinator source output is out of bounds");
      const auto& worker = worker_schema.columns[source->worker_output_index];
      if (worker.type != descriptor.type || worker.nullable != descriptor.nullable)
        return invalid("vector row coordinator source shape differs from its worker output");
      continue;
    }
    const auto& constant =
        std::get<query::DistributedVectorRowConstantOutput>(projection.outputs[index]);
    if ((constant.is_null && !descriptor.nullable) ||
        (constant.is_null && !constant.canonical_value.empty()))
      return invalid("vector row coordinator NULL constant shape is invalid");
    auto canonical = query::compare_canonical_scalar_bytes(
        descriptor.type, constant.is_null, constant.canonical_value, constant.is_null,
        constant.canonical_value, query::ScalarNullPlacement::kLast);
    if (!canonical.has_value())
      return invalid("vector row coordinator constant bytes are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<int>
compare_rows(const std::vector<network::QueryResultBatchView>& batches,
             const query::DistributedVectorResultSchema& schema,
             const std::span<const query::DistributedVectorOrderKey> keys, const RowReference left,
             const RowReference right) {
  if (left.batch_index >= batches.size() || right.batch_index >= batches.size())
    return common::make_unexpected(internal("vector row sort references an absent batch"));
  for (const query::DistributedVectorOrderKey& key : keys) {
    if (key.output_index >= schema.columns.size())
      return common::make_unexpected(internal("vector row sort key escaped its schema"));
    const network::QueryResultCell* left_cell =
        batches[left.batch_index].cell(left.row, key.output_index);
    const network::QueryResultCell* right_cell =
        batches[right.batch_index].cell(right.row, key.output_index);
    if (left_cell == nullptr || right_cell == nullptr)
      return common::make_unexpected(internal("vector row sort cell disappeared"));
    auto comparison = query::compare_canonical_scalar_bytes(
        schema.columns[key.output_index].type, left_cell->is_null, left_cell->value,
        right_cell->is_null, right_cell->value, key.null_placement);
    if (!comparison.has_value())
      return common::make_unexpected(comparison.error());
    const bool compared_null = left_cell->is_null || right_cell->is_null;
    if (!compared_null && key.direction == query::PhysicalSortDirection::kDescending)
      *comparison = -*comparison;
    if (*comparison != 0)
      return *comparison;
  }
  return 0;
}

[[nodiscard]] common::Result<void>
stable_sort_rows(std::vector<RowReference>& rows, std::vector<RowReference>& scratch,
                 const std::vector<network::QueryResultBatchView>& batches,
                 const query::DistributedVectorResultSchema& schema,
                 const std::span<const query::DistributedVectorOrderKey> keys) {
  scratch.resize(rows.size());
  bool rows_are_source = true;
  for (std::size_t width = 1U; width < rows.size();) {
    std::vector<RowReference>& source = rows_are_source ? rows : scratch;
    std::vector<RowReference>& destination = rows_are_source ? scratch : rows;
    for (std::size_t begin = 0U; begin < rows.size();) {
      const std::size_t middle = std::min(rows.size(), begin + width);
      const std::size_t run_width = std::min(rows.size() - begin, width * 2U);
      const std::size_t end = begin + run_width;
      std::size_t left = begin;
      std::size_t right = middle;
      std::size_t output = begin;
      while (left < middle && right < end) {
        auto order = compare_rows(batches, schema, keys, source[left], source[right]);
        if (!order.has_value())
          return common::make_unexpected(order.error());
        destination[output++] = *order <= 0 ? source[left++] : source[right++];
      }
      while (left < middle)
        destination[output++] = source[left++];
      while (right < end)
        destination[output++] = source[right++];
      begin = end;
    }
    rows_are_source = !rows_are_source;
    width = width > rows.size() / 2U ? rows.size() : width * 2U;
  }
  if (!rows_are_source)
    rows.swap(scratch);
  return {};
}

[[nodiscard]] common::Result<std::size_t>
descriptor_prefix_size(const query::DistributedVectorResultSchema& schema) {
  std::size_t total = network::kQueryResultEnvelopeSize;
  for (const auto& column : schema.columns) {
    const auto descriptor =
        common::checked_add(network::kQueryResultColumnEnvelopeSize, column.name.size());
    if (!descriptor.has_value())
      return common::make_unexpected(exhausted("vector row descriptor size overflowed"));
    const auto next = common::checked_add(total, *descriptor);
    if (!next.has_value())
      return common::make_unexpected(exhausted("vector row descriptor size overflowed"));
    total = *next;
  }
  return total;
}

[[nodiscard]] common::Result<std::size_t>
encoded_row_size(const std::vector<network::QueryResultBatchView>& batches,
                 const std::span<const std::uint32_t> output_indices,
                 const query::DistributedVectorRowCoordinatorProjection* projection,
                 const RowReference row) {
  if (row.batch_index >= batches.size())
    return common::make_unexpected(internal("vector row output references an absent batch"));
  std::size_t total{};
  if (projection != nullptr) {
    for (const query::DistributedVectorRowCoordinatorOutput& output : projection->outputs) {
      std::size_t value_size{};
      if (const auto* source = std::get_if<query::DistributedVectorRowSourceOutput>(&output);
          source != nullptr) {
        const network::QueryResultCell* cell =
            batches[row.batch_index].cell(row.row, source->worker_output_index);
        if (cell == nullptr)
          return common::make_unexpected(internal("vector row output cell disappeared"));
        value_size = cell->value.size();
      } else {
        value_size =
            std::get<query::DistributedVectorRowConstantOutput>(output).canonical_value.size();
      }
      const auto cell_size = common::checked_add(std::size_t{4U}, value_size);
      const auto next =
          cell_size.has_value() ? common::checked_add(total, *cell_size) : std::nullopt;
      if (!next.has_value())
        return common::make_unexpected(exhausted("vector row output size overflowed"));
      total = *next;
    }
    return total;
  }
  for (const std::uint32_t column : output_indices) {
    const network::QueryResultCell* cell = batches[row.batch_index].cell(row.row, column);
    if (cell == nullptr)
      return common::make_unexpected(internal("vector row output cell disappeared"));
    const auto cell_size = common::checked_add(std::size_t{4U}, cell->value.size());
    if (!cell_size.has_value())
      return common::make_unexpected(exhausted("vector row output size overflowed"));
    const auto next = common::checked_add(total, *cell_size);
    if (!next.has_value())
      return common::make_unexpected(exhausted("vector row output size overflowed"));
    total = *next;
  }
  return total;
}

} // namespace

[[nodiscard]] common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_rows_impl(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorRowCoordinatorProjection* projection,
    const DistributedVectorRowFinalizationLimitsV2 limits) {
  try {
    if (!valid_limits(limits))
      return common::make_unexpected(invalid("vector row finalization limits are invalid"));
    if (input.plan.mode != query::DistributedVectorPlanMode::kRows)
      return common::make_unexpected(invalid("vector row finalization requires a row-mode plan"));
    const common::Status schema_status =
        query::validate_distributed_vector_result_schema_value(input.result.result_schema);
    if (!schema_status.is_ok())
      return common::make_unexpected(schema_status);
    if (input.result.result_schema.columns.size() != input.plan.row_output_indices.size())
      return common::make_unexpected(invalid("vector row plan and result schema widths differ"));
    if (projection != nullptr && !input.plan.visible_row_output_indices.empty()) {
      return common::make_unexpected(
          invalid("vector row coordinator projection conflicts with worker visibility"));
    }
    if (projection != nullptr) {
      const common::Status projection_status =
          validate_projection(*projection, input.result.result_schema);
      if (!projection_status.is_ok())
        return common::make_unexpected(projection_status);
    }
    const common::Status plan_status = query::validate_distributed_vector_plan_intent(
        input.plan, query::distributed_vector_plan_format::kMaximumInputColumns,
        static_cast<std::uint32_t>(input.result.result_schema.columns.size()));
    if (!plan_status.is_ok())
      return common::make_unexpected(plan_status);
    auto visibility_bytes =
        visibility_working_bytes(input.plan, input.result.result_schema, projection);
    if (!visibility_bytes.has_value())
      return common::make_unexpected(visibility_bytes.error());
    if (*visibility_bytes > limits.maximum_working_bytes)
      return common::make_unexpected(exhausted("vector row working-memory limit is exhausted"));
    std::vector<std::uint32_t> visible_output_indices;
    query::DistributedVectorResultSchema visible_schema;
    if (projection != nullptr) {
      visible_schema = projection->result_schema;
    } else if (input.plan.visible_row_output_indices.empty()) {
      visible_output_indices.reserve(input.plan.row_output_indices.size());
      for (std::size_t index = 0U; index < input.plan.row_output_indices.size(); ++index)
        visible_output_indices.push_back(static_cast<std::uint32_t>(index));
    } else {
      visible_output_indices = input.plan.visible_row_output_indices;
    }
    const std::size_t visible_column_count =
        projection != nullptr ? projection->outputs.size() : visible_output_indices.size();
    if (visible_column_count > limits.output_batch.maximum_columns)
      return common::make_unexpected(exhausted("vector row output column limit is exhausted"));
    if (projection == nullptr) {
      visible_schema.columns.reserve(visible_output_indices.size());
      for (const std::uint32_t index : visible_output_indices) {
        if (index >= input.result.result_schema.columns.size()) {
          return common::make_unexpected(internal("visible vector row output escaped its schema"));
        }
        visible_schema.columns.push_back(input.result.result_schema.columns[index]);
      }
    }
    for (const auto& column : visible_schema.columns) {
      if (column.name.size() > limits.output_batch.maximum_column_name_bytes)
        return common::make_unexpected(exhausted("vector row output name limit is exhausted"));
    }
    auto prefix_size = descriptor_prefix_size(visible_schema);
    if (!prefix_size.has_value())
      return common::make_unexpected(prefix_size.error());
    if (*prefix_size > limits.output_batch.protocol.maximum_payload_size)
      return common::make_unexpected(exhausted("vector row output descriptors exceed one batch"));
    if (input.result.messages.empty())
      return common::make_unexpected(invalid("vector row input has no tablet streams"));
    if (input.result.messages.size() > limits.maximum_input_messages) {
      return common::make_unexpected(exhausted("vector row input message limit is exhausted"));
    }
    auto preliminary_state = working_bytes(input.result.messages.size(), 0U, 0U, 0U, 0U,
                                           !input.plan.order_keys.empty(), 0U);
    if (!preliminary_state.has_value())
      return common::make_unexpected(preliminary_state.error());
    if (*preliminary_state > limits.maximum_working_bytes - *visibility_bytes)
      return common::make_unexpected(exhausted("vector row working-memory limit is exhausted"));

    std::vector<schema::TabletId> tablet_segments;
    tablet_segments.reserve(input.result.messages.size());
    std::optional<schema::TabletId> current_tablet;
    common::Uuid query_id;
    std::uint64_t expected_sequence = 1U;
    bool current_terminal = false;
    std::size_t batch_count{};
    std::size_t total_columns{};
    std::size_t total_cells{};
    std::size_t input_encoded_bytes{};
    std::uint64_t total_rows{};
    constexpr std::size_t kExchangeOverhead =
        distributed_vector_result_exchange_v2_format::kHeaderLength +
        distributed_vector_result_exchange_v2_format::kTrailerLength;

    for (const DistributedVectorResultExchangeMessage& message : input.result.messages) {
      if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() ||
          message.sequence == 0U) {
        return common::make_unexpected(invalid("vector row input identity is invalid"));
      }
      if (query_id.is_nil())
        query_id = message.query_id;
      else if (message.query_id != query_id)
        return common::make_unexpected(invalid("vector row input mixes query identities"));
      if (!current_tablet.has_value() || message.tablet_id != *current_tablet) {
        if (current_tablet.has_value() && !current_terminal)
          return common::make_unexpected(invalid("vector row tablet stream is not terminal"));
        current_tablet = message.tablet_id;
        tablet_segments.push_back(message.tablet_id);
        expected_sequence = 1U;
      } else if (current_terminal) {
        return common::make_unexpected(
            invalid("vector row tablet stream continues after terminal"));
      }
      if (message.sequence != expected_sequence)
        return common::make_unexpected(invalid("vector row tablet sequence is not contiguous"));
      if (expected_sequence == std::numeric_limits<std::uint64_t>::max()) {
        if (!message.terminal)
          return common::make_unexpected(exhausted("vector row tablet sequence is exhausted"));
      } else {
        ++expected_sequence;
      }
      current_terminal = message.terminal;
      if (message.encoded_result_batch.empty() && (!message.terminal || message.sequence != 1U)) {
        return common::make_unexpected(invalid("vector row empty tablet stream is noncanonical"));
      }

      const auto frame_size =
          common::checked_add(kExchangeOverhead, message.encoded_result_batch.size());
      if (!frame_size.has_value())
        return common::make_unexpected(exhausted("vector row input byte size overflowed"));
      const auto next_input = common::checked_add(input_encoded_bytes, *frame_size);
      if (!next_input.has_value() || *next_input > limits.maximum_input_encoded_bytes)
        return common::make_unexpected(exhausted("vector row input byte limit is exhausted"));
      input_encoded_bytes = *next_input;

      if (message.encoded_result_batch.empty())
        continue;
      auto shape = batch_shape(message.encoded_result_batch);
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      if (shape->columns != input.result.result_schema.columns.size())
        return common::make_unexpected(corruption("vector row input batch width is invalid"));
      if (shape->rows > limits.maximum_input_rows - total_rows)
        return common::make_unexpected(exhausted("vector row input row limit is exhausted"));
      total_rows += shape->rows;
      const auto cells = common::checked_multiply(static_cast<std::size_t>(shape->rows),
                                                  static_cast<std::size_t>(shape->columns));
      const auto next_cells =
          cells.has_value() ? common::checked_add(total_cells, *cells) : std::nullopt;
      const auto next_columns =
          common::checked_add(total_columns, static_cast<std::size_t>(shape->columns));
      if (!next_cells.has_value() || !next_columns.has_value())
        return common::make_unexpected(exhausted("vector row decoded shape overflows"));
      total_cells = *next_cells;
      total_columns = *next_columns;
      ++batch_count;
    }
    if (!current_terminal)
      return common::make_unexpected(invalid("vector row final tablet stream is not terminal"));
    std::ranges::sort(tablet_segments);
    if (std::ranges::adjacent_find(tablet_segments) != tablet_segments.end())
      return common::make_unexpected(invalid("vector row tablet stream is duplicated"));

    const std::size_t maximum_planned_batches =
        std::min(limits.maximum_output_batches, static_cast<std::size_t>(total_rows) + 1U);
    auto state_bytes = working_bytes(input.result.messages.size(), batch_count, total_columns,
                                     total_cells, static_cast<std::size_t>(total_rows),
                                     !input.plan.order_keys.empty(), maximum_planned_batches);
    if (!state_bytes.has_value())
      return common::make_unexpected(state_bytes.error());
    if (*state_bytes > limits.maximum_working_bytes - *visibility_bytes)
      return common::make_unexpected(exhausted("vector row working-memory limit is exhausted"));

    network::QueryResultLimits input_batch_limits;
    input_batch_limits.maximum_rows = static_cast<std::uint32_t>(limits.maximum_input_rows);
    input_batch_limits.maximum_columns =
        query::distributed_vector_result_schema_format::kMaximumColumns;
    input_batch_limits.maximum_column_name_bytes = 65'536U;
    std::vector<network::QueryResultBatchView> batches;
    std::vector<RowReference> rows;
    batches.reserve(batch_count);
    rows.reserve(static_cast<std::size_t>(total_rows));
    for (const DistributedVectorResultExchangeMessage& message : input.result.messages) {
      if (message.encoded_result_batch.empty())
        continue;
      auto decoded =
          network::decode_query_result_batch(message.encoded_result_batch, input_batch_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (!descriptors_match(*decoded, input.result.result_schema))
        return common::make_unexpected(corruption("vector row input schema is inconsistent"));
      const std::size_t batch_index = batches.size();
      const std::uint32_t row_count = decoded->row_count();
      batches.push_back(std::move(*decoded));
      for (std::uint32_t row = 0U; row < row_count; ++row)
        rows.push_back({.batch_index = batch_index, .row = row});
    }
    if (rows.size() != total_rows)
      return common::make_unexpected(internal("vector row decoded count changed"));

    if (!input.plan.order_keys.empty() && rows.size() > 1U) {
      std::vector<RowReference> scratch;
      auto sorted = stable_sort_rows(rows, scratch, batches, input.result.result_schema,
                                     input.plan.order_keys);
      if (!sorted.has_value())
        return common::make_unexpected(sorted.error());
    }
    const std::uint64_t requested_rows = input.plan.limit.value_or(total_rows);
    const std::size_t selected_rows =
        static_cast<std::size_t>(std::min<std::uint64_t>(requested_rows, total_rows));
    rows.resize(selected_rows);

    std::vector<BatchRange> ranges;
    ranges.reserve(std::min(limits.maximum_output_batches, selected_rows + 1U));
    std::size_t output_encoded_bytes{};
    if (selected_rows == 0U) {
      ranges.push_back({.begin = 0U, .end = 0U, .encoded_size = *prefix_size});
      output_encoded_bytes = *prefix_size;
    } else {
      std::size_t begin{};
      while (begin < selected_rows) {
        std::size_t end = begin;
        std::size_t encoded_size = *prefix_size;
        while (end < selected_rows && end - begin < limits.output_batch.maximum_rows) {
          auto row_size = encoded_row_size(batches, visible_output_indices, projection, rows[end]);
          if (!row_size.has_value())
            return common::make_unexpected(row_size.error());
          const auto next = common::checked_add(encoded_size, *row_size);
          if (!next.has_value())
            return common::make_unexpected(exhausted("vector row output size overflowed"));
          if (*next > limits.output_batch.protocol.maximum_payload_size) {
            if (end == begin)
              return common::make_unexpected(
                  exhausted("one vector row exceeds the output payload limit"));
            break;
          }
          encoded_size = *next;
          ++end;
        }
        if (ranges.size() >= limits.maximum_output_batches)
          return common::make_unexpected(exhausted("vector row output batch limit is exhausted"));
        const auto next_output = common::checked_add(output_encoded_bytes, encoded_size);
        if (!next_output.has_value() || *next_output > limits.maximum_output_encoded_bytes)
          return common::make_unexpected(exhausted("vector row output byte limit is exhausted"));
        output_encoded_bytes = *next_output;
        ranges.push_back({.begin = begin, .end = end, .encoded_size = encoded_size});
        begin = end;
      }
    }
    if (output_encoded_bytes > limits.maximum_output_encoded_bytes)
      return common::make_unexpected(exhausted("vector row output byte limit is exhausted"));

    std::vector<network::QueryResultColumn> columns;
    columns.reserve(visible_schema.columns.size());
    for (const auto& column : visible_schema.columns)
      columns.push_back({.name = column.name, .type = column.type, .nullable = column.nullable});
    std::vector<std::vector<std::byte>> encoded_batches;
    encoded_batches.reserve(ranges.size());
    std::vector<network::QueryResultCell> cells;
    for (const BatchRange& range : ranges) {
      cells.clear();
      const std::size_t row_count = range.end - range.begin;
      const auto cell_count = common::checked_multiply(row_count, visible_column_count);
      if (!cell_count.has_value())
        return common::make_unexpected(exhausted("vector row output cell count overflowed"));
      cells.reserve(*cell_count);
      for (std::size_t index = range.begin; index < range.end; ++index) {
        const RowReference row = rows[index];
        if (projection != nullptr) {
          for (const query::DistributedVectorRowCoordinatorOutput& output : projection->outputs) {
            if (const auto* source = std::get_if<query::DistributedVectorRowSourceOutput>(&output);
                source != nullptr) {
              const network::QueryResultCell* cell =
                  batches[row.batch_index].cell(row.row, source->worker_output_index);
              if (cell == nullptr)
                return common::make_unexpected(internal("vector row output cell disappeared"));
              cells.push_back(*cell);
            } else {
              const auto& constant = std::get<query::DistributedVectorRowConstantOutput>(output);
              cells.push_back({.is_null = constant.is_null, .value = constant.canonical_value});
            }
          }
          continue;
        }
        for (const std::uint32_t column : visible_output_indices) {
          const network::QueryResultCell* cell = batches[row.batch_index].cell(row.row, column);
          if (cell == nullptr)
            return common::make_unexpected(internal("vector row output cell disappeared"));
          cells.push_back(*cell);
        }
      }
      auto encoded = network::encode_query_result_batch(static_cast<std::uint32_t>(row_count),
                                                        columns, cells, limits.output_batch);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() != range.encoded_size)
        return common::make_unexpected(internal("vector row output sizing changed"));
      encoded_batches.push_back(std::move(*encoded));
    }
    return DistributedVectorRowsFinalizedResultV2{.result_schema = std::move(visible_schema),
                                                  .encoded_batches = std::move(encoded_batches),
                                                  .row_count =
                                                      static_cast<std::uint64_t>(selected_rows),
                                                  .encoded_bytes = output_encoded_bytes};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("vector row finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("vector row finalization exceeds container limits"));
  }
}

common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_rows_v2(DistributedVectorQueryExecutionResultV2&& input,
                                    const DistributedVectorRowFinalizationLimitsV2 limits) {
  return finalize_distributed_vector_rows_impl(std::move(input), nullptr, limits);
}

common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_rows_with_projection_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::DistributedVectorRowCoordinatorProjection& projection,
    const DistributedVectorRowFinalizationLimitsV2 limits) {
  return finalize_distributed_vector_rows_impl(std::move(input), &projection, limits);
}

} // namespace chronos::cluster
