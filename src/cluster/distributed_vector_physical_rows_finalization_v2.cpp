#include "chronos/cluster/distributed_vector_physical_rows_finalization_v2.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/vector_chunk.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

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

[[nodiscard]] bool
valid_limits(const DistributedVectorPhysicalRowsFinalizationLimitsV2& limits) noexcept {
  return limits.maximum_input_rows > 0U &&
         limits.maximum_input_rows <= kMaximumDistributedVectorRowFinalizationRowsV2 &&
         limits.maximum_input_messages > 0U &&
         limits.maximum_input_messages <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_input_encoded_bytes > 0U &&
         limits.maximum_input_encoded_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.maximum_batch_working_bytes > 0U &&
         limits.maximum_batch_working_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.maximum_query_memory_bytes > 0U &&
         limits.maximum_query_memory_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.maximum_output_rows > 0U &&
         limits.maximum_output_rows <= kMaximumDistributedVectorRowFinalizationRowsV2 &&
         limits.maximum_output_batches > 0U &&
         limits.maximum_output_batches <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_output_encoded_bytes > 0U &&
         limits.maximum_output_encoded_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.input_batch.protocol.maximum_payload_size > 0U &&
         limits.input_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.input_batch.maximum_rows > 0U && limits.input_batch.maximum_columns > 0U &&
         limits.output_batch.protocol.maximum_payload_size > 0U &&
         limits.output_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.output_batch.maximum_rows > 0U && limits.output_batch.maximum_columns > 0U;
}

[[nodiscard]] bool
descriptors_match(const network::QueryResultBatchView& batch,
                  const query::DistributedVectorResultSchema& expected) noexcept {
  if (batch.columns().size() != expected.columns.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < expected.columns.size(); ++ordinal) {
    const network::QueryResultColumn& actual = batch.columns()[ordinal];
    const query::DistributedVectorResultColumn& wanted = expected.columns[ordinal];
    if (actual.name != wanted.name || actual.type != wanted.type ||
        actual.nullable != wanted.nullable) {
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

[[nodiscard]] common::Result<std::size_t>
materialized_batch_bytes(const network::QueryResultBatchView& batch,
                         const query::DistributedVectorResultSchema& expected_schema) {
  const std::size_t rows = batch.row_count();
  auto total = common::checked_multiply(rows, sizeof(std::uint32_t));
  if (!total.has_value())
    return common::make_unexpected(exhausted("physical row selection size overflows"));
  for (std::size_t column = 0U; column < expected_schema.columns.size(); ++column) {
    const auto& descriptor = expected_schema.columns[column];
    if (descriptor.nullable) {
      const auto next = common::checked_add(*total, columnar::bitmap_size(batch.row_count()));
      if (!next.has_value())
        return common::make_unexpected(exhausted("physical row validity size overflows"));
      total = next;
    }
    if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
      const auto next = common::checked_add(*total, columnar::bitmap_size(batch.row_count()));
      if (!next.has_value())
        return common::make_unexpected(exhausted("physical row Boolean size overflows"));
      total = next;
      continue;
    }
    if (descriptor.type.is_variable_width()) {
      const auto offsets = common::checked_multiply(rows + 1U, sizeof(std::uint32_t));
      if (!offsets.has_value())
        return common::make_unexpected(exhausted("physical row offsets size overflows"));
      auto next = common::checked_add(*total, *offsets);
      if (!next.has_value())
        return common::make_unexpected(exhausted("physical row offsets size overflows"));
      total = next;
      for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
        const network::QueryResultCell* cell = batch.cell(row, column);
        if (cell == nullptr)
          return common::make_unexpected(corruption("physical row cell is absent"));
        next = common::checked_add(*total, cell->value.size());
        if (!next.has_value())
          return common::make_unexpected(exhausted("physical row variable size overflows"));
        total = next;
      }
      continue;
    }
    const std::size_t width = fixed_width(descriptor.type.kind());
    const auto values = common::checked_multiply(rows, width);
    const auto next = values.has_value() ? common::checked_add(*total, *values) : std::nullopt;
    if (width == 0U || !next.has_value())
      return common::make_unexpected(exhausted("physical row fixed size overflows"));
    total = next;
  }
  return *total;
}

void set_bitmap(std::vector<std::byte>& bytes, const std::uint32_t row) noexcept {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint32_t{0xffU});
  }
}

[[nodiscard]] common::Result<query::AccountedVectorChunk>
materialize_batch(const network::QueryResultBatchView& batch,
                  const query::DistributedVectorResultSchema& expected_schema,
                  const DistributedVectorPhysicalRowsFinalizationLimitsV2& limits,
                  const query::QueryResourceContext& resources) {
  try {
    auto retained = materialized_batch_bytes(batch, expected_schema);
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_batch_working_bytes)
      return common::make_unexpected(exhausted("physical row batch working limit is exhausted"));
    auto reservation = resources.reserve(*retained);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());

    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(expected_schema.columns.size());
    for (std::size_t column = 0U; column < expected_schema.columns.size(); ++column) {
      const auto& descriptor = expected_schema.columns[column];
      columnar::ColumnVectorBuffers buffers;
      if (descriptor.nullable)
        buffers.validity.resize(columnar::bitmap_size(batch.row_count()));
      std::uint32_t null_count{};
      if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
        buffers.values.resize(columnar::bitmap_size(batch.row_count()));
      } else if (descriptor.type.is_variable_width()) {
        std::size_t payload_bytes{};
        for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
          const network::QueryResultCell* cell = batch.cell(row, column);
          if (cell == nullptr)
            return common::make_unexpected(corruption("physical row cell is absent"));
          const auto next = common::checked_add(payload_bytes, cell->value.size());
          if (!next.has_value() || *next > std::numeric_limits<std::uint32_t>::max()) {
            return common::make_unexpected(exhausted("physical row variable payload is too large"));
          }
          payload_bytes = *next;
        }
        buffers.offsets.resize((static_cast<std::size_t>(batch.row_count()) + 1U) *
                               sizeof(std::uint32_t));
        buffers.values.resize(payload_bytes);
      } else {
        buffers.values.resize(static_cast<std::size_t>(batch.row_count()) *
                              fixed_width(descriptor.type.kind()));
      }

      std::size_t variable_offset{};
      for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
        const network::QueryResultCell* cell = batch.cell(row, column);
        if (cell == nullptr)
          return common::make_unexpected(corruption("physical row cell is absent"));
        if (descriptor.type.is_variable_width())
          store_u32(buffers.offsets, static_cast<std::size_t>(row) * sizeof(std::uint32_t),
                    static_cast<std::uint32_t>(variable_offset));
        if (cell->is_null) {
          ++null_count;
          continue;
        }
        if (descriptor.nullable)
          set_bitmap(buffers.validity, row);
        if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
          if (cell->value.size() != 1U)
            return common::make_unexpected(corruption("physical row Boolean width is invalid"));
          if (cell->value.front() == std::byte{1U})
            set_bitmap(buffers.values, row);
        } else if (descriptor.type.is_variable_width()) {
          std::ranges::copy(cell->value,
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(variable_offset));
          variable_offset += cell->value.size();
        } else {
          const std::size_t width = fixed_width(descriptor.type.kind());
          if (cell->value.size() != width)
            return common::make_unexpected(corruption("physical row fixed width is invalid"));
          std::ranges::copy(cell->value,
                            buffers.values.begin() +
                                static_cast<std::ptrdiff_t>(static_cast<std::size_t>(row) * width));
        }
      }
      if (descriptor.type.is_variable_width()) {
        store_u32(buffers.offsets,
                  static_cast<std::size_t>(batch.row_count()) * sizeof(std::uint32_t),
                  static_cast<std::uint32_t>(variable_offset));
      }
      auto owned = columnar::OwnedPhysicalColumn::create({.type = descriptor.type,
                                                          .nullable = descriptor.nullable,
                                                          .row_count = batch.row_count(),
                                                          .null_count = null_count},
                                                         std::move(buffers));
      if (!owned.has_value())
        return common::make_unexpected(corruption("physical row column is not canonical"));
      columns.push_back(std::move(*owned));
    }
    auto selection = query::VectorSelection::all(batch.row_count());
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    auto chunk = query::VectorChunk::create(
        std::move(columns), std::move(*selection),
        {.maximum_rows = limits.input_batch.maximum_rows,
         .maximum_columns = limits.input_batch.maximum_columns,
         .maximum_buffer_bytes = limits.maximum_batch_working_bytes,
         .maximum_retained_buffer_bytes = limits.maximum_batch_working_bytes});
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    return query::AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation),
                                               resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical row batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical row batch exceeds container limits"));
  }
}

class NativeBatchSource final : public query::PhysicalOperator {
public:
  NativeBatchSource(DistributedVectorQueryResultV2 result,
                    DistributedVectorPhysicalRowsFinalizationLimitsV2 limits) noexcept
      : result_(std::move(result)), limits_(limits) {}

  [[nodiscard]] common::Result<query::PhysicalOperatorStep>
  next(const query::QueryResourceContext& resources) override {
    if (ended_)
      return query::PhysicalOperatorStep::end();
    while (message_index_ < result_.messages.size()) {
      const DistributedVectorResultExchangeMessage& message = result_.messages[message_index_++];
      if (message.encoded_result_batch.empty())
        continue;
      auto batch =
          network::decode_query_result_batch(message.encoded_result_batch, limits_.input_batch);
      if (!batch.has_value()) {
        ended_ = true;
        return common::make_unexpected(batch.error());
      }
      if (!descriptors_match(*batch, result_.result_schema)) {
        ended_ = true;
        return common::make_unexpected(corruption("physical row batch schema changed"));
      }
      if (batch->row_count() == 0U)
        continue;
      auto chunk = materialize_batch(*batch, result_.result_schema, limits_, resources);
      if (!chunk.has_value()) {
        ended_ = true;
        return common::make_unexpected(chunk.error());
      }
      return query::PhysicalOperatorStep::chunk(std::move(*chunk));
    }
    ended_ = true;
    return query::PhysicalOperatorStep::end();
  }

private:
  DistributedVectorQueryResultV2 result_;
  DistributedVectorPhysicalRowsFinalizationLimitsV2 limits_;
  std::size_t message_index_{};
  bool ended_{};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_chunk(const query::VectorChunk& chunk,
             const std::span<const network::QueryResultColumn> columns,
             const network::QueryResultLimits& limits) {
  if (chunk.column_count() != columns.size() ||
      chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(internal("physical result chunk shape differs"));
  }
  try {
    const auto cells_count = common::checked_multiply(chunk.selected_row_count(), columns.size());
    if (!cells_count.has_value())
      return common::make_unexpected(exhausted("physical result cell count overflows"));
    std::vector<network::QueryResultCell> cells;
    cells.reserve(*cells_count);
    std::vector<std::byte> booleans(*cells_count);
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      for (std::size_t column = 0U; column < columns.size(); ++column) {
        auto cell = chunk.cell({.column_ordinal = column, .selected_row = row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (cell->is_null()) {
          cells.push_back({.is_null = true});
        } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
          auto value = cell->boolean();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          const std::size_t ordinal = cells.size();
          booleans[ordinal] = *value ? std::byte{1U} : std::byte{};
          cells.push_back({.value = {&booleans[ordinal], 1U}});
        } else {
          auto value = cell->bytes();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          cells.push_back({.value = *value});
        }
      }
    }
    return network::encode_query_result_batch(
        static_cast<std::uint32_t>(chunk.selected_row_count()), columns, cells, limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical result exceeds container limits"));
  }
}

} // namespace

common::Result<DistributedVectorRowsFinalizedResultV2> finalize_distributed_vector_physical_rows_v2(
    DistributedVectorQueryExecutionResultV2&& input,
    const query::PhysicalPipelinePlan& coordinator_pipeline,
    query::DistributedVectorResultSchema&& result_schema,
    const DistributedVectorPhysicalRowsFinalizationLimitsV2 limits) {
  try {
    if (!valid_limits(limits))
      return common::make_unexpected(invalid("physical row finalization limits are invalid"));
    const auto& input_schema = input.result.result_schema;
    if (input.plan.mode != query::DistributedVectorPlanMode::kRows ||
        input.plan.row_output_indices.size() != input_schema.columns.size() ||
        !input.plan.visible_row_output_indices.empty() ||
        !input.plan.group_key_input_indices.empty() || !input.plan.aggregates.empty() ||
        !input.plan.order_keys.empty() || input.plan.limit.has_value()) {
      return common::make_unexpected(invalid("physical input requires an unlimited identity plan"));
    }
    for (std::size_t ordinal = 0U; ordinal < input.plan.row_output_indices.size(); ++ordinal) {
      if (input.plan.row_output_indices[ordinal] != ordinal)
        return common::make_unexpected(invalid("physical input projection is not identity"));
    }
    const common::Status input_schema_status =
        query::validate_distributed_vector_result_schema_value(input_schema);
    const common::Status output_schema_status =
        query::validate_distributed_vector_result_schema_value(result_schema);
    const common::Status plan_status = query::validate_distributed_vector_plan_intent(
        input.plan, static_cast<std::uint32_t>(input_schema.columns.size()),
        static_cast<std::uint32_t>(input_schema.columns.size()));
    if (!input_schema_status.is_ok())
      return common::make_unexpected(input_schema_status);
    if (!output_schema_status.is_ok())
      return common::make_unexpected(output_schema_status);
    if (!plan_status.is_ok())
      return common::make_unexpected(plan_status);
    if (coordinator_pipeline.input_columns().size() != input_schema.columns.size() ||
        coordinator_pipeline.output_columns().size() != result_schema.columns.size()) {
      return common::make_unexpected(invalid("physical pipeline schema width differs"));
    }
    for (std::size_t ordinal = 0U; ordinal < input_schema.columns.size(); ++ordinal) {
      if (coordinator_pipeline.input_columns()[ordinal] !=
          query::PhysicalColumnShape{.type = input_schema.columns[ordinal].type,
                                     .nullable = input_schema.columns[ordinal].nullable}) {
        return common::make_unexpected(invalid("physical pipeline input shape differs"));
      }
    }
    for (std::size_t ordinal = 0U; ordinal < result_schema.columns.size(); ++ordinal) {
      if (coordinator_pipeline.output_columns()[ordinal] !=
          query::PhysicalColumnShape{.type = result_schema.columns[ordinal].type,
                                     .nullable = result_schema.columns[ordinal].nullable}) {
        return common::make_unexpected(invalid("physical pipeline output shape differs"));
      }
    }
    if (input.result.messages.empty())
      return common::make_unexpected(invalid("physical input has no tablet streams"));
    if (input.result.messages.size() > limits.maximum_input_messages)
      return common::make_unexpected(exhausted("physical input message limit is exhausted"));

    common::Uuid query_id;
    std::optional<schema::TabletId> current_tablet;
    std::vector<schema::TabletId> tablets;
    tablets.reserve(input.result.messages.size());
    std::uint64_t expected_sequence = 1U;
    bool current_terminal{};
    std::uint64_t input_rows{};
    std::size_t input_bytes{};
    constexpr std::size_t kExchangeOverhead =
        distributed_vector_result_exchange_v2_format::kHeaderLength +
        distributed_vector_result_exchange_v2_format::kTrailerLength;
    for (const DistributedVectorResultExchangeMessage& message : input.result.messages) {
      if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
        return common::make_unexpected(invalid("physical input identity is invalid"));
      if (query_id.is_nil())
        query_id = message.query_id;
      else if (message.query_id != query_id)
        return common::make_unexpected(invalid("physical input mixes query identities"));
      if (!current_tablet.has_value() || message.tablet_id != *current_tablet) {
        if (current_tablet.has_value() && !current_terminal)
          return common::make_unexpected(invalid("physical tablet stream is not terminal"));
        if (std::ranges::find(tablets, message.tablet_id) != tablets.end())
          return common::make_unexpected(invalid("physical tablet stream is duplicated"));
        tablets.push_back(message.tablet_id);
        current_tablet = message.tablet_id;
        expected_sequence = 1U;
      } else if (current_terminal) {
        return common::make_unexpected(invalid("physical tablet continues after terminal"));
      }
      if (message.sequence != expected_sequence)
        return common::make_unexpected(invalid("physical tablet sequence is not contiguous"));
      if (expected_sequence == std::numeric_limits<std::uint64_t>::max()) {
        if (!message.terminal)
          return common::make_unexpected(exhausted("physical tablet sequence is exhausted"));
      } else {
        ++expected_sequence;
      }
      current_terminal = message.terminal;
      if (message.encoded_result_batch.empty() && (!message.terminal || message.sequence != 1U)) {
        return common::make_unexpected(invalid("physical empty tablet stream is noncanonical"));
      }
      const auto frame =
          common::checked_add(kExchangeOverhead, message.encoded_result_batch.size());
      const auto next_bytes =
          frame.has_value() ? common::checked_add(input_bytes, *frame) : std::nullopt;
      if (!next_bytes.has_value() || *next_bytes > limits.maximum_input_encoded_bytes)
        return common::make_unexpected(exhausted("physical input byte limit is exhausted"));
      input_bytes = *next_bytes;
      if (message.encoded_result_batch.empty())
        continue;
      auto batch =
          network::decode_query_result_batch(message.encoded_result_batch, limits.input_batch);
      if (!batch.has_value())
        return common::make_unexpected(batch.error());
      if (!descriptors_match(*batch, input_schema))
        return common::make_unexpected(corruption("physical input batch schema differs"));
      if (batch->row_count() > limits.maximum_input_rows - input_rows)
        return common::make_unexpected(exhausted("physical input row limit is exhausted"));
      input_rows += batch->row_count();
      auto working = materialized_batch_bytes(*batch, input_schema);
      if (!working.has_value())
        return common::make_unexpected(working.error());
      if (*working > limits.maximum_batch_working_bytes)
        return common::make_unexpected(exhausted("physical batch working limit is exhausted"));
    }
    if (!current_terminal)
      return common::make_unexpected(invalid("physical final tablet stream is not terminal"));

    auto resources = query::QueryResourceContext::create(limits.maximum_query_memory_bytes);
    if (!resources.has_value())
      return common::make_unexpected(resources.error());
    std::unique_ptr<query::PhysicalOperator> source{
        new NativeBatchSource{std::move(input.result), limits}};
    auto pipeline = coordinator_pipeline.instantiate(std::move(source));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());

    std::vector<network::QueryResultColumn> columns;
    columns.reserve(result_schema.columns.size());
    for (const auto& column : result_schema.columns) {
      columns.push_back({.name = column.name, .type = column.type, .nullable = column.nullable});
    }
    DistributedVectorRowsFinalizedResultV2 result{.result_schema = std::move(result_schema)};
    result.encoded_batches.reserve(std::min<std::size_t>(limits.maximum_output_batches, 16U));
    for (;;) {
      auto step = (*pipeline)->next(*resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == query::PhysicalOperatorStepKind::kEnd)
        break;
      if (step->chunk() == nullptr)
        return common::make_unexpected(internal("physical pipeline returned no chunk"));
      const std::size_t rows = step->chunk()->chunk().selected_row_count();
      if (rows > limits.maximum_output_rows - result.row_count)
        return common::make_unexpected(exhausted("physical output row limit is exhausted"));
      if (result.encoded_batches.size() >= limits.maximum_output_batches)
        return common::make_unexpected(exhausted("physical output batch limit is exhausted"));
      auto encoded = encode_chunk(step->chunk()->chunk(), columns, limits.output_batch);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > limits.maximum_output_encoded_bytes - result.encoded_bytes)
        return common::make_unexpected(exhausted("physical output byte limit is exhausted"));
      result.row_count += rows;
      result.encoded_bytes += encoded->size();
      result.encoded_batches.push_back(std::move(*encoded));
    }
    if (result.encoded_batches.empty()) {
      auto encoded = network::encode_query_result_batch(0U, columns, {}, limits.output_batch);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > limits.maximum_output_encoded_bytes)
        return common::make_unexpected(exhausted("physical empty output exceeds byte limit"));
      result.encoded_bytes = encoded->size();
      result.encoded_batches.push_back(std::move(*encoded));
    }
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("physical row finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("physical row finalization exceeds container limits"));
  }
}

} // namespace chronos::cluster
