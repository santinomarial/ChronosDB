#include "chronos/live/committed_batch_evaluator.hpp"

#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/physical_plan.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::array<std::byte, 8U> kResultKeyMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'L'},
                                                    std::byte{'B'}, std::byte{'R'}, std::byte{'K'},
                                                    std::byte{'1'}, std::byte{0}};
constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Result<std::size_t> add(const std::size_t left, const std::size_t right,
                                              const char* message) {
  const std::optional<std::size_t> result = common::checked_add(left, right);
  return result.has_value() ? common::Result<std::size_t>{*result}
                            : common::make_unexpected(exhausted(message));
}

[[nodiscard]] common::Result<std::size_t> multiply(const std::size_t left, const std::size_t right,
                                                   const char* message) {
  const std::optional<std::size_t> result = common::checked_multiply(left, right);
  return result.has_value() ? common::Result<std::size_t>{*result}
                            : common::make_unexpected(exhausted(message));
}

[[nodiscard]] bool row_preserving_stage(const query::PhysicalPipelineStage& stage) noexcept {
  return std::holds_alternative<query::BooleanFilterStage>(stage) ||
         std::holds_alternative<query::TimestampRangeFilterStage>(stage) ||
         std::holds_alternative<query::ColumnSubsetStage>(stage) ||
         std::holds_alternative<query::SourceColumnOutputStage>(stage) ||
         std::holds_alternative<query::ColumnOutputStage>(stage);
}

[[nodiscard]] common::Result<std::size_t>
expected_chunks(const columnar::OwnedColumnarBatch& batch,
                const CommittedBatchEvaluatorLimits& limits) {
  const std::uint32_t rows_per_chunk =
      std::min(limits.scan.maximum_rows_per_chunk, limits.scan.chunk.maximum_rows);
  if (rows_per_chunk == 0U)
    return common::make_unexpected(invalid("committed batch scan row limits must be nonzero"));
  if (batch.row_count() == 0U)
    return 0U;
  return (static_cast<std::size_t>(batch.row_count()) + rows_per_chunk - 1U) / rows_per_chunk;
}

[[nodiscard]] common::Result<std::size_t> wire_cell_bytes(const columnar::ColumnCellView& cell) {
  if (cell.is_null())
    return sizeof(std::uint32_t);
  std::size_t value_bytes{};
  if (cell.kind() == columnar::ColumnCellView::Kind::kBoolean) {
    value_bytes = 1U;
  } else {
    auto bytes = cell.bytes();
    if (!bytes.has_value())
      return common::make_unexpected(bytes.error());
    value_bytes = bytes->size();
  }
  return add(sizeof(std::uint32_t), value_bytes, "committed result cell size overflowed");
}

struct ResultShape {
  std::uint32_t rows{};
  std::size_t cells{};
  std::size_t encoded_bytes{};
};

[[nodiscard]] common::Result<ResultShape>
result_shape(const PreparedSubscriptionPlan& plan,
             const std::vector<query::AccountedVectorChunk>& chunks,
             const CommittedBatchEvaluatorLimits& limits) {
  if (plan.columns().size() > limits.result.maximum_columns)
    return common::make_unexpected(exhausted("committed result exceeds the column limit"));
  std::size_t encoded = network::kQueryResultEnvelopeSize;
  for (const SnapshotSubscriptionColumn& column : plan.columns()) {
    if (column.name.empty() || column.name.size() > limits.result.maximum_column_name_bytes)
      return common::make_unexpected(invalid("committed result column name is invalid"));
    auto next = add(encoded, network::kQueryResultColumnEnvelopeSize + column.name.size(),
                    "committed result descriptor size overflowed");
    if (!next.has_value())
      return common::make_unexpected(next.error());
    encoded = *next;
  }

  std::size_t rows{};
  for (const query::AccountedVectorChunk& accounted : chunks) {
    const query::VectorChunk& chunk = accounted.chunk();
    if (chunk.column_count() != plan.columns().size())
      return common::make_unexpected(invalid("committed result output shape is invalid"));
    auto next_rows = add(rows, chunk.selected_row_count(), "committed result row count overflowed");
    if (!next_rows.has_value())
      return common::make_unexpected(next_rows.error());
    rows = *next_rows;
    if (rows > limits.result.maximum_rows || rows > std::numeric_limits<std::uint32_t>::max())
      return common::make_unexpected(exhausted("committed result exceeds the row limit"));
    for (std::size_t column = 0U; column < plan.columns().size(); ++column) {
      const columnar::PhysicalColumnView* physical = chunk.column(column);
      if (physical == nullptr || physical->type() != plan.columns()[column].type ||
          physical->nullable() != plan.columns()[column].nullable)
        return common::make_unexpected(invalid("committed result disagrees with bound output"));
    }
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      for (std::size_t column = 0U; column < plan.columns().size(); ++column) {
        auto cell = chunk.cell({column, row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        auto bytes = wire_cell_bytes(*cell);
        if (!bytes.has_value())
          return common::make_unexpected(bytes.error());
        auto next = add(encoded, *bytes, "committed result payload size overflowed");
        if (!next.has_value())
          return common::make_unexpected(next.error());
        encoded = *next;
      }
    }
  }
  const std::size_t change_overhead =
      network::kSubscriptionChangeEnvelopeSize + kCommittedBatchResultKeySize;
  if (change_overhead > limits.subscription.protocol.maximum_payload_size ||
      encoded > limits.result.protocol.maximum_payload_size ||
      encoded > limits.subscription.protocol.maximum_payload_size - change_overhead)
    return common::make_unexpected(exhausted("committed result exceeds the payload limit"));
  auto cells = multiply(rows, plan.columns().size(), "committed result cell count overflowed");
  if (!cells.has_value())
    return common::make_unexpected(cells.error());
  return ResultShape{static_cast<std::uint32_t>(rows), *cells, encoded};
}

[[nodiscard]] common::Result<std::vector<std::byte>>
result_key(const PreparedSubscriptionPlan& plan, const SourcePosition& position) {
  try {
    std::vector<std::byte> key(kCommittedBatchResultKeySize, std::byte{0});
    common::ByteWriter writer{key};
    common::Status status = writer.write_exact(kResultKeyMagic);
    if (status.is_ok())
      status = writer.write_exact(plan.fingerprint());
    if (status.is_ok())
      status = writer.write_exact(position.tablet_id.bytes());
    if (status.is_ok())
      status = writer.write_exact(position.wal_id.bytes);
    if (status.is_ok())
      status = writer.write_u64_le(position.record_sequence);
    if (!status.is_ok() || !writer.full())
      return common::make_unexpected(invalid("committed result key layout mismatch"));
    return key;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("committed result key allocation failed"));
  }
}

} // namespace

common::Status validate_committed_batch_plan(const PreparedSubscriptionPlan& plan) {
  for (const query::PhysicalPipelineStage& stage : plan.physical_plan().stages()) {
    if (!row_preserving_stage(stage))
      return unsupported("committed batch evaluation requires a stateless row-preserving plan");
  }
  if (plan.columns().empty() ||
      plan.columns().size() != plan.physical_plan().output_columns().size())
    return invalid("committed batch plan output metadata is invalid");
  return common::Status::ok();
}

common::Result<CommittedChange>
evaluate_committed_batch(const PreparedSubscriptionPlan& plan, const SourcePosition position,
                         std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                         const query::QueryResourceContext& resources,
                         const CommittedBatchEvaluatorLimits limits) {
  if (batch == nullptr || !position.is_valid() ||
      position.source_kind != SubscriptionSourceKind::kWal || position.record_sequence == 0U ||
      limits.maximum_output_chunks == 0U || limits.maximum_workspace_bytes == 0U)
    return common::make_unexpected(invalid("committed batch evaluation input is invalid"));
  if (const common::Status status =
          network::validate_subscription_message_limits(limits.subscription);
      !status.is_ok())
    return common::make_unexpected(status);
  if (limits.subscription.maximum_result_key_bytes < kCommittedBatchResultKeySize)
    return common::make_unexpected(
        invalid("committed batch result key exceeds the subscription limit"));
  if (batch->schema().table_id() != plan.schema_ptr()->table_id() ||
      batch->schema().schema_id() != plan.schema_ptr()->schema_id() ||
      batch->schema().version() != plan.schema_ptr()->version())
    return common::make_unexpected(invalid("committed batch schema does not match the plan"));
  if (const common::Status status = validate_committed_batch_plan(plan); !status.is_ok())
    return common::make_unexpected(status);
  auto chunk_count = expected_chunks(*batch, limits);
  if (!chunk_count.has_value())
    return common::make_unexpected(chunk_count.error());
  if (*chunk_count > limits.maximum_output_chunks)
    return common::make_unexpected(exhausted("committed result exceeds the chunk limit"));
  auto chunk_owner_bytes = multiply(*chunk_count, sizeof(query::AccountedVectorChunk),
                                    "committed result chunk ownership overflowed");
  if (!chunk_owner_bytes.has_value())
    return common::make_unexpected(chunk_owner_bytes.error());
  auto chunk_workspace = add(*chunk_owner_bytes, kConservativeAllocationOverheadBytes,
                             "committed result chunk ownership overflowed");
  if (!chunk_workspace.has_value())
    return common::make_unexpected(chunk_workspace.error());
  if (*chunk_workspace > limits.maximum_workspace_bytes)
    return common::make_unexpected(exhausted("committed result exceeds the workspace limit"));
  auto chunk_reservation = resources.reserve(*chunk_workspace);
  if (!chunk_reservation.has_value())
    return common::make_unexpected(chunk_reservation.error());

  try {
    auto source = query::ColumnarBatchScanOperator::create(std::move(batch), limits.scan);
    if (!source.has_value())
      return common::make_unexpected(source.error());
    auto pipeline = plan.physical_plan().instantiate(std::move(*source));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    std::vector<query::AccountedVectorChunk> chunks;
    chunks.reserve(*chunk_count);
    while (true) {
      auto step = (*pipeline)->next(resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == query::PhysicalOperatorStepKind::kEnd)
        break;
      if (chunks.size() == *chunk_count)
        return common::make_unexpected(
            invalid("committed result pipeline produced an unexpected extra chunk"));
      auto chunk = std::move(*step).take_chunk();
      if (!chunk.has_value())
        return common::make_unexpected(chunk.error());
      chunks.push_back(std::move(*chunk));
    }

    auto shape = result_shape(plan, chunks, limits);
    if (!shape.has_value())
      return common::make_unexpected(shape.error());
    auto cell_owner_bytes = multiply(shape->cells, sizeof(network::QueryResultCell),
                                     "committed result cell workspace overflowed");
    if (!cell_owner_bytes.has_value())
      return common::make_unexpected(cell_owner_bytes.error());
    auto descriptor_bytes = multiply(plan.columns().size(), sizeof(network::QueryResultColumn),
                                     "committed result descriptor workspace overflowed");
    if (!descriptor_bytes.has_value())
      return common::make_unexpected(descriptor_bytes.error());
    auto workspace =
        add(*cell_owner_bytes, *descriptor_bytes, "committed result workspace overflowed");
    if (workspace.has_value())
      workspace = add(*workspace, shape->encoded_bytes, "committed result workspace overflowed");
    if (workspace.has_value())
      workspace =
          add(*workspace, kCommittedBatchResultKeySize + 4U * kConservativeAllocationOverheadBytes,
              "committed result workspace overflowed");
    if (!workspace.has_value())
      return common::make_unexpected(workspace.error());
    auto total_workspace =
        add(*chunk_workspace, *workspace, "committed result total workspace overflowed");
    if (!total_workspace.has_value())
      return common::make_unexpected(total_workspace.error());
    if (*total_workspace > limits.maximum_workspace_bytes)
      return common::make_unexpected(exhausted("committed result exceeds the workspace limit"));
    auto workspace_reservation = resources.reserve(*workspace);
    if (!workspace_reservation.has_value())
      return common::make_unexpected(workspace_reservation.error());

    std::vector<network::QueryResultColumn> descriptors;
    descriptors.reserve(plan.columns().size());
    for (const SnapshotSubscriptionColumn& column : plan.columns())
      descriptors.push_back({column.name, column.type, column.nullable});
    std::vector<network::QueryResultCell> cells;
    cells.reserve(shape->cells);
    static constexpr std::array<std::byte, 1U> kFalse{std::byte{0}};
    static constexpr std::array<std::byte, 1U> kTrue{std::byte{1}};
    for (const query::AccountedVectorChunk& accounted : chunks) {
      const query::VectorChunk& chunk = accounted.chunk();
      for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
        for (std::size_t column = 0U; column < plan.columns().size(); ++column) {
          auto cell = chunk.cell({column, row});
          if (!cell.has_value())
            return common::make_unexpected(cell.error());
          if (cell->is_null()) {
            cells.push_back({.is_null = true, .value = {}});
          } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
            auto value = cell->boolean();
            if (!value.has_value())
              return common::make_unexpected(value.error());
            cells.push_back({.value = *value ? common::ByteView{kTrue} : common::ByteView{kFalse}});
          } else {
            auto value = cell->bytes();
            if (!value.has_value())
              return common::make_unexpected(value.error());
            cells.push_back({.value = *value});
          }
        }
      }
    }
    auto payload =
        network::encode_query_result_batch(shape->rows, descriptors, cells, limits.result);
    if (!payload.has_value())
      return common::make_unexpected(payload.error());
    if (payload->size() != shape->encoded_bytes)
      return common::make_unexpected(invalid("committed result payload planning mismatch"));
    auto key = result_key(plan, position);
    if (!key.has_value())
      return common::make_unexpected(key.error());
    return CommittedChange{position,
                           plan.schema_ptr()->schema_id(),
                           plan.schema_ptr()->version(),
                           LogicalChangeOperation::kUpsert,
                           std::move(*key),
                           std::move(*payload)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("committed batch evaluation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("committed batch evaluation exceeds container limits"));
  }
}

} // namespace chronos::live
