#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/query/physical_plan.hpp"

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

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status internal(const char* message) {
  return {common::StatusCode::kInternal, message};
}

[[nodiscard]] bool
valid_limits(const DistributedVectorGroupedAggregateFinalizationLimitsV2& limits) noexcept {
  return query::sort_state_reservation_bytes(limits.sort).has_value() &&
         limits.maximum_output_rows <= kMaximumDistributedVectorRowFinalizationRowsV2 &&
         limits.maximum_output_batches > 0U &&
         limits.maximum_output_batches <= query::kMaximumDistributedCoordinatorMessages &&
         limits.maximum_output_encoded_bytes > 0U &&
         limits.maximum_output_encoded_bytes <= kMaximumDistributedVectorRowFinalizationBytesV2 &&
         limits.sort.maximum_rows > 0U && limits.sort.maximum_keys > 0U &&
         limits.sort.maximum_state_bytes > 0U &&
         limits.output_batch.protocol.maximum_payload_size > 0U &&
         limits.output_batch.protocol.maximum_payload_size <= network::kDefaultMaximumPayloadSize &&
         limits.output_batch.maximum_rows > 0U && limits.output_batch.maximum_columns > 0U &&
         limits.output_batch.maximum_columns <=
             query::distributed_vector_result_schema_format::kMaximumColumns &&
         limits.output_batch.maximum_column_name_bytes > 0U &&
         limits.output_batch.maximum_column_name_bytes <= 65'536U;
}

class GroupedExecutionSource final : public query::PhysicalOperator {
public:
  explicit GroupedExecutionSource(
      DistributedVectorGroupedAggregateQueryExecutionV2& execution) noexcept
      : execution_(std::addressof(execution)) {}

  [[nodiscard]] common::Result<query::PhysicalOperatorStep>
  next(const query::QueryResourceContext&) override {
    return execution_->next();
  }

private:
  DistributedVectorGroupedAggregateQueryExecutionV2* execution_;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_chunk(const query::VectorChunk& chunk,
             const std::span<const network::QueryResultColumn> columns,
             const network::QueryResultLimits& limits) {
  if (chunk.column_count() != columns.size() ||
      chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(internal("grouped final result chunk shape differs"));
  }
  try {
    const auto cell_count = common::checked_multiply(chunk.selected_row_count(), columns.size());
    if (!cell_count.has_value())
      return common::make_unexpected(exhausted("grouped final result cell count overflows"));
    std::vector<network::QueryResultCell> cells;
    cells.reserve(*cell_count);
    std::vector<std::byte> booleans(*cell_count);
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
    return common::make_unexpected(exhausted("grouped final result allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped final result exceeds container limits"));
  }
}

} // namespace

common::Status validate_distributed_vector_grouped_aggregate_finalization_limits_v2(
    const DistributedVectorGroupedAggregateFinalizationLimitsV2& limits) noexcept {
  return valid_limits(limits) ? common::Status::ok()
                              : invalid("grouped finalization limits are invalid");
}

common::Result<DistributedVectorRowsFinalizedResultV2>
finalize_distributed_vector_grouped_aggregate_v2(
    DistributedVectorGroupedAggregateQueryExecutionV2& input,
    const DistributedVectorGroupedAggregateFinalizationLimitsV2 limits) {
  try {
    const common::Status limits_status =
        validate_distributed_vector_grouped_aggregate_finalization_limits_v2(limits);
    if (!limits_status.is_ok())
      return common::make_unexpected(limits_status);
    const auto dispatches = input.snapshot().dispatches();
    if (dispatches.empty())
      return common::make_unexpected(invalid("grouped finalization snapshot is empty"));
    const query::DistributedVectorPlanIntent& plan = dispatches.front().plan;
    const auto keys = input.key_definitions();
    const auto aggregates = input.aggregate_definitions();
    query::DistributedVectorResultSchema result_schema = input.snapshot().result_schema();
    const std::optional<query::QueryResourceContext> output_resources = input.output_resources();
    if (plan.mode != query::DistributedVectorPlanMode::kGroupedAggregate || keys.empty() ||
        plan.group_key_input_indices.size() != keys.size() ||
        plan.aggregates.size() != aggregates.size() ||
        result_schema.columns.size() != keys.size() + aggregates.size() ||
        !output_resources.has_value()) {
      return common::make_unexpected(invalid("grouped finalization authority widths differ"));
    }
    const common::Status plan_status = query::validate_distributed_vector_plan_intent(
        plan, static_cast<std::uint32_t>(dispatches.front().destination_column_ordinals.size()),
        static_cast<std::uint32_t>(result_schema.columns.size()));
    const common::Status schema_status =
        query::validate_distributed_vector_result_schema_value(result_schema);
    if (!plan_status.is_ok())
      return common::make_unexpected(plan_status);
    if (!schema_status.is_ok())
      return common::make_unexpected(schema_status);

    std::vector<query::PhysicalColumnShape> shapes;
    shapes.reserve(result_schema.columns.size());
    for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
      const query::VectorGroupKeyDefinition& key = keys[ordinal];
      const auto& column = result_schema.columns[ordinal];
      if (key.column_ordinal != plan.group_key_input_indices[ordinal] || key.type != column.type ||
          key.nullable != column.nullable) {
        return common::make_unexpected(invalid("grouped finalization key authority differs"));
      }
      shapes.emplace_back(column.type, column.nullable);
    }
    for (std::size_t ordinal = 0U; ordinal < aggregates.size(); ++ordinal) {
      const query::VectorAggregateDefinition& definition = aggregates[ordinal];
      const query::DistributedVectorAggregateIntent& intent = plan.aggregates[ordinal];
      std::optional<std::uint32_t> input_index;
      if (definition.input.has_value())
        input_index = static_cast<std::uint32_t>(definition.input->column_ordinal);
      auto shape = query::vector_aggregate_output_shape(definition);
      if (!shape.has_value())
        return common::make_unexpected(shape.error());
      const auto& column = result_schema.columns[keys.size() + ordinal];
      if (intent.operation != definition.operation || intent.input_index != input_index ||
          shape->type != column.type || shape->nullable != column.nullable) {
        return common::make_unexpected(invalid("grouped finalization aggregate authority differs"));
      }
      shapes.emplace_back(column.type, column.nullable);
    }

    std::vector<query::PhysicalPipelineStage> stages;
    if (!plan.order_keys.empty()) {
      std::vector<query::VectorSortKey> sort_keys;
      sort_keys.reserve(plan.order_keys.size());
      for (const query::DistributedVectorOrderKey& key : plan.order_keys) {
        sort_keys.push_back({key.output_index, key.direction, key.null_placement});
      }
      stages.emplace_back(query::SortStage{std::move(sort_keys), limits.sort});
    }
    if (plan.limit.has_value())
      stages.emplace_back(query::LimitStage{*plan.limit});
    auto pipeline_plan = query::PhysicalPipelinePlan::create(shapes, std::move(stages));
    if (!pipeline_plan.has_value())
      return common::make_unexpected(pipeline_plan.error());
    std::unique_ptr<query::PhysicalOperator> source{new GroupedExecutionSource{input}};
    auto pipeline = pipeline_plan->instantiate(std::move(source));
    if (!pipeline.has_value())
      return common::make_unexpected(pipeline.error());
    std::vector<network::QueryResultColumn> columns;
    columns.reserve(result_schema.columns.size());
    for (const auto& column : result_schema.columns) {
      if (column.name.size() > limits.output_batch.maximum_column_name_bytes)
        return common::make_unexpected(exhausted("grouped final result name limit is exhausted"));
      columns.push_back({column.name, column.type, column.nullable});
    }
    DistributedVectorRowsFinalizedResultV2 result{.result_schema = std::move(result_schema)};
    result.encoded_batches.reserve(std::min<std::size_t>(limits.maximum_output_batches, 16U));
    for (;;) {
      auto step = (*pipeline)->next(*output_resources);
      if (!step.has_value())
        return common::make_unexpected(step.error());
      if (step->kind() == query::PhysicalOperatorStepKind::kEnd)
        break;
      if (step->chunk() == nullptr)
        return common::make_unexpected(internal("grouped final pipeline returned no chunk"));
      const std::size_t rows = step->chunk()->chunk().selected_row_count();
      if (rows > limits.maximum_output_rows - result.row_count)
        return common::make_unexpected(exhausted("grouped final output row limit is exhausted"));
      if (result.encoded_batches.size() >= limits.maximum_output_batches)
        return common::make_unexpected(exhausted("grouped final output batch limit is exhausted"));
      auto encoded = encode_chunk(step->chunk()->chunk(), columns, limits.output_batch);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > limits.maximum_output_encoded_bytes - result.encoded_bytes)
        return common::make_unexpected(exhausted("grouped final output byte limit is exhausted"));
      result.row_count += rows;
      result.encoded_bytes += encoded->size();
      result.encoded_batches.push_back(std::move(*encoded));
    }
    if (result.encoded_batches.empty()) {
      auto encoded = network::encode_query_result_batch(0U, columns, {}, limits.output_batch);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      if (encoded->size() > limits.maximum_output_encoded_bytes)
        return common::make_unexpected(exhausted("grouped empty output byte limit is exhausted"));
      result.encoded_bytes = encoded->size();
      result.encoded_batches.push_back(std::move(*encoded));
    }
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped finalization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped finalization exceeds container limits"));
  }
}

} // namespace chronos::cluster
