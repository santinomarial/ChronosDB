#include "chronos/query/aggregate.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/value.hpp"
#include "query/decimal_internal.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Status out_of_range(const std::string_view message) {
  return common::Status{common::StatusCode::kOutOfRange, std::string{message}};
}

[[nodiscard]] bool numeric(const schema::LogicalTypeKind kind) noexcept {
  return (kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kFloat64) ||
         kind == schema::LogicalTypeKind::kDecimal;
}

[[nodiscard]] bool variable(const schema::LogicalTypeKind kind) noexcept {
  return kind == schema::LogicalTypeKind::kString || kind == schema::LogicalTypeKind::kSymbol ||
         kind == schema::LogicalTypeKind::kBinary;
}

[[nodiscard]] bool signed_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool unsigned_integer(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] common::Result<double> numeric_double(const ScalarValue& value,
                                                    const schema::LogicalType& input_type) {
  if (const auto* signed_value = std::get_if<std::int64_t>(&value.storage());
      signed_value != nullptr) {
    return static_cast<double>(*signed_value);
  }
  if (const auto* unsigned_value = std::get_if<std::uint64_t>(&value.storage());
      unsigned_value != nullptr) {
    return static_cast<double>(*unsigned_value);
  }
  if (const auto* float_value = std::get_if<float>(&value.storage()); float_value != nullptr)
    return static_cast<double>(*float_value);
  if (const auto* double_value = std::get_if<double>(&value.storage()); double_value != nullptr)
    return *double_value;
  const auto* decimal_value = std::get_if<Decimal128Value>(&value.storage());
  if (decimal_value == nullptr || !input_type.is_decimal())
    return common::make_unexpected(invalid("aggregate numeric input has invalid storage"));
  return detail::decimal_to_double(*decimal_value, input_type);
}

class SingleChunkSource final : public PhysicalOperator {
public:
  explicit SingleChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk chunk = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(chunk));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

struct AggregateState {
  explicit AggregateState(const VectorAggregateDefinition& configured) : definition(configured) {}

  VectorAggregateDefinition definition;
  std::int64_t count{};
  std::size_t moment_count{};
  detail::ExactNumericAccumulator exact_sum;
  float float_sum{};
  double double_sum{};
  double mean{};
  double squared_distance{};
  std::optional<ScalarValue> extremum;
  bool has_value{};
};

[[nodiscard]] common::Result<void> increment_count(std::int64_t& count) {
  if (count == std::numeric_limits<std::int64_t>::max())
    return common::make_unexpected(out_of_range("aggregate COUNT exceeds INT64"));
  ++count;
  return {};
}

[[nodiscard]] common::Result<void> add_exact(AggregateState& state, const ScalarValue& value) {
  if (const auto* signed_value = std::get_if<std::int64_t>(&value.storage());
      signed_value != nullptr) {
    return state.exact_sum.add_signed(*signed_value);
  }
  if (const auto* unsigned_value = std::get_if<std::uint64_t>(&value.storage());
      unsigned_value != nullptr) {
    return state.exact_sum.add_unsigned(*unsigned_value);
  }
  const auto* decimal_value = std::get_if<Decimal128Value>(&value.storage());
  if (decimal_value == nullptr)
    return common::make_unexpected(invalid("exact aggregate input has invalid storage"));
  return state.exact_sum.add_decimal(*decimal_value);
}

[[nodiscard]] common::Result<void> accumulate_value(AggregateState& state,
                                                    const ScalarValue& value) {
  const VectorAggregateOperation operation = state.definition.operation;
  if (!state.definition.input.has_value())
    return common::make_unexpected(invalid("aggregate value operation has no input definition"));
  const schema::LogicalType& input_type = state.definition.input.value().type;
  if (operation == VectorAggregateOperation::kSum) {
    state.has_value = true;
    if (input_type.kind() == schema::LogicalTypeKind::kFloat32) {
      const auto* number = std::get_if<float>(&value.storage());
      if (number == nullptr)
        return common::make_unexpected(invalid("FLOAT32 aggregate input has invalid storage"));
      state.float_sum += *number;
      return {};
    }
    if (input_type.kind() == schema::LogicalTypeKind::kFloat64) {
      const auto* number = std::get_if<double>(&value.storage());
      if (number == nullptr)
        return common::make_unexpected(invalid("FLOAT64 aggregate input has invalid storage"));
      state.double_sum += *number;
      return {};
    }
    return add_exact(state, value);
  }
  if (operation == VectorAggregateOperation::kAverage) {
    const common::Result<double> number = numeric_double(value, input_type);
    if (!number.has_value())
      return common::make_unexpected(number.error());
    if (state.moment_count == std::numeric_limits<std::size_t>::max())
      return common::make_unexpected(out_of_range("aggregate AVG row count overflowed"));
    state.double_sum += *number;
    ++state.moment_count;
    return {};
  }
  if (operation == VectorAggregateOperation::kVariancePopulation ||
      operation == VectorAggregateOperation::kVarianceSample) {
    const common::Result<double> number = numeric_double(value, input_type);
    if (!number.has_value())
      return common::make_unexpected(number.error());
    if (state.moment_count == std::numeric_limits<std::size_t>::max())
      return common::make_unexpected(out_of_range("aggregate variance row count overflowed"));
    ++state.moment_count;
    const double delta = *number - state.mean;
    state.mean += delta / static_cast<double>(state.moment_count);
    state.squared_distance += delta * (*number - state.mean);
    return {};
  }
  if (operation == VectorAggregateOperation::kMinimum ||
      operation == VectorAggregateOperation::kMaximum) {
    if (!state.extremum.has_value()) {
      state.extremum = value;
      return {};
    }
    const common::Result<int> order =
        compare_scalar_values(value, *state.extremum, ScalarNullPlacement::kLast);
    if (!order.has_value())
      return common::make_unexpected(order.error());
    if ((operation == VectorAggregateOperation::kMinimum && *order < 0) ||
        (operation == VectorAggregateOperation::kMaximum && *order > 0)) {
      state.extremum = value;
    }
    return {};
  }
  return common::make_unexpected(invalid("aggregate operation cannot consume a value"));
}

[[nodiscard]] common::Result<ScalarValue> finish_exact_sum(const AggregateState& state) {
  if (!state.definition.input.has_value())
    return common::make_unexpected(invalid("exact SUM has no input definition"));
  const schema::LogicalType& result_type = state.definition.input.value().type;
  if (result_type.is_decimal()) {
    const common::Result<Decimal128Value> result = state.exact_sum.decimal_result(result_type);
    if (!result.has_value())
      return common::make_unexpected(result.error());
    return ScalarValue::decimal(result_type, *result);
  }
  if (signed_integer(result_type.kind())) {
    const common::Result<std::int64_t> result = state.exact_sum.signed_result();
    if (!result.has_value())
      return common::make_unexpected(result.error());
    return ScalarValue::signed_value(result_type, *result);
  }
  if (unsigned_integer(result_type.kind())) {
    const common::Result<std::uint64_t> result = state.exact_sum.unsigned_result();
    if (!result.has_value())
      return common::make_unexpected(result.error());
    return ScalarValue::unsigned_value(result_type, *result);
  }
  return common::make_unexpected(invalid("exact SUM result type is invalid"));
}

[[nodiscard]] common::Result<ScalarValue> finish(const AggregateState& state) {
  const VectorAggregateOperation operation = state.definition.operation;
  const common::Result<VectorAggregateOutputShape> shape =
      vector_aggregate_output_shape(state.definition);
  if (!shape.has_value())
    return common::make_unexpected(shape.error());
  if (operation == VectorAggregateOperation::kCountStar ||
      operation == VectorAggregateOperation::kCount) {
    return ScalarValue::signed_value(shape->type, state.count);
  }
  if (operation == VectorAggregateOperation::kMinimum ||
      operation == VectorAggregateOperation::kMaximum) {
    return state.extremum.has_value() ? common::Result<ScalarValue>{*state.extremum}
                                      : common::Result<ScalarValue>{ScalarValue::null(shape->type)};
  }
  if (operation == VectorAggregateOperation::kAverage) {
    if (state.moment_count == 0U)
      return ScalarValue::null(shape->type);
    return ScalarValue::float64(state.double_sum / static_cast<double>(state.moment_count));
  }
  if (operation == VectorAggregateOperation::kVariancePopulation ||
      operation == VectorAggregateOperation::kVarianceSample) {
    if (state.moment_count == 0U ||
        (operation == VectorAggregateOperation::kVarianceSample && state.moment_count < 2U)) {
      return ScalarValue::null(shape->type);
    }
    const std::size_t divisor = operation == VectorAggregateOperation::kVarianceSample
                                    ? state.moment_count - 1U
                                    : state.moment_count;
    return ScalarValue::float64(state.squared_distance / static_cast<double>(divisor));
  }
  if (!state.has_value)
    return ScalarValue::null(shape->type);
  if (shape->type.kind() == schema::LogicalTypeKind::kFloat32)
    return ScalarValue::float32(state.float_sum);
  if (shape->type.kind() == schema::LogicalTypeKind::kFloat64)
    return ScalarValue::float64(state.double_sum);
  return finish_exact_sum(state);
}

} // namespace

class UngroupedAggregateOperator::Impl {
public:
  struct Result {
    ScalarValue value;
    bool nullable;
  };

  explicit Impl(std::vector<AggregateState> states) noexcept : states_(std::move(states)) {}

  [[nodiscard]] common::Result<void> consume(const VectorChunk& chunk,
                                             const QueryResourceContext& resources) {
    for (const AggregateState& state : states_) {
      if (!state.definition.input.has_value())
        continue;
      const VectorAggregateInput& expected = *state.definition.input;
      const columnar::PhysicalColumnView* column = chunk.column(expected.column_ordinal);
      if (column == nullptr || column->type() != expected.type ||
          column->nullable() != expected.nullable) {
        return common::make_unexpected(invalid("aggregate input column shape mismatch"));
      }
    }

    for (std::size_t selected_row = 0U; selected_row < chunk.selected_row_count(); ++selected_row) {
      if ((selected_row % 256U) == 0U) {
        const common::Result<void> active = resources.check_cancelled();
        if (!active.has_value())
          return common::make_unexpected(active.error());
      }
      for (AggregateState& state : states_) {
        if (state.definition.operation == VectorAggregateOperation::kCountStar) {
          const common::Result<void> counted = increment_count(state.count);
          if (!counted.has_value())
            return counted;
          continue;
        }
        const VectorAggregateInput& input = *state.definition.input;
        const common::Result<columnar::ColumnCellView> cell =
            chunk.cell({.column_ordinal = input.column_ordinal, .selected_row = selected_row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (state.definition.operation == VectorAggregateOperation::kCount) {
          if (!cell->is_null()) {
            const common::Result<void> counted = increment_count(state.count);
            if (!counted.has_value())
              return counted;
          }
          continue;
        }
        if (cell->is_null())
          continue;
        const common::Result<ScalarValue> value = ScalarValue::from_column_cell(input.type, *cell);
        if (!value.has_value())
          return common::make_unexpected(value.error());
        const common::Result<void> accumulated = accumulate_value(state, *value);
        if (!accumulated.has_value())
          return accumulated;
      }
    }
    return {};
  }

  [[nodiscard]] common::Result<std::vector<Result>> finish_all() const {
    try {
      std::vector<Result> results;
      results.reserve(states_.size());
      for (const AggregateState& state : states_) {
        common::Result<ScalarValue> value = finish(state);
        if (!value.has_value())
          return common::make_unexpected(value.error());
        common::Result<VectorAggregateOutputShape> shape =
            vector_aggregate_output_shape(state.definition);
        if (!shape.has_value())
          return common::make_unexpected(shape.error());
        results.push_back({.value = std::move(*value), .nullable = shape->nullable});
      }
      return results;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("aggregate result allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("aggregate result exceeds container limits"));
    }
  }

private:
  std::vector<AggregateState> states_;
};

common::Result<VectorAggregateOutputShape>
vector_aggregate_output_shape(const VectorAggregateDefinition& definition) {
  if (definition.operation == VectorAggregateOperation::kCountStar) {
    if (definition.input.has_value())
      return common::make_unexpected(invalid("COUNT(*) cannot have an input column"));
    return VectorAggregateOutputShape{.type = type(schema::LogicalTypeKind::kInt64),
                                      .nullable = false};
  }
  if (!definition.input.has_value())
    return common::make_unexpected(invalid("aggregate operation requires an input column"));
  const schema::LogicalType& input_type = definition.input->type;
  if (definition.operation == VectorAggregateOperation::kCount) {
    return VectorAggregateOutputShape{.type = type(schema::LogicalTypeKind::kInt64),
                                      .nullable = false};
  }
  if (definition.operation == VectorAggregateOperation::kSum ||
      definition.operation == VectorAggregateOperation::kAverage ||
      definition.operation == VectorAggregateOperation::kVariancePopulation ||
      definition.operation == VectorAggregateOperation::kVarianceSample) {
    if (!numeric(input_type.kind()))
      return common::make_unexpected(invalid("numeric aggregate requires a numeric input"));
    const schema::LogicalType result = definition.operation == VectorAggregateOperation::kSum
                                           ? input_type
                                           : type(schema::LogicalTypeKind::kFloat64);
    return VectorAggregateOutputShape{.type = result, .nullable = true};
  }
  if (definition.operation == VectorAggregateOperation::kMinimum ||
      definition.operation == VectorAggregateOperation::kMaximum) {
    if (variable(input_type.kind())) {
      return common::make_unexpected(
          invalid("variable-width MIN/MAX requires the grouped-state accounting decision"));
    }
    return VectorAggregateOutputShape{.type = input_type, .nullable = true};
  }
  return common::make_unexpected(invalid("aggregate operation is invalid"));
}

UngroupedAggregateOperator::UngroupedAggregateOperator(
    std::unique_ptr<PhysicalOperator> input, std::unique_ptr<Impl> impl,
    const VectorChunkLimits output_limits) noexcept
    : input_(std::move(input)), impl_(std::move(impl)), output_limits_(output_limits) {}

UngroupedAggregateOperator::~UngroupedAggregateOperator() = default;

common::Result<std::unique_ptr<PhysicalOperator>>
UngroupedAggregateOperator::create(std::unique_ptr<PhysicalOperator> input,
                                   const std::vector<VectorAggregateDefinition>& definitions,
                                   const UngroupedAggregateLimits limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("aggregate input operator is required"));
  if (limits.maximum_aggregates == 0U ||
      limits.maximum_aggregates > kMaximumUngroupedAggregateWidth ||
      limits.maximum_retained_configuration_bytes == 0U ||
      limits.output_limits.maximum_rows == 0U || limits.output_limits.maximum_columns == 0U ||
      limits.output_limits.maximum_buffer_bytes == 0U ||
      limits.output_limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("aggregate limits are invalid"));
  }
  if (definitions.empty())
    return common::make_unexpected(invalid("aggregate definition list cannot be empty"));
  if (definitions.size() > limits.maximum_aggregates ||
      definitions.size() > limits.output_limits.maximum_columns) {
    return common::make_unexpected(exhausted("aggregate definition width exceeds its limit"));
  }
  for (const VectorAggregateDefinition& definition : definitions) {
    common::Result<VectorAggregateOutputShape> shape = vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return common::make_unexpected(shape.error());
  }
  const std::optional<std::size_t> state_bytes =
      common::checked_multiply(definitions.size(), sizeof(AggregateState));
  if (!state_bytes.has_value() || *state_bytes > limits.maximum_retained_configuration_bytes) {
    return common::make_unexpected(
        exhausted("aggregate retained configuration exceeds its byte limit"));
  }

  try {
    std::vector<AggregateState> states;
    states.reserve(definitions.size());
    const std::optional<std::size_t> retained =
        common::checked_multiply(states.capacity(), sizeof(AggregateState));
    if (!retained.has_value() || *retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("aggregate retained configuration exceeds its byte limit"));
    }
    for (const VectorAggregateDefinition& definition : definitions)
      states.emplace_back(definition);
    auto impl = std::make_unique<Impl>(std::move(states));
    return std::unique_ptr<PhysicalOperator>{
        new UngroupedAggregateOperator{std::move(input), std::move(impl), limits.output_limits}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate operator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate configuration exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep>
UngroupedAggregateOperator::next(const QueryResourceContext& resources) {
  if (emitted_)
    return PhysicalOperatorStep::end();
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());

  while (input_ != nullptr) {
    common::Result<PhysicalOperatorStep> step = input_->next(resources);
    if (!step.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(step.error());
    }
    if (step->kind() == PhysicalOperatorStepKind::kEnd) {
      input_.reset();
      break;
    }
    common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
    if (!chunk.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(chunk.error());
    }
    if (!chunk->belongs_to(resources)) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(invalid("aggregate input chunk belongs to another query"));
    }
    const common::Result<void> consumed = impl_->consume(chunk->chunk(), resources);
    if (!consumed.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(consumed.error());
    }
  }

  common::Result<std::vector<Impl::Result>> values = impl_->finish_all();
  if (!values.has_value()) {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(values.error());
  }
  const auto fail_output =
      [&resources](common::Status status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    return common::make_unexpected(std::move(status));
  };
  try {
    std::vector<ColumnOutputPosition> positions;
    positions.reserve(values->size());
    for (Impl::Result& result : *values) {
      positions.emplace_back(ConstantColumnOutputPosition{.value = std::move(result.value),
                                                          .force_nullable = result.nullable});
    }

    common::Result<QueryMemoryReservation> reservation = resources.reserve(sizeof(std::uint32_t));
    if (!reservation.has_value())
      return fail_output(reservation.error());
    common::Result<VectorSelection> selection = VectorSelection::all(1U);
    if (!selection.has_value())
      return fail_output(selection.error());
    common::Result<VectorChunk> cardinality =
        VectorChunk::create({}, std::move(*selection),
                            {.maximum_rows = 1U,
                             .maximum_columns = 1U,
                             .maximum_buffer_bytes = sizeof(std::uint32_t),
                             .maximum_retained_buffer_bytes = sizeof(std::uint32_t)});
    if (!cardinality.has_value())
      return fail_output(cardinality.error());
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*cardinality), std::move(*reservation), resources);
    if (!accounted.has_value())
      return fail_output(accounted.error());
    common::Result<std::unique_ptr<PhysicalOperator>> materializer =
        ColumnOutputOperator::create(std::make_unique<SingleChunkSource>(std::move(*accounted)),
                                     std::move(positions), output_limits_);
    if (!materializer.has_value())
      return fail_output(materializer.error());
    common::Result<PhysicalOperatorStep> result = (*materializer)->next(resources);
    if (!result.has_value()) {
      static_cast<void>(resources.request_cancel());
      return common::make_unexpected(result.error());
    }
    emitted_ = true;
    return result;
  } catch (const std::bad_alloc&) {
    return fail_output(exhausted("aggregate output allocation failed"));
  } catch (const std::length_error&) {
    return fail_output(exhausted("aggregate output exceeds container limits"));
  }
}

} // namespace chronos::query
