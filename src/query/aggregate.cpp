#include "chronos/query/aggregate.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/column_output.hpp"
#include "chronos/query/value.hpp"
#include "query/decimal_internal.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

inline constexpr std::size_t kConservativeAllocationOverheadBytes = 64U;

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
  QueryMemoryReservation extremum_reservation;
  bool has_value{};
};

[[nodiscard]] common::Result<int> compare_variable_extremum(const common::ByteView candidate,
                                                            const ScalarValue& extremum,
                                                            const schema::LogicalTypeKind kind) {
  common::ByteView stored;
  if (kind == schema::LogicalTypeKind::kBinary) {
    const auto* bytes = std::get_if<std::vector<std::byte>>(&extremum.storage());
    if (bytes == nullptr)
      return common::make_unexpected(invalid("variable aggregate binary storage is invalid"));
    stored = *bytes;
  } else {
    const auto* text = std::get_if<std::string>(&extremum.storage());
    if (text == nullptr)
      return common::make_unexpected(invalid("variable aggregate text storage is invalid"));
    const std::size_t prefix = std::min(candidate.size(), text->size());
    const int compared = prefix == 0U ? 0 : std::memcmp(candidate.data(), text->data(), prefix);
    if (compared != 0)
      return compared < 0 ? -1 : 1;
    return candidate.size() == text->size() ? 0 : (candidate.size() < text->size() ? -1 : 1);
  }
  const std::size_t prefix = std::min(candidate.size(), stored.size());
  const int compared = prefix == 0U ? 0 : std::memcmp(candidate.data(), stored.data(), prefix);
  if (compared != 0)
    return compared < 0 ? -1 : 1;
  return candidate.size() == stored.size() ? 0 : (candidate.size() < stored.size() ? -1 : 1);
}

[[nodiscard]] common::Result<void>
accumulate_variable_extremum(AggregateState& state, const columnar::ColumnCellView& cell,
                             const schema::LogicalType& input_type,
                             const QueryResourceContext& resources,
                             const std::size_t maximum_bytes) {
  const common::Result<common::ByteView> bytes = cell.bytes();
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  bool replace = !state.extremum.has_value();
  if (!replace) {
    const common::Result<int> order =
        compare_variable_extremum(*bytes, *state.extremum, input_type.kind());
    if (!order.has_value())
      return common::make_unexpected(order.error());
    replace = (state.definition.operation == VectorAggregateOperation::kMinimum && *order < 0) ||
              (state.definition.operation == VectorAggregateOperation::kMaximum && *order > 0);
  }
  if (!replace)
    return {};
  if (bytes->size() > maximum_bytes)
    return common::make_unexpected(exhausted("aggregate extremum exceeds its byte limit"));
  const std::optional<std::size_t> doubled =
      common::checked_multiply(bytes->size(), std::size_t{2U});
  const std::optional<std::size_t> charge =
      doubled.has_value() ? common::checked_add(*doubled, kConservativeAllocationOverheadBytes)
                          : std::nullopt;
  if (!charge.has_value())
    return common::make_unexpected(exhausted("aggregate extremum accounting overflowed"));
  common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
  if (!reservation.has_value())
    return common::make_unexpected(reservation.error());
  try {
    common::Result<ScalarValue> value = ScalarValue::from_column_cell(input_type, cell);
    if (!value.has_value())
      return common::make_unexpected(value.error());
    std::size_t retained_payload = 0U;
    if (const auto* text = std::get_if<std::string>(&value->storage()); text != nullptr) {
      retained_payload = text->capacity();
    } else if (const auto* binary = std::get_if<std::vector<std::byte>>(&value->storage());
               binary != nullptr) {
      retained_payload = binary->capacity();
    } else {
      return common::make_unexpected(invalid("variable aggregate storage is invalid"));
    }
    if (retained_payload > reservation->bytes()) {
      return common::make_unexpected(
          exhausted("aggregate extremum allocation exceeded its charge"));
    }
    state.extremum = std::move(*value);
    state.extremum_reservation = std::move(*reservation);
    return {};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate extremum allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate extremum exceeds container limits"));
  }
}

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

[[nodiscard]] common::Result<void> accumulate_cell(AggregateState& state,
                                                   const columnar::ColumnCellView& cell,
                                                   const QueryResourceContext& resources,
                                                   const std::size_t maximum_extremum_bytes) {
  if (state.definition.operation == VectorAggregateOperation::kCount) {
    if (!cell.is_null())
      return increment_count(state.count);
    return {};
  }
  if (cell.is_null())
    return {};
  if (!state.definition.input.has_value())
    return common::make_unexpected(invalid("aggregate operation has no input definition"));
  const schema::LogicalType& input_type = state.definition.input.value().type;
  if ((state.definition.operation == VectorAggregateOperation::kMinimum ||
       state.definition.operation == VectorAggregateOperation::kMaximum) &&
      variable(input_type.kind())) {
    return accumulate_variable_extremum(state, cell, input_type, resources, maximum_extremum_bytes);
  }
  const common::Result<ScalarValue> value = ScalarValue::from_column_cell(input_type, cell);
  if (!value.has_value())
    return common::make_unexpected(value.error());
  return accumulate_value(state, *value);
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

[[nodiscard]] common::Result<ScalarValue> finish(AggregateState& state) {
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
    return state.extremum.has_value() ? common::Result<ScalarValue>{std::move(*state.extremum)}
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

[[nodiscard]] common::Result<PhysicalOperatorStep>
materialize_single_row(const QueryResourceContext& resources,
                       std::vector<ColumnOutputPosition> positions,
                       const VectorChunkLimits output_limits) {
  try {
    common::Result<QueryMemoryReservation> reservation = resources.reserve(sizeof(std::uint32_t));
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    common::Result<VectorSelection> selection = VectorSelection::all(1U);
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    common::Result<VectorChunk> cardinality =
        VectorChunk::create({}, std::move(*selection),
                            {.maximum_rows = 1U,
                             .maximum_columns = 1U,
                             .maximum_buffer_bytes = sizeof(std::uint32_t),
                             .maximum_retained_buffer_bytes = sizeof(std::uint32_t)});
    if (!cardinality.has_value())
      return common::make_unexpected(cardinality.error());
    common::Result<AccountedVectorChunk> accounted =
        AccountedVectorChunk::create(std::move(*cardinality), std::move(*reservation), resources);
    if (!accounted.has_value())
      return common::make_unexpected(accounted.error());
    common::Result<std::unique_ptr<PhysicalOperator>> materializer =
        ColumnOutputOperator::create(std::make_unique<SingleChunkSource>(std::move(*accounted)),
                                     std::move(positions), output_limits);
    if (!materializer.has_value())
      return common::make_unexpected(materializer.error());
    return (*materializer)->next(resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("aggregate output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("aggregate output exceeds container limits"));
  }
}

} // namespace

class UngroupedAggregateOperator::Impl {
public:
  struct Result {
    ScalarValue value;
    bool nullable;
  };

  Impl(std::vector<AggregateState> states, const std::size_t maximum_extremum_bytes) noexcept
      : states_(std::move(states)), maximum_extremum_bytes_(maximum_extremum_bytes) {}

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
        const common::Result<void> accumulated =
            accumulate_cell(state, *cell, resources, maximum_extremum_bytes_);
        if (!accumulated.has_value())
          return accumulated;
      }
    }
    return {};
  }

  [[nodiscard]] common::Result<std::vector<Result>> finish_all() {
    try {
      std::vector<Result> results;
      results.reserve(states_.size());
      for (AggregateState& state : states_) {
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
  std::size_t maximum_extremum_bytes_;
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
      limits.maximum_variable_extremum_bytes == 0U ||
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
    auto impl = std::make_unique<Impl>(std::move(states), limits.maximum_variable_extremum_bytes);
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
  const auto fail = [this,
                     &resources](common::Status status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    input_.reset();
    impl_.reset();
    emitted_ = true;
    return common::make_unexpected(std::move(status));
  };
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return fail(active.error());

  while (input_ != nullptr) {
    common::Result<PhysicalOperatorStep> step = input_->next(resources);
    if (!step.has_value())
      return fail(step.error());
    if (step->kind() == PhysicalOperatorStepKind::kEnd) {
      input_.reset();
      break;
    }
    common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
    if (!chunk.has_value())
      return fail(chunk.error());
    if (!chunk->belongs_to(resources))
      return fail(invalid("aggregate input chunk belongs to another query"));
    const common::Result<void> consumed = impl_->consume(chunk->chunk(), resources);
    if (!consumed.has_value())
      return fail(consumed.error());
  }

  common::Result<std::vector<Impl::Result>> values = impl_->finish_all();
  if (!values.has_value())
    return fail(values.error());
  try {
    std::vector<ColumnOutputPosition> positions;
    positions.reserve(values->size());
    for (Impl::Result& result : *values) {
      positions.emplace_back(ConstantColumnOutputPosition{.value = std::move(result.value),
                                                          .force_nullable = result.nullable});
    }
    common::Result<PhysicalOperatorStep> result =
        materialize_single_row(resources, std::move(positions), output_limits_);
    if (!result.has_value())
      return fail(result.error());
    impl_.reset();
    emitted_ = true;
    return result;
  } catch (const std::bad_alloc&) {
    return fail(exhausted("aggregate output allocation failed"));
  } catch (const std::length_error&) {
    return fail(exhausted("aggregate output exceeds container limits"));
  }
}

namespace {

struct GroupState {
  std::vector<ScalarValue> key;
  std::vector<AggregateState> aggregates;
  QueryMemoryReservation reservation;
};

[[nodiscard]] common::Result<std::size_t> add_bytes(const std::size_t left, const std::size_t right,
                                                    const std::string_view message) {
  const std::optional<std::size_t> sum = common::checked_add(left, right);
  if (!sum.has_value())
    return common::make_unexpected(exhausted(message));
  return *sum;
}

[[nodiscard]] common::Result<std::size_t>
multiply_bytes(const std::size_t count, const std::size_t width, const std::string_view message) {
  const std::optional<std::size_t> product = common::checked_multiply(count, width);
  if (!product.has_value())
    return common::make_unexpected(exhausted(message));
  return *product;
}

[[nodiscard]] common::Result<bool> cell_equals_scalar(const columnar::ColumnCellView& cell,
                                                      const schema::LogicalType& type,
                                                      const ScalarValue& scalar) {
  if (cell.is_null() || scalar.is_null())
    return cell.is_null() == scalar.is_null();
  if (type.is_variable_width()) {
    const common::Result<common::ByteView> bytes = cell.bytes();
    if (!bytes.has_value())
      return common::make_unexpected(bytes.error());
    if (type.kind() == schema::LogicalTypeKind::kBinary) {
      const auto* stored = std::get_if<std::vector<std::byte>>(&scalar.storage());
      if (stored == nullptr)
        return common::make_unexpected(invalid("group key scalar storage is invalid"));
      return std::ranges::equal(*bytes, *stored);
    }
    const auto* stored = std::get_if<std::string>(&scalar.storage());
    if (stored == nullptr)
      return common::make_unexpected(invalid("group key scalar storage is invalid"));
    if (bytes->size() != stored->size())
      return false;
    return bytes->empty() || std::memcmp(bytes->data(), stored->data(), bytes->size()) == 0;
  }
  const common::Result<ScalarValue> physical = ScalarValue::from_column_cell(type, cell);
  if (!physical.has_value())
    return common::make_unexpected(physical.error());
  const common::Result<int> order =
      compare_scalar_values(*physical, scalar, ScalarNullPlacement::kLast);
  if (!order.has_value())
    return common::make_unexpected(order.error());
  return *order == 0;
}

[[nodiscard]] common::Result<std::size_t>
retained_group_bytes(const std::vector<ScalarValue>& key,
                     const std::vector<AggregateState>& aggregates) {
  common::Result<std::size_t> retained = multiply_bytes(
      key.capacity(), sizeof(ScalarValue), "grouped aggregate retained key size overflowed");
  if (!retained.has_value())
    return retained;
  common::Result<std::size_t> aggregate_bytes =
      multiply_bytes(aggregates.capacity(), sizeof(AggregateState),
                     "grouped aggregate retained state size overflowed");
  if (!aggregate_bytes.has_value())
    return aggregate_bytes;
  retained =
      add_bytes(*retained, *aggregate_bytes, "grouped aggregate retained state size overflowed");
  if (!retained.has_value())
    return retained;
  for (const ScalarValue& value : key) {
    if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr) {
      retained =
          add_bytes(*retained, text->capacity(), "grouped aggregate retained key size overflowed");
    } else if (const auto* binary = std::get_if<std::vector<std::byte>>(&value.storage());
               binary != nullptr) {
      retained = add_bytes(*retained, binary->capacity(),
                           "grouped aggregate retained key size overflowed");
    }
    if (!retained.has_value())
      return retained;
  }
  return retained;
}

} // namespace

class GroupedAggregateOperator::Impl {
public:
  struct EmittedGroup {
    std::vector<ColumnOutputPosition> positions;
    GroupState retained_state;
    QueryMemoryReservation emission_reservation;
  };

  Impl(std::vector<VectorGroupKeyDefinition> keys,
       std::vector<VectorAggregateDefinition> definitions,
       const GroupedAggregateLimits limits) noexcept
      : keys_(std::move(keys)), definitions_(std::move(definitions)), limits_(limits) {}

  [[nodiscard]] std::size_t group_count() const noexcept {
    return groups_.size();
  }

  [[nodiscard]] common::Result<void> consume(const VectorChunk& chunk,
                                             const QueryResourceContext& resources) {
    const common::Result<void> valid = validate_chunk(chunk);
    if (!valid.has_value())
      return valid;
    for (std::size_t selected_row = 0U; selected_row < chunk.selected_row_count(); ++selected_row) {
      if ((selected_row % 256U) == 0U) {
        const common::Result<void> active = resources.check_cancelled();
        if (!active.has_value())
          return common::make_unexpected(active.error());
      }
      common::Result<std::optional<std::size_t>> existing = find_group(chunk, selected_row);
      if (!existing.has_value())
        return common::make_unexpected(existing.error());
      std::size_t group_index = existing->value_or(groups_.size());
      if (group_index == groups_.size()) {
        common::Result<std::size_t> created = create_group(chunk, selected_row, resources);
        if (!created.has_value())
          return common::make_unexpected(created.error());
        group_index = *created;
      }
      common::Result<void> accumulated =
          accumulate_group(groups_[group_index], chunk, selected_row, resources);
      if (!accumulated.has_value())
        return accumulated;
    }
    return {};
  }

  [[nodiscard]] common::Result<EmittedGroup> take_output(const std::size_t group_index,
                                                         const QueryResourceContext& resources) {
    if (group_index >= groups_.size())
      return common::make_unexpected(out_of_range("grouped aggregate output index is invalid"));
    common::Result<std::size_t> position_bytes =
        multiply_bytes(keys_.size() + definitions_.size(), sizeof(ColumnOutputPosition) * 2U,
                       "grouped aggregate output configuration size overflowed");
    if (!position_bytes.has_value())
      return common::make_unexpected(position_bytes.error());
    common::Result<std::size_t> emission_charge =
        add_bytes(*position_bytes, kConservativeAllocationOverheadBytes,
                  "grouped aggregate output configuration size overflowed");
    if (!emission_charge.has_value())
      return common::make_unexpected(emission_charge.error());
    common::Result<QueryMemoryReservation> emission = resources.reserve(*emission_charge);
    if (!emission.has_value())
      return common::make_unexpected(emission.error());

    try {
      GroupState& source = groups_[group_index];
      std::vector<ColumnOutputPosition> positions;
      positions.reserve(keys_.size() + definitions_.size());
      const std::optional<std::size_t> retained_positions =
          common::checked_multiply(positions.capacity(), sizeof(ColumnOutputPosition));
      if (!retained_positions.has_value() || *retained_positions > emission->bytes()) {
        return common::make_unexpected(
            exhausted("grouped aggregate output allocation exceeded its charge"));
      }
      for (std::size_t key = 0U; key < keys_.size(); ++key) {
        positions.emplace_back(ConstantColumnOutputPosition{.value = std::move(source.key[key]),
                                                            .force_nullable = keys_[key].nullable});
      }
      for (AggregateState& state : source.aggregates) {
        common::Result<ScalarValue> value = finish(state);
        if (!value.has_value())
          return common::make_unexpected(value.error());
        common::Result<VectorAggregateOutputShape> shape =
            vector_aggregate_output_shape(state.definition);
        if (!shape.has_value())
          return common::make_unexpected(shape.error());
        positions.emplace_back(ConstantColumnOutputPosition{.value = std::move(*value),
                                                            .force_nullable = shape->nullable});
      }
      return EmittedGroup{.positions = std::move(positions),
                          .retained_state = std::move(source),
                          .emission_reservation = std::move(*emission)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("grouped aggregate output allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("grouped aggregate output exceeds container limits"));
    }
  }

private:
  [[nodiscard]] common::Result<void> validate_chunk(const VectorChunk& chunk) const {
    for (const VectorGroupKeyDefinition& key : keys_) {
      const columnar::PhysicalColumnView* column = chunk.column(key.column_ordinal);
      if (column == nullptr || column->type() != key.type || column->nullable() != key.nullable) {
        return common::make_unexpected(invalid("group key input column shape mismatch"));
      }
    }
    for (const VectorAggregateDefinition& definition : definitions_) {
      if (!definition.input.has_value())
        continue;
      const VectorAggregateInput& input = *definition.input;
      const columnar::PhysicalColumnView* column = chunk.column(input.column_ordinal);
      if (column == nullptr || column->type() != input.type ||
          column->nullable() != input.nullable) {
        return common::make_unexpected(invalid("aggregate input column shape mismatch"));
      }
    }
    return {};
  }

  [[nodiscard]] common::Result<std::optional<std::size_t>>
  find_group(const VectorChunk& chunk, const std::size_t selected_row) const {
    for (std::size_t candidate = 0U; candidate < groups_.size(); ++candidate) {
      bool equal = true;
      for (std::size_t key = 0U; key < keys_.size(); ++key) {
        const common::Result<columnar::ColumnCellView> cell =
            chunk.cell({.column_ordinal = keys_[key].column_ordinal, .selected_row = selected_row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        common::Result<bool> same =
            cell_equals_scalar(*cell, keys_[key].type, groups_[candidate].key[key]);
        if (!same.has_value())
          return common::make_unexpected(same.error());
        if (!*same) {
          equal = false;
          break;
        }
      }
      if (equal)
        return std::optional<std::size_t>{candidate};
    }
    return std::optional<std::size_t>{};
  }

  [[nodiscard]] common::Result<void> ensure_group_slots(const QueryResourceContext& resources) {
    if (group_slots_reservation_.is_valid())
      return {};
    common::Result<std::size_t> slot_bytes = multiply_bytes(
        limits_.maximum_groups, sizeof(GroupState) * 2U, "grouped aggregate slot size overflowed");
    if (!slot_bytes.has_value())
      return common::make_unexpected(slot_bytes.error());
    common::Result<std::size_t> charge =
        add_bytes(*slot_bytes, kConservativeAllocationOverheadBytes,
                  "grouped aggregate slot size overflowed");
    if (!charge.has_value())
      return common::make_unexpected(charge.error());
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    try {
      groups_.reserve(limits_.maximum_groups);
      const std::optional<std::size_t> retained =
          common::checked_multiply(groups_.capacity(), sizeof(GroupState));
      if (!retained.has_value() || *retained > reservation->bytes()) {
        std::vector<GroupState>{}.swap(groups_);
        return common::make_unexpected(
            exhausted("grouped aggregate slot allocation exceeded its charge"));
      }
      group_slots_reservation_ = std::move(*reservation);
      return {};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("grouped aggregate slot allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("grouped aggregate slots exceed container limits"));
    }
  }

  [[nodiscard]] common::Result<std::size_t> group_charge(const VectorChunk& chunk,
                                                         const std::size_t selected_row) const {
    common::Result<std::size_t> charge = multiply_bytes(
        keys_.size(), sizeof(ScalarValue) * 2U, "grouped aggregate key state size overflowed");
    if (!charge.has_value())
      return charge;
    common::Result<std::size_t> aggregate_bytes =
        multiply_bytes(definitions_.size(), sizeof(AggregateState) * 2U,
                       "grouped aggregate state size overflowed");
    if (!aggregate_bytes.has_value())
      return aggregate_bytes;
    charge = add_bytes(*charge, *aggregate_bytes, "grouped aggregate state size overflowed");
    if (!charge.has_value())
      return charge;
    std::size_t key_bytes = 0U;
    std::size_t allocation_count = 2U;
    for (const VectorGroupKeyDefinition& key : keys_) {
      if (!key.type.is_variable_width())
        continue;
      const common::Result<columnar::ColumnCellView> cell =
          chunk.cell({.column_ordinal = key.column_ordinal, .selected_row = selected_row});
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      if (cell->is_null())
        continue;
      const common::Result<common::ByteView> bytes = cell->bytes();
      if (!bytes.has_value())
        return common::make_unexpected(bytes.error());
      common::Result<std::size_t> next =
          add_bytes(key_bytes, bytes->size(), "group key payload size overflowed");
      if (!next.has_value())
        return next;
      key_bytes = *next;
      ++allocation_count;
    }
    if (key_bytes > limits_.maximum_key_bytes_per_group)
      return common::make_unexpected(exhausted("group key payload exceeds its byte limit"));
    charge = add_bytes(*charge, key_bytes, "grouped aggregate state size overflowed");
    if (!charge.has_value())
      return charge;
    common::Result<std::size_t> overhead =
        multiply_bytes(allocation_count, kConservativeAllocationOverheadBytes,
                       "grouped aggregate allocation overhead overflowed");
    if (!overhead.has_value())
      return overhead;
    return add_bytes(*charge, *overhead, "grouped aggregate state size overflowed");
  }

  [[nodiscard]] common::Result<std::size_t> create_group(const VectorChunk& chunk,
                                                         const std::size_t selected_row,
                                                         const QueryResourceContext& resources) {
    if (groups_.size() >= limits_.maximum_groups)
      return common::make_unexpected(exhausted("grouped aggregate group count exceeds its limit"));
    const common::Result<void> slots = ensure_group_slots(resources);
    if (!slots.has_value())
      return common::make_unexpected(slots.error());
    common::Result<std::size_t> charge = group_charge(chunk, selected_row);
    if (!charge.has_value())
      return common::make_unexpected(charge.error());
    common::Result<QueryMemoryReservation> reservation = resources.reserve(*charge);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    try {
      std::vector<ScalarValue> key;
      key.reserve(keys_.size());
      for (const VectorGroupKeyDefinition& definition : keys_) {
        const common::Result<columnar::ColumnCellView> cell =
            chunk.cell({.column_ordinal = definition.column_ordinal, .selected_row = selected_row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        common::Result<ScalarValue> value = ScalarValue::from_column_cell(definition.type, *cell);
        if (!value.has_value())
          return common::make_unexpected(value.error());
        key.push_back(std::move(*value));
      }
      std::vector<AggregateState> aggregates;
      aggregates.reserve(definitions_.size());
      for (const VectorAggregateDefinition& definition : definitions_)
        aggregates.emplace_back(definition);
      common::Result<std::size_t> retained = retained_group_bytes(key, aggregates);
      if (!retained.has_value())
        return common::make_unexpected(retained.error());
      if (*retained > reservation->bytes()) {
        return common::make_unexpected(
            exhausted("grouped aggregate state allocation exceeded its charge"));
      }
      groups_.push_back(GroupState{.key = std::move(key),
                                   .aggregates = std::move(aggregates),
                                   .reservation = std::move(*reservation)});
      return groups_.size() - 1U;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("grouped aggregate state allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("grouped aggregate state exceeds container limits"));
    }
  }

  [[nodiscard]] common::Result<void> accumulate_group(GroupState& group, const VectorChunk& chunk,
                                                      const std::size_t selected_row,
                                                      const QueryResourceContext& resources) const {
    for (AggregateState& state : group.aggregates) {
      if (state.definition.operation == VectorAggregateOperation::kCountStar) {
        common::Result<void> counted = increment_count(state.count);
        if (!counted.has_value())
          return counted;
        continue;
      }
      const VectorAggregateInput& input = *state.definition.input;
      const common::Result<columnar::ColumnCellView> cell =
          chunk.cell({.column_ordinal = input.column_ordinal, .selected_row = selected_row});
      if (!cell.has_value())
        return common::make_unexpected(cell.error());
      common::Result<void> accumulated =
          accumulate_cell(state, *cell, resources, limits_.maximum_variable_extremum_bytes);
      if (!accumulated.has_value())
        return accumulated;
    }
    return {};
  }

  std::vector<VectorGroupKeyDefinition> keys_;
  std::vector<VectorAggregateDefinition> definitions_;
  GroupedAggregateLimits limits_;
  std::vector<GroupState> groups_;
  QueryMemoryReservation group_slots_reservation_;
};

GroupedAggregateOperator::GroupedAggregateOperator(std::unique_ptr<PhysicalOperator> input,
                                                   std::unique_ptr<Impl> impl,
                                                   const VectorChunkLimits output_limits) noexcept
    : input_(std::move(input)), impl_(std::move(impl)), output_limits_(output_limits) {}

GroupedAggregateOperator::~GroupedAggregateOperator() = default;

common::Result<std::unique_ptr<PhysicalOperator>>
GroupedAggregateOperator::create(std::unique_ptr<PhysicalOperator> input,
                                 const std::vector<VectorGroupKeyDefinition>& keys,
                                 const std::vector<VectorAggregateDefinition>& definitions,
                                 const GroupedAggregateLimits limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("grouped aggregate input operator is required"));
  if (limits.maximum_groups == 0U || limits.maximum_groups > kMaximumGroupedAggregateGroups ||
      limits.maximum_group_keys == 0U || limits.maximum_group_keys > kMaximumGroupedAggregateKeys ||
      limits.maximum_aggregates > kMaximumGroupedAggregateWidth ||
      limits.maximum_key_bytes_per_group == 0U || limits.maximum_variable_extremum_bytes == 0U ||
      limits.maximum_retained_configuration_bytes == 0U ||
      limits.output_limits.maximum_rows == 0U || limits.output_limits.maximum_columns == 0U ||
      limits.output_limits.maximum_buffer_bytes == 0U ||
      limits.output_limits.maximum_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("grouped aggregate limits are invalid"));
  }
  if (keys.empty())
    return common::make_unexpected(invalid("grouped aggregate requires at least one key"));
  if (keys.size() > limits.maximum_group_keys)
    return common::make_unexpected(exhausted("grouped aggregate key width exceeds its limit"));
  if (definitions.size() > limits.maximum_aggregates)
    return common::make_unexpected(
        exhausted("grouped aggregate definition width exceeds its limit"));
  const std::optional<std::size_t> output_width =
      common::checked_add(keys.size(), definitions.size());
  if (!output_width.has_value() || *output_width > limits.output_limits.maximum_columns)
    return common::make_unexpected(exhausted("grouped aggregate output width exceeds its limit"));
  for (const VectorAggregateDefinition& definition : definitions) {
    common::Result<VectorAggregateOutputShape> shape = vector_aggregate_output_shape(definition);
    if (!shape.has_value())
      return common::make_unexpected(shape.error());
  }
  const std::optional<std::size_t> key_bytes =
      common::checked_multiply(keys.size(), sizeof(VectorGroupKeyDefinition));
  const std::optional<std::size_t> definition_bytes =
      common::checked_multiply(definitions.size(), sizeof(VectorAggregateDefinition));
  const std::optional<std::size_t> configuration_bytes =
      key_bytes.has_value() && definition_bytes.has_value()
          ? common::checked_add(*key_bytes, *definition_bytes)
          : std::nullopt;
  if (!configuration_bytes.has_value() ||
      *configuration_bytes > limits.maximum_retained_configuration_bytes) {
    return common::make_unexpected(
        exhausted("grouped aggregate retained configuration exceeds its byte limit"));
  }
  try {
    std::vector<VectorGroupKeyDefinition> retained_keys{keys};
    std::vector<VectorAggregateDefinition> retained_definitions{definitions};
    const std::optional<std::size_t> retained_key_bytes =
        common::checked_multiply(retained_keys.capacity(), sizeof(VectorGroupKeyDefinition));
    const std::optional<std::size_t> retained_definition_bytes = common::checked_multiply(
        retained_definitions.capacity(), sizeof(VectorAggregateDefinition));
    const std::optional<std::size_t> retained =
        retained_key_bytes.has_value() && retained_definition_bytes.has_value()
            ? common::checked_add(*retained_key_bytes, *retained_definition_bytes)
            : std::nullopt;
    if (!retained.has_value() || *retained > limits.maximum_retained_configuration_bytes) {
      return common::make_unexpected(
          exhausted("grouped aggregate retained configuration exceeds its byte limit"));
    }
    auto impl =
        std::make_unique<Impl>(std::move(retained_keys), std::move(retained_definitions), limits);
    return std::unique_ptr<PhysicalOperator>{
        new GroupedAggregateOperator{std::move(input), std::move(impl), limits.output_limits}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("grouped aggregate operator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped aggregate configuration exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep>
GroupedAggregateOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  const auto fail = [this,
                     &resources](common::Status status) -> common::Result<PhysicalOperatorStep> {
    static_cast<void>(resources.request_cancel());
    input_.reset();
    impl_.reset();
    ended_ = true;
    return common::make_unexpected(std::move(status));
  };
  const common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return fail(active.error());
  if (!input_consumed_) {
    while (input_ != nullptr) {
      common::Result<PhysicalOperatorStep> step = input_->next(resources);
      if (!step.has_value())
        return fail(step.error());
      if (step->kind() == PhysicalOperatorStepKind::kEnd) {
        input_.reset();
        break;
      }
      common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
      if (!chunk.has_value())
        return fail(chunk.error());
      if (!chunk->belongs_to(resources))
        return fail(invalid("grouped aggregate input chunk belongs to another query"));
      common::Result<void> consumed = impl_->consume(chunk->chunk(), resources);
      if (!consumed.has_value())
        return fail(consumed.error());
    }
    input_consumed_ = true;
  }
  if (output_group_ >= impl_->group_count()) {
    impl_.reset();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }
  common::Result<Impl::EmittedGroup> emitted = impl_->take_output(output_group_, resources);
  if (!emitted.has_value())
    return fail(emitted.error());
  common::Result<PhysicalOperatorStep> output =
      materialize_single_row(resources, std::move(emitted->positions), output_limits_);
  if (!output.has_value())
    return fail(output.error());
  ++output_group_;
  return output;
}

} // namespace chronos::query
