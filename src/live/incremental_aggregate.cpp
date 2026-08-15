#include "chronos/live/incremental_aggregate.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <utility>
#include <vector>

namespace chronos::live {

class IncrementalAggregateSet::Impl {
public:
  using OrderKey = std::tuple<std::int64_t, std::uint64_t, std::uint64_t>;

  struct NumericState {
    std::uint64_t count{};
    double sum{};
    double weighted_sum{};
    double weight_sum{};
    double mean{};
    double m2{};
  };

  [[nodiscard]] NumericState numeric_state() const noexcept {
    return NumericState{count, sum, weighted_sum, weight_sum, mean, m2};
  }

  static void add_numeric(NumericState& state, const AggregateInput& input) noexcept {
    ++state.count;
    state.sum += input.value;
    state.weighted_sum += input.value * input.weight;
    state.weight_sum += input.weight;
    const double delta = input.value - state.mean;
    state.mean += delta / static_cast<double>(state.count);
    const double delta_after = input.value - state.mean;
    state.m2 += delta * delta_after;
  }

  static void remove_numeric(NumericState& state, const AggregateInput& input) noexcept {
    state.sum -= input.value;
    state.weighted_sum -= input.value * input.weight;
    state.weight_sum -= input.weight;
    if (state.count <= 1U) {
      state = {};
      return;
    }
    const std::uint64_t next_count = state.count - 1U;
    const double delta = input.value - state.mean;
    const double next_mean = state.mean - delta / static_cast<double>(next_count);
    state.m2 -= delta * (input.value - next_mean);
    if (state.m2 < 0.0 && std::abs(state.m2) < 1e-12) {
      state.m2 = 0.0;
    }
    state.mean = next_mean;
    state.count = next_count;
  }

  [[nodiscard]] static bool finite(const NumericState& state) noexcept {
    return std::isfinite(state.sum) && std::isfinite(state.weighted_sum) &&
           std::isfinite(state.weight_sum) && std::isfinite(state.mean) && std::isfinite(state.m2);
  }

  void install_numeric(const NumericState& state) noexcept {
    count = state.count;
    sum = state.sum;
    weighted_sum = state.weighted_sum;
    weight_sum = state.weight_sum;
    mean = state.mean;
    m2 = state.m2;
  }

  void add(const AggregateInput& input) {
    ++count;
    sum += input.value;
    weighted_sum += input.value * input.weight;
    weight_sum += input.weight;
    ordered_values.insert(input.value);
    ordered_events.emplace(OrderKey{input.event_time, input.source_order, input.row_identity},
                           input.value);

    const double delta = input.value - mean;
    mean += delta / static_cast<double>(count);
    const double delta_after = input.value - mean;
    m2 += delta * delta_after;
  }

  void remove(const AggregateInput& input) {
    sum -= input.value;
    weighted_sum -= input.value * input.weight;
    weight_sum -= input.weight;
    const auto value = ordered_values.find(input.value);
    if (value != ordered_values.end()) {
      ordered_values.erase(value);
    }
    ordered_events.erase(OrderKey{input.event_time, input.source_order, input.row_identity});

    if (count <= 1U) {
      count = 0U;
      mean = 0.0;
      m2 = 0.0;
      sum = 0.0;
      weighted_sum = 0.0;
      weight_sum = 0.0;
      return;
    }
    const std::uint64_t next_count = count - 1U;
    const double delta = input.value - mean;
    const double next_mean = mean - delta / static_cast<double>(next_count);
    m2 -= delta * (input.value - next_mean);
    if (m2 < 0.0 && std::abs(m2) < 1e-12) {
      m2 = 0.0;
    }
    mean = next_mean;
    count = next_count;
  }

  std::map<std::uint64_t, AggregateInput> rows;
  std::multiset<double> ordered_values;
  std::map<OrderKey, double> ordered_events;
  std::uint64_t count{};
  double sum{};
  double weighted_sum{};
  double weight_sum{};
  double mean{};
  double m2{};
};

IncrementalAggregateSet::IncrementalAggregateSet() : impl_(std::make_unique<Impl>()) {}
IncrementalAggregateSet::~IncrementalAggregateSet() = default;
IncrementalAggregateSet::IncrementalAggregateSet(IncrementalAggregateSet&&) noexcept = default;
IncrementalAggregateSet&
IncrementalAggregateSet::operator=(IncrementalAggregateSet&&) noexcept = default;

common::Status IncrementalAggregateSet::validate_upsert(const AggregateInput& input) const {
  if (input.row_identity == 0U || input.source_order == 0U || !std::isfinite(input.value) ||
      !std::isfinite(input.weight) || !std::isfinite(input.value * input.weight)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "aggregate row identity/order and finite value/weight are required"};
  }
  const auto existing = impl_->rows.find(input.row_identity);
  Impl::NumericState candidate = impl_->numeric_state();
  if (existing != impl_->rows.end()) {
    Impl::remove_numeric(candidate, existing->second);
  }
  Impl::add_numeric(candidate, input);
  if (!Impl::finite(candidate)) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "aggregate numeric state would overflow"};
  }
  return common::Status::ok();
}

common::Status IncrementalAggregateSet::validate_erase(const std::uint64_t row_identity) const {
  if (row_identity == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "aggregate row identity must be nonzero"};
  }
  const auto existing = impl_->rows.find(row_identity);
  if (existing == impl_->rows.end()) {
    return common::Status::ok();
  }
  Impl::NumericState candidate = impl_->numeric_state();
  Impl::remove_numeric(candidate, existing->second);
  return Impl::finite(candidate)
             ? common::Status::ok()
             : common::Status{common::StatusCode::kOutOfRange,
                              "aggregate removal would produce invalid numeric state"};
}

common::Status IncrementalAggregateSet::upsert(AggregateInput input) {
  common::Status validated = validate_upsert(input);
  if (!validated.is_ok()) {
    return validated;
  }
  const auto existing = impl_->rows.find(input.row_identity);
  Impl::NumericState candidate = impl_->numeric_state();
  if (existing != impl_->rows.end()) {
    Impl::remove_numeric(candidate, existing->second);
  }
  Impl::add_numeric(candidate, input);
  if (existing != impl_->rows.end()) {
    impl_->remove(existing->second);
    existing->second = input;
    impl_->add(existing->second);
    impl_->install_numeric(candidate);
    return common::Status::ok();
  }
  const auto [iterator, inserted] = impl_->rows.emplace(input.row_identity, input);
  if (!inserted) {
    return common::Status{common::StatusCode::kInternal,
                          "aggregate row insertion did not make progress"};
  }
  impl_->add(iterator->second);
  impl_->install_numeric(candidate);
  return common::Status::ok();
}

common::Status IncrementalAggregateSet::erase(const std::uint64_t row_identity) {
  common::Status validated = validate_erase(row_identity);
  if (!validated.is_ok()) {
    return validated;
  }
  const auto existing = impl_->rows.find(row_identity);
  if (existing == impl_->rows.end()) {
    return common::Status::ok();
  }
  Impl::NumericState candidate = impl_->numeric_state();
  Impl::remove_numeric(candidate, existing->second);
  impl_->remove(existing->second);
  impl_->rows.erase(existing);
  impl_->install_numeric(candidate);
  return common::Status::ok();
}

AggregateSnapshot IncrementalAggregateSet::snapshot() const {
  AggregateSnapshot output{};
  output.count = impl_->count;
  output.sum = impl_->sum;
  if (impl_->count == 0U) {
    return output;
  }
  output.minimum = *impl_->ordered_values.begin();
  output.maximum = *impl_->ordered_values.rbegin();
  if (impl_->weight_sum != 0.0) {
    output.vwap = impl_->weighted_sum / impl_->weight_sum;
  }
  const double open = impl_->ordered_events.begin()->second;
  const double close = impl_->ordered_events.rbegin()->second;
  output.ohlc = OhlcValue{open, *output.maximum, *output.minimum, close};
  output.variance_population = impl_->m2 / static_cast<double>(impl_->count);
  if (impl_->count > 1U) {
    output.variance_sample = impl_->m2 / static_cast<double>(impl_->count - 1U);
  }
  return output;
}

common::Result<IncrementalAggregateCheckpoint> IncrementalAggregateSet::checkpoint() const {
  try {
    IncrementalAggregateCheckpoint checkpoint;
    checkpoint.rows.reserve(impl_->rows.size());
    for (const auto& [identity, input] : impl_->rows) {
      static_cast<void>(identity);
      checkpoint.rows.push_back(input);
    }
    checkpoint.count = impl_->count;
    checkpoint.sum = impl_->sum;
    checkpoint.weighted_sum = impl_->weighted_sum;
    checkpoint.weight_sum = impl_->weight_sum;
    checkpoint.mean = impl_->mean;
    checkpoint.m2 = impl_->m2;
    return checkpoint;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "aggregate checkpoint allocation failed"});
  }
}

common::Result<IncrementalAggregateSet>
IncrementalAggregateSet::restore(const IncrementalAggregateCheckpoint& checkpoint) {
  if (checkpoint.count != checkpoint.rows.size() ||
      !Impl::finite(Impl::NumericState{checkpoint.count, checkpoint.sum, checkpoint.weighted_sum,
                                       checkpoint.weight_sum, checkpoint.mean, checkpoint.m2})) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "aggregate checkpoint numeric state is inconsistent"});
  }
  std::uint64_t prior_identity = 0U;
  IncrementalAggregateSet restored;
  try {
    for (const AggregateInput& input : checkpoint.rows) {
      if (input.row_identity <= prior_identity || input.source_order == 0U ||
          !std::isfinite(input.value) || !std::isfinite(input.weight) ||
          !std::isfinite(input.value * input.weight)) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kCorruption, "aggregate checkpoint rows are not canonical"});
      }
      prior_identity = input.row_identity;
      restored.impl_->rows.emplace(input.row_identity, input);
      restored.impl_->ordered_values.insert(input.value);
      restored.impl_->ordered_events.emplace(
          Impl::OrderKey{input.event_time, input.source_order, input.row_identity}, input.value);
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "aggregate checkpoint restore allocation failed"});
  }
  restored.impl_->count = checkpoint.count;
  restored.impl_->sum = checkpoint.sum;
  restored.impl_->weighted_sum = checkpoint.weighted_sum;
  restored.impl_->weight_sum = checkpoint.weight_sum;
  restored.impl_->mean = checkpoint.mean;
  restored.impl_->m2 = checkpoint.m2;
  return restored;
}

std::size_t IncrementalAggregateSet::retained_rows() const noexcept {
  return impl_->rows.size();
}

} // namespace chronos::live
