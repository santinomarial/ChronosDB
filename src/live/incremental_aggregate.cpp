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

namespace chronos::live {

class IncrementalAggregateSet::Impl {
public:
  using OrderKey = std::tuple<std::int64_t, std::uint64_t, std::uint64_t>;

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

common::Status IncrementalAggregateSet::upsert(AggregateInput input) {
  if (input.row_identity == 0U || !std::isfinite(input.weight)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "aggregate row identity must be nonzero and weight finite"};
  }
  const auto existing = impl_->rows.find(input.row_identity);
  if (existing != impl_->rows.end()) {
    impl_->remove(existing->second);
    existing->second = input;
    impl_->add(existing->second);
    return common::Status::ok();
  }
  const auto [iterator, inserted] = impl_->rows.emplace(input.row_identity, input);
  if (!inserted) {
    return common::Status{common::StatusCode::kInternal,
                          "aggregate row insertion did not make progress"};
  }
  impl_->add(iterator->second);
  return common::Status::ok();
}

common::Status IncrementalAggregateSet::erase(const std::uint64_t row_identity) {
  if (row_identity == 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "aggregate row identity must be nonzero"};
  }
  const auto existing = impl_->rows.find(row_identity);
  if (existing == impl_->rows.end()) {
    return common::Status::ok();
  }
  impl_->remove(existing->second);
  impl_->rows.erase(existing);
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

std::size_t IncrementalAggregateSet::retained_rows() const noexcept {
  return impl_->rows.size();
}

} // namespace chronos::live
