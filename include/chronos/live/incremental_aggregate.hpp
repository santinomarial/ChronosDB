#ifndef CHRONOS_LIVE_INCREMENTAL_AGGREGATE_HPP_
#define CHRONOS_LIVE_INCREMENTAL_AGGREGATE_HPP_

#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::live {

struct AggregateInput {
  std::uint64_t row_identity{};
  std::int64_t event_time{};
  std::uint64_t source_order{};
  double value{};
  double weight{1.0};

  friend bool operator==(const AggregateInput&, const AggregateInput&) = default;
};

struct IncrementalAggregateCheckpoint {
  std::vector<AggregateInput> rows;
  std::uint64_t count{};
  double sum{};
  double weighted_sum{};
  double weight_sum{};
  double mean{};
  double m2{};
};

struct OhlcValue {
  double open{};
  double high{};
  double low{};
  double close{};

  friend bool operator==(const OhlcValue&, const OhlcValue&) = default;
};

struct AggregateSnapshot {
  std::uint64_t count{};
  double sum{};
  std::optional<double> minimum;
  std::optional<double> maximum;
  std::optional<double> vwap;
  std::optional<OhlcValue> ohlc;
  std::optional<double> variance_population;
  std::optional<double> variance_sample;

  friend bool operator==(const AggregateSnapshot&, const AggregateSnapshot&) = default;
};

// Single-owner exact removable state for the Phase 11 numeric aggregate family. Each logical row
// identity has at most one visible contribution. Upsert replaces its prior contribution; erase is
// idempotent. Min/max/OHLC retain ordered state so deleting an endpoint remains exact.
class IncrementalAggregateSet {
public:
  IncrementalAggregateSet();
  ~IncrementalAggregateSet();

  IncrementalAggregateSet(const IncrementalAggregateSet&) = delete;
  IncrementalAggregateSet& operator=(const IncrementalAggregateSet&) = delete;
  IncrementalAggregateSet(IncrementalAggregateSet&&) noexcept;
  IncrementalAggregateSet& operator=(IncrementalAggregateSet&&) noexcept;

  [[nodiscard]] common::Status validate_upsert(const AggregateInput& input) const;
  [[nodiscard]] common::Status validate_erase(std::uint64_t row_identity) const;
  [[nodiscard]] common::Status upsert(AggregateInput input);
  [[nodiscard]] common::Status erase(std::uint64_t row_identity);
  [[nodiscard]] AggregateSnapshot snapshot() const;
  [[nodiscard]] common::Result<IncrementalAggregateCheckpoint> checkpoint() const;
  [[nodiscard]] static common::Result<IncrementalAggregateSet>
  restore(IncrementalAggregateCheckpoint checkpoint);
  [[nodiscard]] std::size_t retained_rows() const noexcept;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_INCREMENTAL_AGGREGATE_HPP_
