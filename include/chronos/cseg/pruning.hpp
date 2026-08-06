#ifndef CHRONOS_CSEG_PRUNING_HPP_
#define CHRONOS_CSEG_PRUNING_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/metadata_codec.hpp"

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cseg {

struct EventTimeBound {
  std::int64_t value{};
  bool inclusive{true};

  friend bool operator==(const EventTimeBound&, const EventTimeBound&) = default;
};

// A normalized conjunction of optional lower and upper event-time bounds. Reversed bounds and an
// equal endpoint with either side open are valid empty predicates, not errors.
struct EventTimePredicate {
  std::optional<EventTimeBound> lower;
  std::optional<EventTimeBound> upper;

  friend bool operator==(const EventTimePredicate&, const EventTimePredicate&) = default;
};

struct CsegEventTimePruningLimits {
  std::uint32_t max_granules{format::kMaximumGranuleCount};
};

class CsegEventTimePruningPlan {
public:
  CsegEventTimePruningPlan() = delete;

  [[nodiscard]] const PartId& part_id() const noexcept;
  [[nodiscard]] std::span<const std::uint32_t> selected_granules() const noexcept;
  [[nodiscard]] std::uint64_t selected_rows() const noexcept;
  [[nodiscard]] std::uint64_t skipped_rows() const noexcept;
  [[nodiscard]] std::uint32_t skipped_granules() const noexcept;

private:
  CsegEventTimePruningPlan(PartId part_id, std::vector<std::uint32_t> selected_granules,
                           std::uint64_t selected_rows, std::uint64_t skipped_rows,
                           std::uint32_t skipped_granules) noexcept;

  PartId part_id_;
  std::vector<std::uint32_t> selected_granules_;
  std::uint64_t selected_rows_{};
  std::uint64_t skipped_rows_{};
  std::uint32_t skipped_granules_{};

  friend common::Result<CsegEventTimePruningPlan>
  plan_cseg_v1_event_time_pruning(const DecodedCsegMetadataView&,
                                  const std::optional<EventTimePredicate>&,
                                  CsegEventTimePruningLimits);
};

// Returns true unless disjointness is mathematically proven. Invalid stored extrema are rejected
// rather than interpreted as pruning evidence. A missing predicate always scans.
[[nodiscard]] common::Result<bool>
cseg_event_time_range_may_match(std::int64_t minimum_event_time, std::int64_t maximum_event_time,
                                const std::optional<EventTimePredicate>& predicate);

// Produces an owned immutable ordinal plan from already authenticated CSEG metadata. It never
// reads page bytes. The projected reader remains responsible for validating every selected page.
[[nodiscard]] common::Result<CsegEventTimePruningPlan>
plan_cseg_v1_event_time_pruning(const DecodedCsegMetadataView& metadata,
                                const std::optional<EventTimePredicate>& predicate = std::nullopt,
                                CsegEventTimePruningLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_PRUNING_HPP_
