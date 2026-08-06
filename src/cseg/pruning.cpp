#include "chronos/cseg/pruning.hpp"

#include "chronos/common/checked_math.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool predicate_is_empty(const EventTimePredicate& predicate) noexcept {
  if (!predicate.lower.has_value() || !predicate.upper.has_value()) {
    return false;
  }
  if (predicate.lower->value != predicate.upper->value) {
    return predicate.lower->value > predicate.upper->value;
  }
  return !predicate.lower->inclusive || !predicate.upper->inclusive;
}

[[nodiscard]] bool range_is_disjoint(const std::int64_t minimum_event_time,
                                     const std::int64_t maximum_event_time,
                                     const EventTimePredicate& predicate) noexcept {
  if (predicate_is_empty(predicate)) {
    return true;
  }
  if (predicate.lower.has_value() &&
      (maximum_event_time < predicate.lower->value ||
       (maximum_event_time == predicate.lower->value && !predicate.lower->inclusive))) {
    return true;
  }
  return predicate.upper.has_value() &&
         (minimum_event_time > predicate.upper->value ||
          (minimum_event_time == predicate.upper->value && !predicate.upper->inclusive));
}

} // namespace

CsegEventTimePruningPlan::CsegEventTimePruningPlan(PartId part_id,
                                                   std::vector<std::uint32_t> selected_granules,
                                                   const std::uint64_t selected_rows,
                                                   const std::uint64_t skipped_rows,
                                                   const std::uint32_t skipped_granules) noexcept
    : part_id_(part_id), selected_granules_(std::move(selected_granules)),
      selected_rows_(selected_rows), skipped_rows_(skipped_rows),
      skipped_granules_(skipped_granules) {}

const PartId& CsegEventTimePruningPlan::part_id() const noexcept {
  return part_id_;
}

std::span<const std::uint32_t> CsegEventTimePruningPlan::selected_granules() const noexcept {
  return selected_granules_;
}

std::uint64_t CsegEventTimePruningPlan::selected_rows() const noexcept {
  return selected_rows_;
}

std::uint64_t CsegEventTimePruningPlan::skipped_rows() const noexcept {
  return skipped_rows_;
}

std::uint32_t CsegEventTimePruningPlan::skipped_granules() const noexcept {
  return skipped_granules_;
}

common::Result<bool>
cseg_event_time_range_may_match(const std::int64_t minimum_event_time,
                                const std::int64_t maximum_event_time,
                                const std::optional<EventTimePredicate>& predicate) {
  if (minimum_event_time > maximum_event_time) {
    return common::make_unexpected(invalid("CSEG pruning range has reversed stored extrema"));
  }
  return !predicate.has_value() ||
         !range_is_disjoint(minimum_event_time, maximum_event_time, *predicate);
}

common::Result<CsegEventTimePruningPlan>
plan_cseg_v1_event_time_pruning(const DecodedCsegMetadataView& metadata,
                                const std::optional<EventTimePredicate>& predicate,
                                const CsegEventTimePruningLimits limits) {
  if (metadata.granules().size() > limits.max_granules) {
    return common::make_unexpected(
        exhausted("CSEG pruning plan exceeds its configured granule limit"));
  }
  const common::Result<bool> part_may_match = cseg_event_time_range_may_match(
      metadata.minimum_event_time(), metadata.maximum_event_time(), predicate);
  if (!part_may_match.has_value()) {
    return common::make_unexpected(part_may_match.error());
  }

  try {
    std::vector<std::uint32_t> selected;
    if (*part_may_match) {
      selected.reserve(metadata.granules().size());
    }
    std::uint64_t selected_rows = 0U;
    std::uint64_t skipped_rows = 0U;
    std::uint32_t skipped_granules = 0U;
    for (std::size_t ordinal = 0U; ordinal < metadata.granules().size(); ++ordinal) {
      const CsegGranuleDescriptor& granule = metadata.granules()[ordinal];
      const common::Result<bool> granule_may_match =
          *part_may_match ? cseg_event_time_range_may_match(granule.minimum_event_time,
                                                            granule.maximum_event_time, predicate)
                          : common::Result<bool>{false};
      if (!granule_may_match.has_value()) {
        return common::make_unexpected(granule_may_match.error());
      }
      std::uint64_t* const row_counter = *granule_may_match ? &selected_rows : &skipped_rows;
      const std::optional<std::uint64_t> next =
          common::checked_add(*row_counter, static_cast<std::uint64_t>(granule.row_count));
      if (!next.has_value()) {
        return common::make_unexpected(exhausted("CSEG pruning row accounting overflows"));
      }
      *row_counter = *next;
      if (*granule_may_match) {
        if (ordinal > std::numeric_limits<std::uint32_t>::max()) {
          return common::make_unexpected(exhausted("CSEG pruning ordinal exceeds uint32"));
        }
        selected.push_back(static_cast<std::uint32_t>(ordinal));
      } else {
        ++skipped_granules;
      }
    }
    return CsegEventTimePruningPlan{metadata.part_id(), std::move(selected), selected_rows,
                                    skipped_rows, skipped_granules};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Cannot allocate CSEG pruning plan"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("CSEG pruning plan exceeds container limits"));
  }
}

} // namespace chronos::cseg
