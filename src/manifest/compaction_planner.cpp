#include "chronos/manifest/compaction_planner.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <numeric>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace detail {

class CompactionPlannerBuilder {
public:
  [[nodiscard]] static PlannedAppendOnlyCompaction
  make(schema::TableId table_id, schema::TabletId tablet_id, schema::SchemaId schema_id,
       const schema::SchemaVersion schema_version, std::vector<cseg::PartId> input_part_ids,
       const std::uint64_t input_bytes, const std::uint64_t input_rows,
       const std::int64_t minimum_event_time, const std::int64_t maximum_event_time,
       const std::uint32_t delta_input_parts) {
    return {
        table_id,    tablet_id,  schema_id,          schema_version,     std::move(input_part_ids),
        input_bytes, input_rows, minimum_event_time, maximum_event_time, delta_input_parts};
  }
};

} // namespace detail

namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool same_group(const PartDescriptor& left, const PartDescriptor& right) noexcept {
  return left.table_id == right.table_id && left.tablet_id == right.tablet_id &&
         left.schema_id == right.schema_id && left.schema_version == right.schema_version;
}

[[nodiscard]] auto group_key(const PartDescriptor& part) noexcept {
  return std::tie(part.table_id, part.tablet_id, part.schema_id, part.schema_version);
}

[[nodiscard]] bool ranges_overlap(const PartDescriptor& left,
                                  const PartDescriptor& right) noexcept {
  return left.minimum_event_time <= right.maximum_event_time &&
         right.minimum_event_time <= left.maximum_event_time;
}

[[nodiscard]] common::Result<std::vector<AppendOnlyPartRole>>
classify_roles(const std::span<const PartDescriptor> parts, const std::uint32_t maximum_parts) {
  if (parts.size() > maximum_parts) {
    return common::make_unexpected(
        exhausted("Append-only part classification exceeds its configured part limit"));
  }
  try {
    std::vector<std::size_t> indices(parts.size());
    std::iota(indices.begin(), indices.end(), std::size_t{0U});
    for (const PartDescriptor& part : parts) {
      if (part.file_length == 0U || part.row_count == 0U || part.minimum_record_sequence == 0U ||
          part.minimum_record_sequence > part.maximum_record_sequence ||
          part.minimum_event_time > part.maximum_event_time) {
        return common::make_unexpected(
            invalid("Append-only planner received an invalid part descriptor"));
      }
    }
    std::ranges::sort(indices, [&](const std::size_t left, const std::size_t right) {
      const PartDescriptor& left_part = parts[left];
      const PartDescriptor& right_part = parts[right];
      return std::tuple{group_key(left_part), left_part.maximum_record_sequence,
                        left_part.part_id} < std::tuple{group_key(right_part),
                                                        right_part.maximum_record_sequence,
                                                        right_part.part_id};
    });
    std::vector<cseg::PartId> identities;
    identities.reserve(parts.size());
    for (const PartDescriptor& part : parts) {
      identities.push_back(part.part_id);
    }
    std::ranges::sort(identities);
    if (std::ranges::adjacent_find(identities) != identities.end()) {
      return common::make_unexpected(invalid("Append-only planner part identities are not unique"));
    }

    std::vector<AppendOnlyPartRole> roles(parts.size(), AppendOnlyPartRole::kBase);
    std::size_t group_begin = 0U;
    while (group_begin < indices.size()) {
      std::size_t group_end = group_begin + 1U;
      while (group_end < indices.size() &&
             same_group(parts[indices[group_begin]], parts[indices[group_end]])) {
        ++group_end;
      }
      std::int64_t frontier = parts[indices[group_begin]].maximum_event_time;
      for (std::size_t position = group_begin + 1U; position < group_end; ++position) {
        const PartDescriptor& part = parts[indices[position]];
        if (part.minimum_event_time < frontier) {
          roles[indices[position]] = AppendOnlyPartRole::kDelta;
        }
        frontier = std::max(frontier, part.maximum_event_time);
      }
      group_begin = group_end;
    }
    return roles;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Cannot allocate append-only planner state"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Append-only planner state exceeds container limits"));
  }
}

[[nodiscard]] common::Result<std::optional<PlannedAppendOnlyCompaction>>
make_plan(const std::span<const PartDescriptor> parts,
          const std::span<const AppendOnlyPartRole> roles,
          const std::span<const std::size_t> ordered_candidates,
          const AppendOnlyCompactionPlannerLimits limits, const bool require_first) {
  std::vector<std::size_t> selected;
  selected.reserve(std::min<std::size_t>(ordered_candidates.size(), limits.maximum_input_parts));
  std::uint64_t bytes = 0U;
  std::uint64_t rows = 0U;
  for (std::size_t position = 0U; position < ordered_candidates.size(); ++position) {
    const std::size_t index = ordered_candidates[position];
    if (selected.size() >= limits.maximum_input_parts) {
      break;
    }
    const std::optional<std::uint64_t> next_bytes =
        common::checked_add(bytes, parts[index].file_length);
    const std::optional<std::uint64_t> next_rows =
        common::checked_add(rows, parts[index].row_count);
    if (!next_bytes.has_value() || !next_rows.has_value() ||
        *next_bytes > limits.maximum_input_bytes || *next_rows > limits.maximum_input_rows) {
      if (position == 0U && require_first) {
        return std::optional<PlannedAppendOnlyCompaction>{};
      }
      continue;
    }
    selected.push_back(index);
    bytes = *next_bytes;
    rows = *next_rows;
  }
  if (selected.size() < limits.minimum_input_parts) {
    return std::optional<PlannedAppendOnlyCompaction>{};
  }
  std::ranges::sort(selected, [&](const std::size_t left, const std::size_t right) {
    return parts[left].part_id < parts[right].part_id;
  });
  std::vector<cseg::PartId> identities;
  identities.reserve(selected.size());
  std::int64_t minimum = parts[selected.front()].minimum_event_time;
  std::int64_t maximum = parts[selected.front()].maximum_event_time;
  std::uint32_t delta_parts = 0U;
  for (const std::size_t index : selected) {
    identities.push_back(parts[index].part_id);
    minimum = std::min(minimum, parts[index].minimum_event_time);
    maximum = std::max(maximum, parts[index].maximum_event_time);
    if (roles[index] == AppendOnlyPartRole::kDelta) {
      ++delta_parts;
    }
  }
  const PartDescriptor& first = parts[selected.front()];
  return std::optional<PlannedAppendOnlyCompaction>{detail::CompactionPlannerBuilder::make(
      first.table_id, first.tablet_id, first.schema_id, first.schema_version, std::move(identities),
      bytes, rows, minimum, maximum, delta_parts)};
}

} // namespace

PlannedAppendOnlyCompaction::PlannedAppendOnlyCompaction(
    schema::TableId table_id, schema::TabletId tablet_id, schema::SchemaId schema_id,
    const schema::SchemaVersion schema_version, std::vector<cseg::PartId> input_part_ids,
    const std::uint64_t input_bytes, const std::uint64_t input_rows,
    const std::int64_t minimum_event_time, const std::int64_t maximum_event_time,
    const std::uint32_t delta_input_parts) noexcept
    : table_id_(table_id), tablet_id_(tablet_id), schema_id_(schema_id),
      schema_version_(schema_version), input_part_ids_(std::move(input_part_ids)),
      input_bytes_(input_bytes), input_rows_(input_rows), minimum_event_time_(minimum_event_time),
      maximum_event_time_(maximum_event_time), delta_input_parts_(delta_input_parts) {}

const schema::TableId& PlannedAppendOnlyCompaction::table_id() const noexcept {
  return table_id_;
}

const schema::TabletId& PlannedAppendOnlyCompaction::tablet_id() const noexcept {
  return tablet_id_;
}

const schema::SchemaId& PlannedAppendOnlyCompaction::schema_id() const noexcept {
  return schema_id_;
}

schema::SchemaVersion PlannedAppendOnlyCompaction::schema_version() const noexcept {
  return schema_version_;
}

std::span<const cseg::PartId> PlannedAppendOnlyCompaction::input_part_ids() const noexcept {
  return input_part_ids_;
}

std::uint64_t PlannedAppendOnlyCompaction::input_bytes() const noexcept {
  return input_bytes_;
}

std::uint64_t PlannedAppendOnlyCompaction::input_rows() const noexcept {
  return input_rows_;
}

std::int64_t PlannedAppendOnlyCompaction::minimum_event_time() const noexcept {
  return minimum_event_time_;
}

std::int64_t PlannedAppendOnlyCompaction::maximum_event_time() const noexcept {
  return maximum_event_time_;
}

std::uint32_t PlannedAppendOnlyCompaction::delta_input_parts() const noexcept {
  return delta_input_parts_;
}

common::Result<std::vector<AppendOnlyPartClassification>>
classify_append_only_parts(const std::span<const PartDescriptor> parts,
                           const std::uint32_t maximum_manifest_parts) {
  common::Result<std::vector<AppendOnlyPartRole>> roles =
      classify_roles(parts, maximum_manifest_parts);
  if (!roles.has_value()) {
    return common::make_unexpected(roles.error());
  }
  try {
    std::vector<AppendOnlyPartClassification> classifications;
    classifications.reserve(parts.size());
    for (std::size_t index = 0U; index < parts.size(); ++index) {
      classifications.push_back({.part_id = parts[index].part_id, .role = (*roles)[index]});
    }
    return classifications;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Cannot allocate append-only classifications"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Append-only classifications exceed container limits"));
  }
}

common::Result<std::optional<PlannedAppendOnlyCompaction>>
plan_append_only_compaction(const std::span<const PartDescriptor> parts,
                            const AppendOnlyCompactionPlannerLimits limits) {
  if (limits.minimum_input_parts < 2U || limits.maximum_input_parts < limits.minimum_input_parts ||
      limits.maximum_manifest_parts < limits.maximum_input_parts ||
      limits.maximum_input_bytes == 0U || limits.maximum_input_rows == 0U) {
    return common::make_unexpected(invalid("Append-only compaction planner limits are invalid"));
  }
  common::Result<std::vector<AppendOnlyPartRole>> roles =
      classify_roles(parts, limits.maximum_manifest_parts);
  if (!roles.has_value()) {
    return common::make_unexpected(roles.error());
  }
  if (parts.size() < limits.minimum_input_parts) {
    return std::optional<PlannedAppendOnlyCompaction>{};
  }

  try {
    std::vector<std::size_t> grouped(parts.size());
    std::iota(grouped.begin(), grouped.end(), std::size_t{0U});
    std::ranges::sort(grouped, [&](const std::size_t left, const std::size_t right) {
      const PartDescriptor& left_part = parts[left];
      const PartDescriptor& right_part = parts[right];
      return std::tuple{group_key(left_part), left_part.part_id} <
             std::tuple{group_key(right_part), right_part.part_id};
    });

    std::size_t group_begin = 0U;
    while (group_begin < grouped.size()) {
      std::size_t group_end = group_begin + 1U;
      while (group_end < grouped.size() &&
             same_group(parts[grouped[group_begin]], parts[grouped[group_end]])) {
        ++group_end;
      }
      std::vector<std::size_t> arrival{grouped.begin() + static_cast<std::ptrdiff_t>(group_begin),
                                       grouped.begin() + static_cast<std::ptrdiff_t>(group_end)};
      std::ranges::sort(arrival, [&](const std::size_t left, const std::size_t right) {
        return std::tie(parts[left].maximum_record_sequence, parts[left].part_id) <
               std::tie(parts[right].maximum_record_sequence, parts[right].part_id);
      });
      for (const std::size_t seed : arrival) {
        if ((*roles)[seed] != AppendOnlyPartRole::kDelta) {
          continue;
        }
        std::vector<std::size_t> candidate{seed};
        for (const std::size_t neighbor : arrival) {
          if (neighbor != seed && ranges_overlap(parts[seed], parts[neighbor])) {
            candidate.push_back(neighbor);
          }
        }
        common::Result<std::optional<PlannedAppendOnlyCompaction>> plan =
            make_plan(parts, *roles, candidate, limits, true);
        if (!plan.has_value()) {
          return common::make_unexpected(plan.error());
        }
        if (plan->has_value()) {
          return plan;
        }
      }

      std::vector<std::size_t> by_range = arrival;
      std::ranges::sort(by_range, [&](const std::size_t left, const std::size_t right) {
        return std::tie(parts[left].minimum_event_time, parts[left].maximum_event_time,
                        parts[left].part_id) < std::tie(parts[right].minimum_event_time,
                                                        parts[right].maximum_event_time,
                                                        parts[right].part_id);
      });
      std::size_t component_begin = 0U;
      while (component_begin < by_range.size()) {
        std::size_t component_end = component_begin + 1U;
        std::int64_t frontier = parts[by_range[component_begin]].maximum_event_time;
        while (component_end < by_range.size() &&
               parts[by_range[component_end]].minimum_event_time <= frontier) {
          frontier = std::max(frontier, parts[by_range[component_end]].maximum_event_time);
          ++component_end;
        }
        if (component_end - component_begin >= limits.minimum_input_parts) {
          const std::span component{by_range.data() + component_begin,
                                    component_end - component_begin};
          common::Result<std::optional<PlannedAppendOnlyCompaction>> plan =
              make_plan(parts, *roles, component, limits, false);
          if (!plan.has_value()) {
            return common::make_unexpected(plan.error());
          }
          if (plan->has_value()) {
            return plan;
          }
        }
        component_begin = component_end;
      }
      group_begin = group_end;
    }
    return std::optional<PlannedAppendOnlyCompaction>{};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Cannot allocate append-only compaction plan"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Append-only compaction plan exceeds container limits"));
  }
}

} // namespace chronos::manifest
