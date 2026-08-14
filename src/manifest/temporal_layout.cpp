#include "chronos/manifest/temporal_layout.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_format.hpp"

#include <optional>

namespace chronos::manifest {
namespace {

struct DescriptorTableSpan {
  std::uint64_t offset{};
  std::uint64_t count{};
  std::uint64_t descriptor_length{};
};

[[nodiscard]] common::Result<std::uint64_t> table_end(const DescriptorTableSpan table) {
  const std::optional<std::uint64_t> length =
      common::checked_multiply(table.count, table.descriptor_length);
  const std::optional<std::uint64_t> end =
      length.has_value() ? common::checked_add(table.offset, *length) : std::nullopt;
  return end.has_value()
             ? common::Result<std::uint64_t>{*end}
             : common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                      "Manifest v2 descriptor table overflowed"});
}

} // namespace

common::Result<ManifestLayout> plan_manifest_v2_temporal_layout(const ManifestLayoutInput input) {
  if (input.tablet_count > format::kMaximumDescriptorCount ||
      input.part_count > format::kMaximumDescriptorCount ||
      input.retry_count > format::kMaximumDescriptorCount) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "Manifest descriptor count is outside the v2 registry limit"});
  }
  const std::uint64_t tablets_offset = format::kTabletsOffset;
  const common::Result<std::uint64_t> parts_offset =
      table_end({.offset = tablets_offset,
                 .count = input.tablet_count,
                 .descriptor_length = temporal_format::kTabletDescriptorLength});
  if (!parts_offset.has_value()) {
    return common::make_unexpected(parts_offset.error());
  }
  const common::Result<std::uint64_t> retries_offset =
      table_end({.offset = *parts_offset,
                 .count = input.part_count,
                 .descriptor_length = temporal_format::kPartDescriptorLength});
  if (!retries_offset.has_value()) {
    return common::make_unexpected(retries_offset.error());
  }
  const common::Result<std::uint64_t> trailer_offset =
      table_end({.offset = *retries_offset,
                 .count = input.retry_count,
                 .descriptor_length = temporal_format::kRetryDescriptorLength});
  if (!trailer_offset.has_value()) {
    return common::make_unexpected(trailer_offset.error());
  }
  const std::optional<std::uint64_t> total_length =
      common::checked_add(*trailer_offset, static_cast<std::uint64_t>(format::kTrailerLength));
  if (!total_length.has_value() || *total_length > format::kMaximumFileLength) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Manifest v2 layout exceeds the file limit"});
  }
  if ((*total_length % format::kAlignment) != 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "Manifest v2 layout is not aligned"});
  }
  return ManifestLayout{tablets_offset, *parts_offset, *retries_offset, *trailer_offset,
                        *total_length};
}

} // namespace chronos::manifest
