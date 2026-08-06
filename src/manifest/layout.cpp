#include "chronos/manifest/layout.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/manifest/format.hpp"

#include <optional>
#include <string>
#include <string_view>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

// Offset, count, and descriptor length are the conventional table-layout inputs.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<std::uint64_t> table_end(const std::uint64_t offset,
                                                      const std::uint64_t count,
                                                      const std::uint64_t descriptor_length,
                                                      const std::string_view label) {
  const std::optional<std::uint64_t> length = common::checked_multiply(count, descriptor_length);
  const std::optional<std::uint64_t> end =
      length.has_value() ? common::checked_add(offset, *length) : std::nullopt;
  if (!end.has_value()) {
    return common::make_unexpected(exhausted(label));
  }
  return *end;
}

} // namespace

common::Result<ManifestLayout> plan_manifest_v1_layout(const ManifestLayoutInput input) {
  if (input.tablet_count > format::kMaximumDescriptorCount ||
      input.part_count > format::kMaximumDescriptorCount ||
      input.retry_count > format::kMaximumDescriptorCount) {
    return common::make_unexpected(
        invalid("Manifest descriptor count is outside the v1 registry limit"));
  }

  const std::uint64_t tablets_offset = format::kTabletsOffset;
  const common::Result<std::uint64_t> parts_offset =
      table_end(tablets_offset, input.tablet_count, format::kTabletDescriptorLength,
                "Manifest tablet descriptor table overflowed");
  if (!parts_offset.has_value()) {
    return common::make_unexpected(parts_offset.error());
  }
  const common::Result<std::uint64_t> retries_offset =
      table_end(*parts_offset, input.part_count, format::kPartDescriptorLength,
                "Manifest part descriptor table overflowed");
  if (!retries_offset.has_value()) {
    return common::make_unexpected(retries_offset.error());
  }
  const common::Result<std::uint64_t> trailer_offset =
      table_end(*retries_offset, input.retry_count, format::kRetryDescriptorLength,
                "Manifest retry descriptor table overflowed");
  if (!trailer_offset.has_value()) {
    return common::make_unexpected(trailer_offset.error());
  }
  const std::optional<std::uint64_t> total_length =
      common::checked_add(*trailer_offset, static_cast<std::uint64_t>(format::kTrailerLength));
  if (!total_length.has_value() || *total_length > format::kMaximumFileLength) {
    return common::make_unexpected(exhausted("Manifest layout exceeds the v1 file limit"));
  }
  if ((*total_length % format::kAlignment) != 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "Manifest layout is not aligned"});
  }

  return ManifestLayout{.tablets_offset = tablets_offset,
                        .parts_offset = *parts_offset,
                        .retries_offset = *retries_offset,
                        .trailer_offset = *trailer_offset,
                        .total_length = *total_length};
}

} // namespace chronos::manifest
