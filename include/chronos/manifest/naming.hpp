#ifndef CHRONOS_MANIFEST_NAMING_HPP_
#define CHRONOS_MANIFEST_NAMING_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/types.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace chronos::manifest {

struct TemporaryPartName {
  cseg::PartId part_id;
  common::Uuid nonce;

  friend bool operator==(const TemporaryPartName&, const TemporaryPartName&) = default;
};

struct TemporaryManifestName {
  std::uint64_t generation{};
  common::Uuid nonce;

  friend bool operator==(const TemporaryManifestName&, const TemporaryManifestName&) = default;
};

// These helpers operate on basenames only. Formatting is canonical and parsing accepts only the
// exact frozen grammar; uppercase hexadecimal, alternate widths, path components, and generation
// zero are rejected rather than normalized. A temporary nonce has no durable semantic meaning, so
// every 128-bit value, including zero, is representable.
[[nodiscard]] std::string part_file_name(const cseg::PartId& part_id);
[[nodiscard]] std::string temporary_part_file_name(const cseg::PartId& part_id,
                                                   const common::Uuid& nonce);
[[nodiscard]] common::Result<cseg::PartId> parse_part_file_name(std::string_view name);
[[nodiscard]] common::Result<TemporaryPartName>
parse_temporary_part_file_name(std::string_view name);

[[nodiscard]] common::Result<std::string> manifest_file_name(std::uint64_t generation);
[[nodiscard]] common::Result<std::string> temporary_manifest_file_name(std::uint64_t generation,
                                                                       const common::Uuid& nonce);
[[nodiscard]] common::Result<std::uint64_t> parse_manifest_file_name(std::string_view name);
[[nodiscard]] common::Result<TemporaryManifestName>
parse_temporary_manifest_file_name(std::string_view name);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_NAMING_HPP_
