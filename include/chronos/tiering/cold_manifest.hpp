#ifndef CHRONOS_TIERING_COLD_MANIFEST_HPP_
#define CHRONOS_TIERING_COLD_MANIFEST_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/tiering/cold_manifest_format.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <vector>

namespace chronos::tiering {

struct ColdPartLocationDescriptor {
  cseg::PartId part_id;
  std::uint64_t file_length{};
  ingest::Sha256Digest content_sha256;
  std::string object_key;

  friend bool operator==(const ColdPartLocationDescriptor&,
                         const ColdPartLocationDescriptor&) = default;
};

struct ColdLocationManifestEncodeInput {
  std::uint64_t generation{};
  std::uint64_t base_manifest_generation{};
  manifest::DatabaseId database_id;
  common::Uuid object_store_id;
  std::span<const ColdPartLocationDescriptor> locations;
};

struct ColdLocationManifestDecodeLimits {
  std::uint64_t maximum_file_length{cold_manifest_format::kMaximumFileLength};
  std::uint64_t maximum_locations{cold_manifest_format::kMaximumLocationCount};
  std::uint64_t maximum_key_bytes{cold_manifest_format::kMaximumKeyBytes};
};

enum class ColdLocationManifestDecodeErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
};

class ColdLocationManifestDecodeError {
public:
  ColdLocationManifestDecodeError(ColdLocationManifestDecodeErrorKind kind, common::Status status,
                                  std::uint64_t required_size = 0U) noexcept;

  [[nodiscard]] ColdLocationManifestDecodeErrorKind kind() const noexcept;
  [[nodiscard]] const common::Status& status() const noexcept;
  [[nodiscard]] std::uint64_t required_size() const noexcept;

private:
  ColdLocationManifestDecodeErrorKind kind_;
  common::Status status_;
  std::uint64_t required_size_{};
};

class EncodedColdLocationManifest {
public:
  EncodedColdLocationManifest() = delete;
  EncodedColdLocationManifest(const EncodedColdLocationManifest&) = delete;
  EncodedColdLocationManifest& operator=(const EncodedColdLocationManifest&) = delete;
  EncodedColdLocationManifest(EncodedColdLocationManifest&&) noexcept = default;
  EncodedColdLocationManifest& operator=(EncodedColdLocationManifest&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedColdLocationManifest(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedColdLocationManifest>
  encode_cold_location_manifest_v1(const ColdLocationManifestEncodeInput& input);
};

class DecodedColdLocationManifest {
public:
  DecodedColdLocationManifest() = delete;

  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] std::uint64_t previous_generation() const noexcept;
  [[nodiscard]] std::uint64_t base_manifest_generation() const noexcept;
  [[nodiscard]] const manifest::DatabaseId& database_id() const noexcept;
  [[nodiscard]] const common::Uuid& object_store_id() const noexcept;
  [[nodiscard]] std::span<const ColdPartLocationDescriptor> locations() const noexcept;
  [[nodiscard]] std::size_t encoded_size() const noexcept;

private:
  struct GenerationLineage {
    std::uint64_t generation{};
    std::uint64_t previous_generation{};
    std::uint64_t base_manifest_generation{};
  };

  DecodedColdLocationManifest(GenerationLineage lineage, manifest::DatabaseId database_id,
                              common::Uuid object_store_id,
                              std::vector<ColdPartLocationDescriptor> locations,
                              std::size_t encoded_size) noexcept;

  std::uint64_t generation_{};
  std::uint64_t previous_generation_{};
  std::uint64_t base_manifest_generation_{};
  manifest::DatabaseId database_id_;
  common::Uuid object_store_id_;
  std::vector<ColdPartLocationDescriptor> locations_;
  std::size_t encoded_size_{};

  friend std::expected<DecodedColdLocationManifest, ColdLocationManifestDecodeError>
      decode_cold_location_manifest_v1_prefix(common::ByteView, ColdLocationManifestDecodeLimits);
};

using ColdLocationManifestDecodeResult =
    std::expected<DecodedColdLocationManifest, ColdLocationManifestDecodeError>;

[[nodiscard]] common::Result<EncodedColdLocationManifest>
encode_cold_location_manifest_v1(const ColdLocationManifestEncodeInput& input);
[[nodiscard]] ColdLocationManifestDecodeResult
decode_cold_location_manifest_v1_prefix(common::ByteView bytes,
                                        ColdLocationManifestDecodeLimits limits = {});
[[nodiscard]] ColdLocationManifestDecodeResult
decode_cold_location_manifest_v1_exact(common::ByteView bytes,
                                       ColdLocationManifestDecodeLimits limits = {});

// Validates that the cold-location authority names exactly the immutable bytes in one pinned
// Manifest v2 database generation. It performs no I/O and accepts no listing-derived state.
[[nodiscard]] common::Status
validate_cold_location_manifest_binding(const DecodedColdLocationManifest& cold,
                                        const manifest::DecodedTemporalManifestView& base_manifest);

// A successor advances exactly one cold generation and never moves its Manifest v2 binding
// backward. At the same base generation it is add-only. After a base-generation advance, a
// predecessor route may disappear only when that part is absent from the supplied successor
// Manifest; every still-logical route remains byte-for-byte immutable.
[[nodiscard]] common::Status validate_cold_location_manifest_transition(
    const DecodedColdLocationManifest& predecessor, const DecodedColdLocationManifest& successor,
    const manifest::DecodedTemporalManifestView& successor_base_manifest);

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_COLD_MANIFEST_HPP_
