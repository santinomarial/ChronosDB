#ifndef CHRONOS_MANIFEST_CODEC_HPP_
#define CHRONOS_MANIFEST_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/types.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

namespace chronos::manifest {

struct ManifestEncodeInput {
  std::uint64_t generation{};
  DatabaseId database_id;
  wal::WalId wal_id;
  WalCheckpoint reclaim_checkpoint;
  std::span<const TabletDescriptor> tablets;
  std::span<const PartDescriptor> parts;
  std::span<const RetryDescriptor> retries;
};

// Owns exactly one complete immutable Manifest v1 generation. No filename or filesystem framing is
// included, and bytes() contains no trailing storage.
class EncodedManifest {
public:
  EncodedManifest() = delete;
  EncodedManifest(const EncodedManifest&) = delete;
  EncodedManifest& operator=(const EncodedManifest&) = delete;
  EncodedManifest(EncodedManifest&&) noexcept = default;
  EncodedManifest& operator=(EncodedManifest&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedManifest(std::vector<std::byte> bytes) noexcept;

  std::vector<std::byte> bytes_;

  friend common::Result<EncodedManifest> encode_manifest_v1(const ManifestEncodeInput& input);
};

[[nodiscard]] common::Result<EncodedManifest> encode_manifest_v1(const ManifestEncodeInput& input);

struct ManifestDecodeLimits {
  std::uint64_t max_file_length{format::kMaximumFileLength};
  std::uint64_t max_tablets{format::kMaximumDescriptorCount};
  std::uint64_t max_parts{format::kMaximumDescriptorCount};
  std::uint64_t max_retries{format::kMaximumDescriptorCount};
};

enum class ManifestDecodeErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
};

class ManifestDecodeError {
public:
  ManifestDecodeError(ManifestDecodeErrorKind kind, common::Status status,
                      std::uint64_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr ManifestDecodeErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::uint64_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  ManifestDecodeErrorKind kind_;
  common::Status status_;
  std::uint64_t required_size_;
};

// Borrows one complete immutable encoded generation. The caller keeps encoded_bytes() alive and
// immutable. Parsed descriptor arrays are owned by the view.
class DecodedManifestView {
public:
  DecodedManifestView() = delete;

  [[nodiscard]] constexpr std::uint64_t generation() const noexcept {
    return generation_;
  }
  [[nodiscard]] constexpr std::uint64_t previous_generation() const noexcept {
    return previous_generation_;
  }
  [[nodiscard]] constexpr const DatabaseId& database_id() const noexcept {
    return database_id_;
  }
  [[nodiscard]] constexpr const wal::WalId& wal_id() const noexcept {
    return wal_id_;
  }
  [[nodiscard]] constexpr const WalCheckpoint& reclaim_checkpoint() const noexcept {
    return reclaim_checkpoint_;
  }
  [[nodiscard]] std::span<const TabletDescriptor> tablets() const noexcept;
  [[nodiscard]] std::span<const PartDescriptor> parts() const noexcept;
  [[nodiscard]] std::span<const RetryDescriptor> retries() const noexcept;
  [[nodiscard]] common::ByteView encoded_bytes() const noexcept;
  // Owned descriptor-vector capacities only. The borrowed encoded bytes remain the caller's
  // accounting responsibility.
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  DecodedManifestView(std::uint64_t generation, std::uint64_t previous_generation,
                      DatabaseId database_id, wal::WalId wal_id, WalCheckpoint reclaim_checkpoint,
                      std::vector<TabletDescriptor> tablets, std::vector<PartDescriptor> parts,
                      std::vector<RetryDescriptor> retries,
                      common::ByteView encoded_bytes) noexcept;

  std::uint64_t generation_;
  std::uint64_t previous_generation_;
  DatabaseId database_id_;
  wal::WalId wal_id_;
  WalCheckpoint reclaim_checkpoint_;
  std::vector<TabletDescriptor> tablets_;
  std::vector<PartDescriptor> parts_;
  std::vector<RetryDescriptor> retries_;
  common::ByteView encoded_bytes_;

  friend std::expected<DecodedManifestView, ManifestDecodeError>
  decode_manifest_v1_prefix(common::ByteView bytes, ManifestDecodeLimits limits);
};

using ManifestDecodeResult = std::expected<DecodedManifestView, ManifestDecodeError>;

// Decodes the first canonical generation and ignores following bytes. Before the header CRC is
// validated, an incomplete input requires at most the 256-byte header; afterward required_size is
// the exact complete generation length.
[[nodiscard]] ManifestDecodeResult decode_manifest_v1_prefix(common::ByteView bytes,
                                                             ManifestDecodeLimits limits = {});

// Requires exactly one canonical generation and rejects trailing bytes.
[[nodiscard]] ManifestDecodeResult decode_manifest_v1_exact(common::ByteView bytes,
                                                            ManifestDecodeLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_CODEC_HPP_
