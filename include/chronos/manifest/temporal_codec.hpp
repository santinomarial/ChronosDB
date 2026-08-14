#ifndef CHRONOS_MANIFEST_TEMPORAL_CODEC_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/temporal_format.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <vector>

namespace chronos::manifest {

using ManifestCommitSource = cseg::temporal_format::CommitSource;

struct TemporalWalReclaimCheckpoint {
  wal::WalId wal_id;
  WalCheckpoint coordinate;
  friend bool operator==(const TemporalWalReclaimCheckpoint&,
                         const TemporalWalReclaimCheckpoint&) = default;
};

struct TemporalTabletDescriptor {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId recovery_schema_id;
  schema::SchemaVersion recovery_schema_version;
  common::Uuid source_id;
  std::uint64_t durable_position{};
  std::uint64_t reclaim_position{};
  std::uint64_t first_part_index{};
  std::uint64_t part_count{};
  std::uint64_t durable_version_count{};
  ManifestCommitSource commit_source{ManifestCommitSource::kWal};
  friend bool operator==(const TemporalTabletDescriptor&,
                         const TemporalTabletDescriptor&) = default;
};

struct TemporalPartDescriptor {
  cseg::PartId part_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t file_length{};
  std::uint64_t row_count{};
  std::uint64_t minimum_commit_position{};
  std::uint64_t maximum_commit_position{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
  std::int64_t minimum_system_time{};
  std::int64_t maximum_system_time{};
  common::Uuid source_id;
  ingest::Sha256Digest content_sha256;
  std::uint16_t cseg_format_major{cseg::temporal_format::kFormatMajor};
  std::uint16_t cseg_format_minor{cseg::temporal_format::kFormatMinor};
  ManifestCommitSource commit_source{ManifestCommitSource::kWal};
  friend bool operator==(const TemporalPartDescriptor&, const TemporalPartDescriptor&) = default;
};

struct TemporalRetryDescriptor {
  ingest::ClientId client_id;
  ingest::ClientBatchId client_batch_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  ingest::Sha256Digest request_digest;
  common::Uuid source_id;
  std::uint64_t commit_position{};
  std::uint32_t applied_row_count{};
  ManifestCommitSource commit_source{ManifestCommitSource::kWal};
  friend bool operator==(const TemporalRetryDescriptor&, const TemporalRetryDescriptor&) = default;
};

struct TemporalManifestEncodeInput {
  std::uint64_t generation{};
  DatabaseId database_id;
  std::optional<TemporalWalReclaimCheckpoint> wal_reclaim_checkpoint;
  std::span<const TemporalTabletDescriptor> tablets;
  std::span<const TemporalPartDescriptor> parts;
  std::span<const TemporalRetryDescriptor> retries;
};

class EncodedTemporalManifest {
public:
  EncodedTemporalManifest() = delete;
  EncodedTemporalManifest(const EncodedTemporalManifest&) = delete;
  EncodedTemporalManifest& operator=(const EncodedTemporalManifest&) = delete;
  EncodedTemporalManifest(EncodedTemporalManifest&&) noexcept = default;
  EncodedTemporalManifest& operator=(EncodedTemporalManifest&&) noexcept = default;
  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;

private:
  explicit EncodedTemporalManifest(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;
  friend common::Result<EncodedTemporalManifest>
  encode_manifest_v2_temporal(const TemporalManifestEncodeInput& input);
};

class DecodedTemporalManifestView {
public:
  DecodedTemporalManifestView() = delete;
  [[nodiscard]] constexpr std::uint64_t generation() const noexcept {
    return generation_;
  }
  [[nodiscard]] constexpr std::uint64_t previous_generation() const noexcept {
    return previous_generation_;
  }
  [[nodiscard]] constexpr const DatabaseId& database_id() const noexcept {
    return database_id_;
  }
  [[nodiscard]] constexpr const std::optional<TemporalWalReclaimCheckpoint>&
  wal_reclaim_checkpoint() const noexcept {
    return wal_reclaim_checkpoint_;
  }
  [[nodiscard]] std::span<const TemporalTabletDescriptor> tablets() const noexcept;
  [[nodiscard]] std::span<const TemporalPartDescriptor> parts() const noexcept;
  [[nodiscard]] std::span<const TemporalRetryDescriptor> retries() const noexcept;
  [[nodiscard]] common::ByteView encoded_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  struct GenerationLineage {
    std::uint64_t generation{};
    std::uint64_t previous_generation{};
  };

  DecodedTemporalManifestView(GenerationLineage lineage, DatabaseId database_id,
                              std::optional<TemporalWalReclaimCheckpoint> wal_reclaim_checkpoint,
                              std::vector<TemporalTabletDescriptor> tablets,
                              std::vector<TemporalPartDescriptor> parts,
                              std::vector<TemporalRetryDescriptor> retries,
                              common::ByteView encoded_bytes) noexcept;
  std::uint64_t generation_;
  std::uint64_t previous_generation_;
  DatabaseId database_id_;
  std::optional<TemporalWalReclaimCheckpoint> wal_reclaim_checkpoint_;
  std::vector<TemporalTabletDescriptor> tablets_;
  std::vector<TemporalPartDescriptor> parts_;
  std::vector<TemporalRetryDescriptor> retries_;
  common::ByteView encoded_bytes_;
  friend std::expected<DecodedTemporalManifestView, ManifestDecodeError>
  decode_manifest_v2_temporal_prefix(common::ByteView bytes, ManifestDecodeLimits limits);
};

using TemporalManifestDecodeResult =
    std::expected<DecodedTemporalManifestView, ManifestDecodeError>;

[[nodiscard]] common::Result<EncodedTemporalManifest>
encode_manifest_v2_temporal(const TemporalManifestEncodeInput& input);
[[nodiscard]] TemporalManifestDecodeResult
decode_manifest_v2_temporal_prefix(common::ByteView bytes, ManifestDecodeLimits limits = {});
[[nodiscard]] TemporalManifestDecodeResult
decode_manifest_v2_temporal_exact(common::ByteView bytes, ManifestDecodeLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_CODEC_HPP_
