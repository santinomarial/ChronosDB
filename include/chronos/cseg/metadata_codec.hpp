#ifndef CHRONOS_CSEG_METADATA_CODEC_HPP_
#define CHRONOS_CSEG_METADATA_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cseg {

// The frozen column descriptor stores this registry in a 16-bit field.
// NOLINTNEXTLINE(performance-enum-size)
enum class StorageKind : std::uint16_t {
  kUser = format::kUserStorageKind,
  kWalId = format::kWalIdStorageKind,
  kRecordSequence = format::kRecordSequenceStorageKind,
  kRowOrdinal = format::kRowOrdinalStorageKind,
  kOperation = format::kOperationStorageKind,
  kCommitSource = temporal_format::kCommitSourceStorageKind,
  kSourceId = temporal_format::kSourceIdStorageKind,
  kCommitPosition = temporal_format::kCommitPositionStorageKind,
  kTemporalRowOrdinal = temporal_format::kRowOrdinalStorageKind,
  kTemporalOperation = temporal_format::kOperationStorageKind,
  kLogicalIdentity = temporal_format::kLogicalIdentityStorageKind,
  kReceiveTime = temporal_format::kReceiveTimeStorageKind,
  kSystemCommitTime = temporal_format::kSystemCommitTimeStorageKind,
};

struct CsegColumnDescriptor {
  std::optional<schema::ColumnId> column_id;
  StorageKind storage_kind;
  schema::LogicalType logical_type;
  bool nullable{};
  bool event_time{};
  std::optional<std::uint32_t> schema_ordinal;
  std::optional<std::uint32_t> ordering_ordinal;

  friend bool operator==(const CsegColumnDescriptor&, const CsegColumnDescriptor&) = default;
};

struct CsegGranuleDescriptor {
  std::uint64_t first_row{};
  std::uint32_t row_count{};
  std::uint64_t first_page_index{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};

  friend bool operator==(const CsegGranuleDescriptor&, const CsegGranuleDescriptor&) = default;
};

// Page offsets are derived by the encoder. The CRC covers the separately owned stored page bytes.
struct CsegPageMetadataInput {
  PageCompression compression;
  std::uint32_t row_count{};
  std::uint32_t null_count{};
  std::uint64_t stored_length{};
  std::uint64_t uncompressed_length{};
  std::uint64_t validity_length{};
  std::uint64_t offsets_length{};
  std::uint64_t values_length{};
  std::uint32_t page_crc32c{};

  friend bool operator==(const CsegPageMetadataInput&, const CsegPageMetadataInput&) = default;
};

struct CsegPageDescriptor {
  std::uint32_t granule_ordinal{};
  std::uint32_t stored_column_ordinal{};
  PageCompression compression;
  std::uint32_t row_count{};
  std::uint32_t null_count{};
  std::uint64_t page_offset{};
  std::uint64_t stored_length{};
  std::uint64_t uncompressed_length{};
  std::uint64_t validity_length{};
  std::uint64_t offsets_length{};
  std::uint64_t values_length{};
  std::uint32_t page_crc32c{};

  friend bool operator==(const CsegPageDescriptor&, const CsegPageDescriptor&) = default;
};

struct CsegMetadataEncodeInput {
  PartId part_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t row_count{};
  std::uint32_t event_time_column_ordinal{};
  std::uint32_t ordering_column_count{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
  std::span<const CsegColumnDescriptor> columns;
  std::span<const CsegGranuleDescriptor> granules;
  std::span<const CsegPageMetadataInput> pages;
};

// Owns exactly the canonical metadata prefix. total_length() includes the separately stored pages
// and their canonical alignment; bytes() does not.
class EncodedCsegMetadata {
public:
  EncodedCsegMetadata() = delete;
  EncodedCsegMetadata(const EncodedCsegMetadata&) = delete;
  EncodedCsegMetadata& operator=(const EncodedCsegMetadata&) = delete;
  EncodedCsegMetadata(EncodedCsegMetadata&&) noexcept = default;
  EncodedCsegMetadata& operator=(EncodedCsegMetadata&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] constexpr std::uint64_t total_length() const noexcept {
    return total_length_;
  }

private:
  EncodedCsegMetadata(std::vector<std::byte> bytes, std::uint64_t total_length) noexcept;

  std::vector<std::byte> bytes_;
  std::uint64_t total_length_;

  friend common::Result<EncodedCsegMetadata>
  encode_cseg_v1_metadata(const CsegMetadataEncodeInput& input);
  friend common::Result<EncodedCsegMetadata>
  encode_cseg_v2_temporal_metadata(const CsegMetadataEncodeInput& input);
  friend common::Result<EncodedCsegMetadata>
  encode_cseg_metadata(const CsegMetadataEncodeInput& input, std::uint16_t format_major);
};

[[nodiscard]] common::Result<EncodedCsegMetadata>
encode_cseg_v1_metadata(const CsegMetadataEncodeInput& input);
[[nodiscard]] common::Result<EncodedCsegMetadata>
encode_cseg_v2_temporal_metadata(const CsegMetadataEncodeInput& input);

struct CsegMetadataDecodeLimits {
  std::uint64_t max_file_length{format::kMaximumFileLength};
  std::uint64_t max_metadata_length{format::kMaximumFileLength};
  std::uint32_t max_user_columns{format::kMaximumUserColumnCount};
  std::uint32_t max_granules{format::kMaximumGranuleCount};
  std::uint32_t max_pages{std::numeric_limits<std::uint32_t>::max()};
};

enum class CsegMetadataDecodeErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
};

class CsegMetadataDecodeError {
public:
  CsegMetadataDecodeError(CsegMetadataDecodeErrorKind kind, common::Status status,
                          std::uint64_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr CsegMetadataDecodeErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::uint64_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  CsegMetadataDecodeErrorKind kind_;
  common::Status status_;
  std::uint64_t required_size_;
};

// Borrows the complete immutable metadata prefix. The caller keeps encoded_metadata() alive and
// immutable. Parsed descriptor vectors are owned by the view; no page bytes are touched.
class DecodedCsegMetadataView {
public:
  DecodedCsegMetadataView() = delete;

  [[nodiscard]] constexpr std::uint16_t format_major() const noexcept {
    return format_major_;
  }
  [[nodiscard]] constexpr std::uint16_t format_minor() const noexcept {
    return format_minor_;
  }

  [[nodiscard]] constexpr const PartId& part_id() const noexcept {
    return part_id_;
  }
  [[nodiscard]] constexpr const schema::TableId& table_id() const noexcept {
    return table_id_;
  }
  [[nodiscard]] constexpr const schema::TabletId& tablet_id() const noexcept {
    return tablet_id_;
  }
  [[nodiscard]] constexpr const schema::SchemaId& schema_id() const noexcept {
    return schema_id_;
  }
  [[nodiscard]] constexpr schema::SchemaVersion schema_version() const noexcept {
    return schema_version_;
  }
  [[nodiscard]] constexpr std::uint64_t total_length() const noexcept {
    return total_length_;
  }
  [[nodiscard]] constexpr std::uint64_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] constexpr std::uint32_t event_time_column_ordinal() const noexcept {
    return event_time_column_ordinal_;
  }
  [[nodiscard]] constexpr std::uint32_t ordering_column_count() const noexcept {
    return ordering_column_count_;
  }
  [[nodiscard]] constexpr std::int64_t minimum_event_time() const noexcept {
    return minimum_event_time_;
  }
  [[nodiscard]] constexpr std::int64_t maximum_event_time() const noexcept {
    return maximum_event_time_;
  }
  [[nodiscard]] std::span<const CsegColumnDescriptor> columns() const noexcept;
  [[nodiscard]] std::span<const CsegGranuleDescriptor> granules() const noexcept;
  [[nodiscard]] std::span<const CsegPageDescriptor> pages() const noexcept;
  [[nodiscard]] common::ByteView encoded_metadata() const noexcept;

private:
  DecodedCsegMetadataView(
      std::uint16_t format_major, std::uint16_t format_minor, PartId part_id,
      schema::TableId table_id, schema::TabletId tablet_id, schema::SchemaId schema_id,
      schema::SchemaVersion schema_version, std::uint64_t total_length, std::uint64_t row_count,
      std::uint32_t event_time_column_ordinal, std::uint32_t ordering_column_count,
      std::int64_t minimum_event_time, std::int64_t maximum_event_time,
      std::vector<CsegColumnDescriptor> columns, std::vector<CsegGranuleDescriptor> granules,
      std::vector<CsegPageDescriptor> pages, common::ByteView encoded_metadata) noexcept;

  std::uint16_t format_major_;
  std::uint16_t format_minor_;
  PartId part_id_;
  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  schema::SchemaId schema_id_;
  schema::SchemaVersion schema_version_;
  std::uint64_t total_length_;
  std::uint64_t row_count_;
  std::uint32_t event_time_column_ordinal_;
  std::uint32_t ordering_column_count_;
  std::int64_t minimum_event_time_;
  std::int64_t maximum_event_time_;
  std::vector<CsegColumnDescriptor> columns_;
  std::vector<CsegGranuleDescriptor> granules_;
  std::vector<CsegPageDescriptor> pages_;
  common::ByteView encoded_metadata_;

  friend std::expected<DecodedCsegMetadataView, CsegMetadataDecodeError>
  decode_cseg_v1_metadata_prefix(common::ByteView bytes, CsegMetadataDecodeLimits limits);
  friend std::expected<DecodedCsegMetadataView, CsegMetadataDecodeError>
  decode_cseg_v2_temporal_metadata_prefix(common::ByteView bytes, CsegMetadataDecodeLimits limits);
  friend std::expected<DecodedCsegMetadataView, CsegMetadataDecodeError>
  decode_cseg_metadata_prefix(common::ByteView bytes, CsegMetadataDecodeLimits limits,
                              std::uint16_t expected_major);
};

using CsegMetadataDecodeResult = std::expected<DecodedCsegMetadataView, CsegMetadataDecodeError>;

// Decodes the first metadata prefix and ignores page bytes or following objects. Before the header
// CRC is validated, incomplete input requires 256 bytes; afterward required_size is exact metadata.
[[nodiscard]] CsegMetadataDecodeResult
decode_cseg_v1_metadata_prefix(common::ByteView bytes, CsegMetadataDecodeLimits limits = {});

// Requires exactly the canonical metadata prefix and rejects page bytes or unrelated trailing data.
[[nodiscard]] CsegMetadataDecodeResult
decode_cseg_v1_metadata_exact(common::ByteView bytes, CsegMetadataDecodeLimits limits = {});
[[nodiscard]] CsegMetadataDecodeResult
decode_cseg_v2_temporal_metadata_prefix(common::ByteView bytes,
                                        CsegMetadataDecodeLimits limits = {});
[[nodiscard]] CsegMetadataDecodeResult
decode_cseg_v2_temporal_metadata_exact(common::ByteView bytes,
                                       CsegMetadataDecodeLimits limits = {});

// Catalog-dependent second stage. Names, routing, admission, and installation are not consulted.
[[nodiscard]] common::Status
validate_cseg_v1_metadata_schema(const DecodedCsegMetadataView& metadata,
                                 const schema::TableSchema& schema,
                                 const schema::TabletId& target_tablet);
[[nodiscard]] common::Status
validate_cseg_v2_temporal_metadata_schema(const DecodedCsegMetadataView& metadata,
                                          const schema::TableSchema& schema,
                                          const schema::TabletId& target_tablet);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_METADATA_CODEC_HPP_
