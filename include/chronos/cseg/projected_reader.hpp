#ifndef CHRONOS_CSEG_PROJECTED_READER_HPP_
#define CHRONOS_CSEG_PROJECTED_READER_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <vector>

namespace chronos::cseg {

class CsegProjectedReaderView;

struct CsegProjectedReaderLimits {
  CsegMetadataDecodeLimits metadata;
  std::uint64_t max_decoded_buffer_bytes{4U * format::kMaximumUncompressedPageLength};
  std::uint32_t max_projected_columns{format::kMaximumUserColumnCount};
};

enum class CsegProjectedReaderOpenErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
  kInvalidArgument,
  kNotFound,
};

class CsegProjectedReaderOpenError {
public:
  CsegProjectedReaderOpenError(CsegProjectedReaderOpenErrorKind kind, common::Status status,
                               std::uint64_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr CsegProjectedReaderOpenErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::uint64_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  CsegProjectedReaderOpenErrorKind kind_;
  common::Status status_;
  std::uint64_t required_size_;
};

// Allocation-free validated plan for one projected granule read. The plan borrows both its reader
// and the caller's ordinal span; neither may be moved, destroyed, or mutated before execution.
// Planning authenticates no page body and allocates no result storage.
class CsegProjectedGranuleReadPlan {
public:
  CsegProjectedGranuleReadPlan() = delete;

  [[nodiscard]] constexpr std::size_t granule_ordinal() const noexcept {
    return granule_ordinal_;
  }
  [[nodiscard]] constexpr std::uint64_t first_row() const noexcept {
    return first_row_;
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] constexpr std::span<const std::uint32_t>
  destination_column_ordinals() const noexcept {
    return destination_column_ordinals_;
  }
  [[nodiscard]] constexpr std::size_t source_user_page_count() const noexcept {
    return source_user_page_count_;
  }
  [[nodiscard]] constexpr std::size_t synthesized_column_count() const noexcept {
    return synthesized_column_count_;
  }
  [[nodiscard]] constexpr std::size_t decoded_page_count() const noexcept {
    return source_user_page_count_ + format::kSystemColumnCount;
  }
  // Exact aggregate canonical bytes for selected source pages, synthesized columns, and all system
  // pages. Raw page bytes are borrowed; compressed page outputs and synthesized columns are owned.
  [[nodiscard]] constexpr std::uint64_t decoded_buffer_bytes() const noexcept {
    return decoded_buffer_bytes_;
  }
  [[nodiscard]] constexpr std::uint64_t owned_buffer_bytes() const noexcept {
    return owned_buffer_bytes_;
  }
  [[nodiscard]] constexpr std::uint64_t borrowed_buffer_bytes() const noexcept {
    return borrowed_buffer_bytes_;
  }

private:
  struct Accounting {
    std::size_t source_user_page_count;
    std::size_t synthesized_column_count;
    std::uint64_t decoded_buffer_bytes;
    std::uint64_t owned_buffer_bytes;
    std::uint64_t borrowed_buffer_bytes;
  };

  CsegProjectedGranuleReadPlan(const CsegProjectedReaderView* reader, std::size_t granule_ordinal,
                               const CsegGranuleDescriptor& descriptor,
                               std::span<const std::uint32_t> destination_column_ordinals,
                               Accounting accounting) noexcept;

  const CsegProjectedReaderView* reader_;
  std::size_t granule_ordinal_;
  std::uint64_t first_row_;
  std::uint32_t row_count_;
  std::span<const std::uint32_t> destination_column_ordinals_;
  std::size_t source_user_page_count_;
  std::size_t synthesized_column_count_;
  std::uint64_t decoded_buffer_bytes_;
  std::uint64_t owned_buffer_bytes_;
  std::uint64_t borrowed_buffer_bytes_;

  friend class CsegProjectedReaderView;
};

// One destination-schema user column in a projected granule. The physical buffers borrow either
// the encoded part (raw source page) or storage owned by the containing ProjectedCsegGranule
// (decompressed source page or synthesized nullable tail).
class CsegProjectedColumnView {
public:
  CsegProjectedColumnView() = delete;

  [[nodiscard]] constexpr const schema::ColumnId& column_id() const noexcept {
    return column_id_;
  }
  [[nodiscard]] constexpr const columnar::PhysicalColumnView& physical() const noexcept {
    return physical_;
  }

private:
  CsegProjectedColumnView(schema::ColumnId column_id,
                          columnar::PhysicalColumnView physical) noexcept;

  schema::ColumnId column_id_;
  columnar::PhysicalColumnView physical_;

  friend class CsegProjectedReaderView;
};

// Owns decompressed and synthesized buffers for one projected granule. Raw page views continue to
// borrow the immutable encoded bytes supplied when the reader was opened, so that byte owner must
// outlive this object. The pinned destination schema remains valid independently of the lineage.
class ProjectedCsegGranule {
public:
  ProjectedCsegGranule() = delete;
  ProjectedCsegGranule(const ProjectedCsegGranule&) = delete;
  ProjectedCsegGranule& operator=(const ProjectedCsegGranule&) = delete;
  ProjectedCsegGranule(ProjectedCsegGranule&&) noexcept = default;
  ProjectedCsegGranule& operator=(ProjectedCsegGranule&&) noexcept = default;

  [[nodiscard]] constexpr std::size_t granule_ordinal() const noexcept {
    return granule_ordinal_;
  }
  [[nodiscard]] constexpr std::uint64_t first_row() const noexcept {
    return first_row_;
  }
  [[nodiscard]] constexpr std::uint32_t row_count() const noexcept {
    return row_count_;
  }
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept {
    return schema_;
  }
  [[nodiscard]] std::span<const CsegProjectedColumnView> columns() const noexcept {
    return columns_;
  }
  [[nodiscard]] const CsegProjectedColumnView* column(std::size_t ordinal) const noexcept;
  [[nodiscard]] const columnar::PhysicalColumnView& wal_id() const noexcept;
  [[nodiscard]] const columnar::PhysicalColumnView& record_sequence() const noexcept;
  [[nodiscard]] const columnar::PhysicalColumnView& row_ordinal() const noexcept;
  [[nodiscard]] const columnar::PhysicalColumnView& operation() const noexcept;
  // Exact logical bytes across requested/synthesized user columns and the four mandatory system
  // pages. Retained bytes count owned page/synthesis capacities and result-container capacities;
  // raw page bytes remain the responsibility of the encoded-part owner.
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  ProjectedCsegGranule(std::size_t granule_ordinal, const CsegGranuleDescriptor& descriptor,
                       std::shared_ptr<const schema::TableSchema> schema,
                       std::vector<DecodedCsegPage> decoded_pages,
                       std::vector<columnar::OwnedColumnVector> synthesized_columns,
                       std::vector<CsegProjectedColumnView> columns,
                       std::size_t system_page_start) noexcept;

  std::size_t granule_ordinal_;
  std::uint64_t first_row_;
  std::uint32_t row_count_;
  std::shared_ptr<const schema::TableSchema> schema_;
  std::vector<DecodedCsegPage> decoded_pages_;
  std::vector<columnar::OwnedColumnVector> synthesized_columns_;
  std::vector<CsegProjectedColumnView> columns_;
  std::size_t system_page_start_;

  friend class CsegProjectedReaderView;
};

// A metadata-authenticated, schema-bound projected reader over one complete immutable in-memory
// part image. Opening does not touch page bytes. Each read validates only requested user pages and
// all four mandatory system pages, including their following alignment and v1 system semantics.
// It is therefore a scan primitive, not evidence of complete-part acceptance.
class CsegProjectedReaderView {
public:
  CsegProjectedReaderView() = delete;
  CsegProjectedReaderView(const CsegProjectedReaderView&) = delete;
  CsegProjectedReaderView& operator=(const CsegProjectedReaderView&) = delete;
  CsegProjectedReaderView(CsegProjectedReaderView&&) noexcept = default;
  CsegProjectedReaderView& operator=(CsegProjectedReaderView&&) noexcept = default;

  [[nodiscard]] constexpr const DecodedCsegMetadataView& metadata() const noexcept {
    return metadata_;
  }
  [[nodiscard]] constexpr common::ByteView encoded_part() const noexcept {
    return encoded_part_;
  }
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>&
  source_schema_ptr() const noexcept {
    return source_schema_;
  }
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>&
  destination_schema_ptr() const noexcept {
    return destination_schema_;
  }

  // Validates a complete projection request and computes its exact aggregate decoded-buffer
  // requirement without allocating or touching page bodies. The returned plan borrows the ordinal
  // span and this unmoved reader.
  [[nodiscard]] common::Result<CsegProjectedGranuleReadPlan>
  plan_granule(std::size_t granule_ordinal,
               std::span<const std::uint32_t> destination_column_ordinals) const;

  // Destination ordinals are returned in caller order and must be unique. An empty projection is
  // valid and still reads and validates the system columns. Added nullable tail columns are
  // synthesized as canonical all-null vectors without touching any unrelated source page.
  [[nodiscard]] common::Result<ProjectedCsegGranule>
  read_granule(std::size_t granule_ordinal,
               std::span<const std::uint32_t> destination_column_ordinals) const;
  // Executes a plan produced by this unmoved reader. The borrowed ordinal request is revalidated
  // before allocation, and allocation failures are reported as RESOURCE_EXHAUSTED.
  [[nodiscard]] common::Result<ProjectedCsegGranule>
  read_granule(const CsegProjectedGranuleReadPlan& plan) const;

private:
  CsegProjectedReaderView(DecodedCsegMetadataView metadata, common::ByteView encoded_part,
                          std::shared_ptr<const schema::TableSchema> source_schema,
                          std::shared_ptr<const schema::TableSchema> destination_schema,
                          schema::SchemaProjection projection,
                          CsegProjectedReaderLimits limits) noexcept;
  [[nodiscard]] common::Result<ProjectedCsegGranule>
  execute_granule_plan(const CsegProjectedGranuleReadPlan& plan) const;

  DecodedCsegMetadataView metadata_;
  common::ByteView encoded_part_;
  std::shared_ptr<const schema::TableSchema> source_schema_;
  std::shared_ptr<const schema::TableSchema> destination_schema_;
  schema::SchemaProjection projection_;
  CsegProjectedReaderLimits limits_;

  friend std::expected<CsegProjectedReaderView, CsegProjectedReaderOpenError>
  open_cseg_v1_projected_reader_prefix(common::ByteView, const schema::SchemaLineage&,
                                       schema::SchemaId, const schema::TabletId&,
                                       CsegProjectedReaderLimits);
};

using CsegProjectedReaderOpenResult =
    std::expected<CsegProjectedReaderView, CsegProjectedReaderOpenError>;

// Opens the first complete CSEG part and leaves following bytes to the caller. Metadata integrity,
// exact source-schema/tablet binding, and ancestor-to-destination lineage are established before
// success; page bytes remain untouched until read_granule().
[[nodiscard]] CsegProjectedReaderOpenResult
open_cseg_v1_projected_reader_prefix(common::ByteView bytes, const schema::SchemaLineage& lineage,
                                     schema::SchemaId destination_schema_id,
                                     const schema::TabletId& target_tablet,
                                     CsegProjectedReaderLimits limits = {});

// Requires exactly one complete part image and rejects trailing bytes.
[[nodiscard]] CsegProjectedReaderOpenResult
open_cseg_v1_projected_reader_exact(common::ByteView bytes, const schema::SchemaLineage& lineage,
                                    schema::SchemaId destination_schema_id,
                                    const schema::TabletId& target_tablet,
                                    CsegProjectedReaderLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_PROJECTED_READER_HPP_
