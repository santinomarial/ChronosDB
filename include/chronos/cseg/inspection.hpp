#ifndef CHRONOS_CSEG_INSPECTION_HPP_
#define CHRONOS_CSEG_INSPECTION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/validator.hpp"

#include <cstdint>
#include <expected>
#include <vector>

namespace chronos::cseg {

struct CsegInspectionLimits {
  CsegMetadataDecodeLimits decode;
  CsegValidationLimits validation;
};

enum class CsegInspectionErrorKind : std::uint8_t {
  kIncomplete,
  kCorruption,
  kUnsupported,
  kResourceLimit,
  kInvalidArgument,
};

class CsegInspectionError {
public:
  CsegInspectionError(CsegInspectionErrorKind kind, common::Status status,
                      std::uint64_t required_size = 0U) noexcept;

  [[nodiscard]] constexpr CsegInspectionErrorKind kind() const noexcept {
    return kind_;
  }
  [[nodiscard]] constexpr std::uint64_t required_size() const noexcept {
    return required_size_;
  }
  [[nodiscard]] const common::Status& status() const noexcept {
    return status_;
  }

private:
  CsegInspectionErrorKind kind_;
  common::Status status_;
  std::uint64_t required_size_;
};

// An owned, value-free description of one structurally and semantically valid CSEG v1 part.
// Descriptor vectors preserve their exact durable ordinal order. Inspection validates every page
// and complete schema-independent content semantics, but cannot perform catalog schema binding.
struct CsegInspectionReport {
  PartId part_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::uint64_t total_length{};
  std::uint64_t row_count{};
  std::uint32_t event_time_column_ordinal{};
  std::uint32_t ordering_column_count{};
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
  std::uint64_t stored_page_bytes{};
  std::uint64_t uncompressed_page_bytes{};
  std::uint64_t raw_page_count{};
  std::uint64_t zstd_page_count{};
  std::vector<CsegColumnDescriptor> columns;
  std::vector<CsegGranuleDescriptor> granules;
  std::vector<CsegPageDescriptor> pages;
};

using CsegInspectionResult = std::expected<CsegInspectionReport, CsegInspectionError>;

// Read-only, pure in-memory inspection. Exact decoding rejects a suffix; complete validation
// rejects bad system values, extrema, or row ordering. No row value bytes are retained or exposed.
[[nodiscard]] CsegInspectionResult inspect_cseg_v1_part(common::ByteView bytes,
                                                        CsegInspectionLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_INSPECTION_HPP_
