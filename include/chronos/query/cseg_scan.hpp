#ifndef CHRONOS_QUERY_CSEG_SCAN_HPP_
#define CHRONOS_QUERY_CSEG_SCAN_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultCsegScanLogicalByteLimit =
    std::size_t{256U} * 1024U * 1024U +
    static_cast<std::size_t>(cseg::format::kMaximumGranuleRowCount) * sizeof(std::uint32_t);
inline constexpr std::size_t kDefaultCsegScanRetainedByteLimit = std::size_t{512U} * 1024U * 1024U;

// Trusted immutable ownership pin for one complete in-memory CSEG image. The opaque owner must
// keep every byte in bytes() alive and immutable. retained_buffer_bytes() conservatively includes
// the complete owner allocation and any external storage/snapshot pin represented by that owner.
class CsegPartPin {
public:
  CsegPartPin() = delete;
  CsegPartPin(const CsegPartPin&) noexcept = default;
  CsegPartPin& operator=(const CsegPartPin&) noexcept = default;
  CsegPartPin(CsegPartPin&&) noexcept = default;
  CsegPartPin& operator=(CsegPartPin&&) noexcept = default;

  [[nodiscard]] static common::Result<CsegPartPin> create(std::shared_ptr<const void> owner,
                                                          common::ByteView bytes,
                                                          std::size_t retained_buffer_bytes);

  [[nodiscard]] constexpr common::ByteView bytes() const noexcept {
    return bytes_;
  }
  [[nodiscard]] constexpr std::size_t retained_buffer_bytes() const noexcept {
    return retained_buffer_bytes_;
  }

private:
  CsegPartPin(std::shared_ptr<const void> owner, common::ByteView bytes,
              std::size_t retained_buffer_bytes) noexcept;

  std::shared_ptr<const void> owner_;
  common::ByteView bytes_;
  std::size_t retained_buffer_bytes_;

  friend class CsegScanOperator;
};

struct CsegScanLimits {
  cseg::CsegProjectedReaderLimits reader;
  cseg::CsegEventTimePruningLimits pruning;
  VectorChunkLimits chunk{.maximum_rows = cseg::format::kMaximumGranuleRowCount,
                          .maximum_columns = cseg::format::kMaximumUserColumnCount,
                          .maximum_buffer_bytes = kDefaultCsegScanLogicalByteLimit,
                          .maximum_retained_buffer_bytes = kDefaultCsegScanRetainedByteLimit};
  RowVersionScanMode row_version_columns{RowVersionScanMode::kOmit};
};

// Thread-affine single-part physical source. Creation reserves the encoded-part pin, reader
// metadata, and projection configuration before opening. Each pull reserves its conservative
// retained output charge before page validation/decompression and returns at most one granule.
class CsegScanOperator final : public PhysicalOperator {
public:
  ~CsegScanOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(const QueryResourceContext& resources, CsegPartPin part,
         const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
         const schema::TabletId& target_tablet,
         std::vector<std::uint32_t> destination_column_ordinals, CsegScanLimits limits = {});

  // Uses authenticated part/granule event-time extrema only to skip provably disjoint granules.
  // Selected granules are still emitted in full; an exact predicate operator remains authoritative.
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>> create_event_time_pruned(
      const QueryResourceContext& resources, CsegPartPin part, const schema::SchemaLineage& lineage,
      schema::SchemaId destination_schema_id, const schema::TabletId& target_tablet,
      std::vector<std::uint32_t> destination_column_ordinals, cseg::EventTimePredicate predicate,
      CsegScanLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class State;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create_impl(const QueryResourceContext& resources, CsegPartPin part,
              const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
              const schema::TabletId& target_tablet,
              std::vector<std::uint32_t> destination_column_ordinals,
              std::optional<cseg::EventTimePredicate> predicate, CsegScanLimits limits);

  explicit CsegScanOperator(std::unique_ptr<State> state) noexcept;

  std::unique_ptr<State> state_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_CSEG_SCAN_HPP_
