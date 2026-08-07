#ifndef CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_
#define CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/cseg_scan.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::uint32_t kDefaultSnapshotCsegPartScanPartLimit = 1U << 16U;
inline constexpr std::size_t kDefaultSnapshotCsegPartScanConfigurationByteLimit =
    std::size_t{8U} * 1024U * 1024U;

struct SnapshotCsegPartScanPlanLimits {
  std::uint32_t maximum_parts{kDefaultSnapshotCsegPartScanPartLimit};
  std::uint32_t maximum_selected_parts{kDefaultSnapshotCsegPartScanPartLimit};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultSnapshotCsegPartScanConfigurationByteLimit};
};

// Bounded CSEG-only work selected from one tablet in one exact aggregate database epoch. Part
// identities preserve canonical Manifest order. The optional predicate is pruning evidence only.
class SnapshotCsegPartScanPlan {
public:
  SnapshotCsegPartScanPlan() = delete;
  SnapshotCsegPartScanPlan(const SnapshotCsegPartScanPlan&) = delete;
  SnapshotCsegPartScanPlan& operator=(const SnapshotCsegPartScanPlan&) = delete;
  SnapshotCsegPartScanPlan(SnapshotCsegPartScanPlan&&) noexcept = default;
  SnapshotCsegPartScanPlan& operator=(SnapshotCsegPartScanPlan&&) noexcept = default;

  [[nodiscard]] const manifest::DatabaseId& database_id() const noexcept;
  [[nodiscard]] const wal::WalId& wal_id() const noexcept;
  [[nodiscard]] std::uint64_t snapshot_generation() const noexcept;
  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const std::optional<cseg::EventTimePredicate>&
  event_time_predicate() const noexcept;
  [[nodiscard]] std::span<const cseg::PartId> selected_part_ids() const noexcept;
  [[nodiscard]] std::uint32_t selected_part_count() const noexcept;
  [[nodiscard]] std::uint32_t skipped_part_count() const noexcept;
  [[nodiscard]] std::uint64_t selected_rows() const noexcept;
  [[nodiscard]] std::uint64_t skipped_rows() const noexcept;
  [[nodiscard]] std::size_t retained_configuration_bytes() const noexcept;

private:
  struct Metrics {
    std::uint32_t skipped_part_count{};
    std::uint64_t selected_rows{};
    std::uint64_t skipped_rows{};
    std::size_t retained_configuration_bytes{};
  };

  SnapshotCsegPartScanPlan(manifest::DatabaseId database_id, wal::WalId wal_id,
                           std::uint64_t snapshot_generation, schema::TableId table_id,
                           schema::TabletId tablet_id,
                           std::optional<cseg::EventTimePredicate> event_time_predicate,
                           std::vector<cseg::PartId> selected_part_ids, Metrics metrics) noexcept;

  manifest::DatabaseId database_id_;
  wal::WalId wal_id_;
  std::uint64_t snapshot_generation_{};
  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  std::optional<cseg::EventTimePredicate> event_time_predicate_;
  std::vector<cseg::PartId> selected_part_ids_;
  std::uint32_t skipped_part_count_{};
  std::uint64_t selected_rows_{};
  std::uint64_t skipped_rows_{};
  std::size_t retained_configuration_bytes_{};

  friend common::Result<SnapshotCsegPartScanPlan>
  plan_snapshot_cseg_part_scan(const manifest::DatabaseStorageSnapshot&, const schema::TabletId&,
                               const std::optional<cseg::EventTimePredicate>&,
                               SnapshotCsegPartScanPlanLimits);
};

// Selects only durable CSEG work. It deliberately does not inspect or cover mutable heads.
[[nodiscard]] common::Result<SnapshotCsegPartScanPlan> plan_snapshot_cseg_part_scan(
    const manifest::DatabaseStorageSnapshot& snapshot, const schema::TabletId& target_tablet,
    const std::optional<cseg::EventTimePredicate>& event_time_predicate = std::nullopt,
    SnapshotCsegPartScanPlanLimits limits = {});

// Loads the exact selected provider images after reproving plan/snapshot provenance. An empty plan
// returns an empty owner vector without touching the filesystem.
[[nodiscard]] common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>>
load_snapshot_cseg_part_scan_images(
    const manifest::ManifestStorage& storage, const manifest::DatabaseStorageSnapshot& snapshot,
    const SnapshotCsegPartScanPlan& plan, const schema::SchemaLineage& lineage,
    manifest::ReferencedPartValidationLimits validation_limits = {});

// Converts one storage-validated, snapshot-bound image into the generic immutable CSEG pin. The
// image's aggregate publication token and complete conservative charge follow every pin copy.
[[nodiscard]] common::Result<CsegPartPin>
pin_snapshot_cseg_part(std::shared_ptr<const manifest::SnapshotPartImage> image);

// Creates one single-part scan only after the snapshot descriptor, retained lineage, destination
// schema, and target tablet agree. Page-level binding/integrity remains the CSEG reader's job.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_cseg_scan(
    const QueryResourceContext& resources, std::shared_ptr<const manifest::SnapshotPartImage> image,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
    CsegScanLimits limits = {});

// Eagerly validates and adopts every selected image, then emits child chunks in canonical part and
// physical granule order. This remains CSEG-only and is not a complete tablet scan with live heads.
[[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>> create_snapshot_cseg_part_scan(
    const QueryResourceContext& resources, const SnapshotCsegPartScanPlan& plan,
    std::vector<std::shared_ptr<const manifest::SnapshotPartImage>> images,
    const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
    const std::vector<std::uint32_t>& destination_column_ordinals, CsegScanLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DATABASE_CSEG_SCAN_HPP_
