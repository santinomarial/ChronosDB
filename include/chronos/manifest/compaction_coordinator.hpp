#ifndef CHRONOS_MANIFEST_COMPACTION_COORDINATOR_HPP_
#define CHRONOS_MANIFEST_COMPACTION_COORDINATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/manifest/compaction.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"

#include <cstdint>
#include <span>

namespace chronos::manifest {

struct AppendOnlyCompactionOperation {
  schema::TabletId tablet_id;
  std::span<const cseg::PartId> input_part_ids;
  cseg::PartId output_part_id;
  common::Uuid part_nonce;
  common::Uuid manifest_nonce;
  cseg::PageCompression compression{cseg::PageCompression::kNone};
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestDecodeLimits manifest_decode_limits;
  ReferencedPartValidationLimits part_validation_limits;
  AppendOnlyCompactionLimits compaction_limits;
};

struct AppendOnlyCompactionCompletion {
  cseg::PartId output_part_id;
  std::uint64_t manifest_generation{};
  std::uint64_t row_count{};
  bool resumed_durable_manifest{false};

  friend bool operator==(const AppendOnlyCompactionCompletion&,
                         const AppendOnlyCompactionCompletion&) = default;
};

struct AppendOnlyCompactionCoordinatorMetrics {
  std::uint64_t attempts{};
  std::uint64_t failures{};
  std::uint64_t completed{};
  std::uint64_t resumed_durable_manifests{};
  std::uint64_t input_parts{};
  std::uint64_t compacted_rows{};
  std::uint64_t output_bytes{};
  bool failed{false};

  friend bool operator==(const AppendOnlyCompactionCoordinatorMetrics&,
                         const AppendOnlyCompactionCoordinatorMetrics&) = default;
};

// Single-threaded storage-owner composition of authoritative input reread, reference merge,
// output/Manifest installation, reload, and aggregate publication. A pre-Manifest failure leaves
// the predecessor selected. Once an exact successor Manifest is durable, any unexpected failure
// fails this coordinator closed so restart recovery can select the durable generation.
class AppendOnlyCompactionCoordinator {
public:
  AppendOnlyCompactionCoordinator() = delete;

  [[nodiscard]] static common::Result<AppendOnlyCompactionCoordinator>
  create(ManifestStorage& storage, DatabaseStoragePublisher& publisher);

  [[nodiscard]] common::Result<AppendOnlyCompactionCompletion>
  compact(const AppendOnlyCompactionOperation& operation);

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] AppendOnlyCompactionCoordinatorMetrics metrics() const noexcept;

private:
  AppendOnlyCompactionCoordinator(ManifestStorage& storage,
                                  DatabaseStoragePublisher& publisher) noexcept;

  ManifestStorage* storage_{};
  DatabaseStoragePublisher* publisher_{};
  common::Status poison_status_;
  AppendOnlyCompactionCoordinatorMetrics metrics_;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_COMPACTION_COORDINATOR_HPP_
