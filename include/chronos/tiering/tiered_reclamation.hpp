#ifndef CHRONOS_TIERING_TIERED_RECLAMATION_HPP_
#define CHRONOS_TIERING_TIERED_RECLAMATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/tiering/object_store.hpp"
#include "chronos/tiering/tiered_pair_commit.hpp"
#include "chronos/tiering/tiered_publication.hpp"

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::tiering {

class TieredLocalPartReclamationProof {
public:
  TieredLocalPartReclamationProof() = delete;
  TieredLocalPartReclamationProof(const TieredLocalPartReclamationProof&) = delete;
  TieredLocalPartReclamationProof& operator=(const TieredLocalPartReclamationProof&) = delete;
  TieredLocalPartReclamationProof(TieredLocalPartReclamationProof&&) noexcept = default;
  TieredLocalPartReclamationProof& operator=(TieredLocalPartReclamationProof&&) noexcept = default;

  [[nodiscard]] std::uint64_t pair_generation() const noexcept;
  [[nodiscard]] std::uint64_t manifest_generation() const noexcept;
  [[nodiscard]] std::span<const manifest::TemporalPartDescriptor> parts() const noexcept;
  [[nodiscard]] bool is_pinned() const noexcept;

private:
  TieredLocalPartReclamationProof(
      TieredPairCommitRecord record, TieredDatabaseStorageSnapshot snapshot,
      std::vector<manifest::TemporalPartDescriptor> parts,
      std::vector<std::weak_ptr<const detail::TieredDatabaseStorageEpoch>> reader_pins) noexcept;

  TieredPairCommitRecord record_;
  TieredDatabaseStorageSnapshot snapshot_;
  std::vector<manifest::TemporalPartDescriptor> parts_;
  std::vector<std::weak_ptr<const detail::TieredDatabaseStorageEpoch>> reader_pins_;

  friend class detail::TieredDatabaseStoragePublisherImpl;
  friend class TieredLocalPartReclamationCoordinator;
};

struct TieredLocalPartReclamationLimits {
  manifest::ManifestDecodeLimits manifest_decode;
  manifest::TemporalPartValidationLimits part_validation;
};

struct TieredLocalPartReclamationReport {
  manifest::PartReclamationOutcome outcome{manifest::PartReclamationOutcome::kPending};
  std::uint64_t pair_generation{};
  std::uint64_t manifest_generation{};
  std::uint64_t candidate_parts{};
  std::uint64_t remote_parts_validated{};
  std::uint64_t removed_parts{};
  std::uint64_t removed_bytes{};
  std::uint64_t already_absent_parts{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const TieredLocalPartReclamationReport&,
                         const TieredLocalPartReclamationReport&) = default;
};

class TieredLocalPartReclamationCoordinator {
public:
  TieredLocalPartReclamationCoordinator() = delete;

  // Captures every live aggregate epoch that still names a candidate but lacks an exact cold route.
  [[nodiscard]] static common::Result<TieredLocalPartReclamationProof>
  authorize(const TieredDatabaseStoragePublisher& publisher,
            const TieredPairCommitRecord& committed_pair, std::span<const cseg::PartId> part_ids);

  // Returns pending without I/O while an unsafe predecessor reader remains. Otherwise it reloads
  // the selected pair marker, fully validates every remote image, then exact-checks, unlinks, and
  // synchronizes all present local finals through ManifestStorage.
  [[nodiscard]] static common::Result<TieredLocalPartReclamationReport>
  reclaim(const TieredLocalPartReclamationProof& proof, const TieredPairCommitStorage& pair_storage,
          manifest::ManifestStorage& manifest_storage, const ObjectStore& remote_store,
          std::span<const manifest::TabletSchemaBinding> schema_bindings,
          TieredLocalPartReclamationLimits limits = {});
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_RECLAMATION_HPP_
