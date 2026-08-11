#ifndef CHRONOS_TIERING_TIERED_PUBLICATION_HPP_
#define CHRONOS_TIERING_TIERED_PUBLICATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/tiering/cold_manifest_storage.hpp"

#include <cstdint>
#include <memory>
#include <span>

namespace chronos::tiering {

class TieredLocalPartReclamationCoordinator;

namespace detail {
class TieredDatabaseStorageEpoch;
class TieredDatabaseStoragePublisherImpl;
} // namespace detail

class TieredDatabaseStorageSnapshot {
public:
  TieredDatabaseStorageSnapshot() = delete;
  TieredDatabaseStorageSnapshot(const TieredDatabaseStorageSnapshot&) noexcept = default;
  TieredDatabaseStorageSnapshot& operator=(const TieredDatabaseStorageSnapshot&) noexcept = default;
  TieredDatabaseStorageSnapshot(TieredDatabaseStorageSnapshot&&) noexcept = default;
  TieredDatabaseStorageSnapshot& operator=(TieredDatabaseStorageSnapshot&&) noexcept = default;
  ~TieredDatabaseStorageSnapshot() = default;

  [[nodiscard]] std::uint64_t manifest_generation() const noexcept;
  [[nodiscard]] const manifest::DatabaseId& database_id() const noexcept;
  // Returned references/pointers remain valid for this snapshot's lifetime.
  [[nodiscard]] const manifest::TemporalDatabaseStorageSnapshot& manifest_snapshot() const noexcept;
  [[nodiscard]] const LoadedColdLocationManifest* cold_manifest() const noexcept;
  [[nodiscard]] const ColdPartLocationDescriptor*
  find_cold_location(const cseg::PartId& part_id) const noexcept;

private:
  explicit TieredDatabaseStorageSnapshot(
      std::shared_ptr<const detail::TieredDatabaseStorageEpoch> epoch) noexcept;

  std::shared_ptr<const detail::TieredDatabaseStorageEpoch> epoch_;

  friend class detail::TieredDatabaseStoragePublisherImpl;
  friend class TieredDatabaseStoragePublisher;
};

struct DurableTieredDatabaseStoragePublicationRequest {
  // Both owners must already have crossed their independent directory-sync durability boundaries.
  // Publication validates and release-publishes only their exact compatible pair.
  manifest::TemporalDatabaseStorageSnapshot manifest_snapshot;
  std::shared_ptr<const LoadedColdLocationManifest> cold_manifest;
  std::span<const manifest::TabletSchemaBinding> schema_bindings;
  manifest::ManifestDecodeLimits manifest_decode_limits;
  ColdLocationManifestDecodeLimits cold_decode_limits;
};

// Single-writer publication owner. One atomic shared epoch owns the Manifest v2 snapshot and its
// optional compatible cold-location generation. Readers acquire one old-or-new pair. Invalid input
// after a claimed durable successor fails the owner closed for restart reconciliation.
class TieredDatabaseStoragePublisher {
public:
  TieredDatabaseStoragePublisher() = delete;
  ~TieredDatabaseStoragePublisher();
  TieredDatabaseStoragePublisher(const TieredDatabaseStoragePublisher&) = delete;
  TieredDatabaseStoragePublisher& operator=(const TieredDatabaseStoragePublisher&) = delete;
  TieredDatabaseStoragePublisher(TieredDatabaseStoragePublisher&&) noexcept;
  TieredDatabaseStoragePublisher& operator=(TieredDatabaseStoragePublisher&&) noexcept;

  [[nodiscard]] static common::Result<TieredDatabaseStoragePublisher>
  create(manifest::TemporalDatabaseStorageSnapshot manifest_snapshot,
         std::shared_ptr<const LoadedColdLocationManifest> cold_manifest,
         manifest::ManifestDecodeLimits manifest_limits = {},
         ColdLocationManifestDecodeLimits cold_limits = {});

  [[nodiscard]] common::Result<TieredDatabaseStorageSnapshot> snapshot() const;
  [[nodiscard]] common::Result<TieredDatabaseStorageSnapshot>
  publish(const DurableTieredDatabaseStoragePublicationRequest& request);

  void fail_closed_after_durable_successor() noexcept;
  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  explicit TieredDatabaseStoragePublisher(
      std::unique_ptr<detail::TieredDatabaseStoragePublisherImpl> impl) noexcept;
  std::unique_ptr<detail::TieredDatabaseStoragePublisherImpl> impl_;

  friend class TieredLocalPartReclamationCoordinator;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_PUBLICATION_HPP_
