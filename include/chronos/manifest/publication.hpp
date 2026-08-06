#ifndef CHRONOS_MANIFEST_PUBLICATION_HPP_
#define CHRONOS_MANIFEST_PUBLICATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/retirement.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::manifest {

namespace detail {
class DatabaseStoragePublication;
class DatabaseStoragePublisherImpl;
class DatabaseStoragePublisherTestAccess;
class PublishedTabletStorageBuilder;
} // namespace detail

// One tablet's exact query-visible in-memory head set in a database storage epoch. Sealed heads
// precede the active head. A recovered durable-only tablet can temporarily have no live head while
// the aggregate recovery owner is still assembling its unpublished state.
class PublishedTabletStorage {
public:
  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const std::optional<head::HeadCommitPosition>& applied_position() const noexcept;
  [[nodiscard]] std::span<const head::HeadSnapshot> sealed_heads() const noexcept;
  [[nodiscard]] const head::HeadSnapshot* active_head() const noexcept;
  [[nodiscard]] std::size_t visible_head_row_count() const noexcept;

private:
  PublishedTabletStorage(schema::TableId table_id, schema::TabletId tablet_id,
                         std::optional<head::HeadCommitPosition> applied_position,
                         std::vector<head::HeadSnapshot> sealed_heads,
                         std::vector<head::HeadSnapshot> active_head,
                         std::size_t visible_head_rows) noexcept;

  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  std::optional<head::HeadCommitPosition> applied_position_;
  std::vector<head::HeadSnapshot> sealed_heads_;
  // A vector is used instead of optional because HeadSnapshot deliberately has no empty state.
  std::vector<head::HeadSnapshot> active_head_;
  std::size_t visible_head_rows_{};

  friend class detail::DatabaseStoragePublisherImpl;
  friend class detail::PublishedTabletStorageBuilder;
};

struct DatabaseStorageTabletInput {
  std::reference_wrapper<const ingest::TabletSnapshot> snapshot;
};

struct SealedHeadReplacement {
  schema::TabletId tablet_id;
  std::uint64_t head_generation{};
  cseg::PartId replacement_part_id;
};

struct DurableManifestPublicationRequest {
  // This owner must have been returned by ManifestStorage after the candidate crossed its
  // directory-sync durability boundary. The publisher retains it in the new epoch.
  std::shared_ptr<const LoadedManifestGeneration> selected_manifest;
  std::span<const SealedHeadReplacement> replacements;
};

struct DurableCompactionPublicationRequest {
  // This must be the exact next generation returned after the compaction Manifest crossed its
  // directory-sync durability boundary.
  std::shared_ptr<const LoadedManifestGeneration> selected_manifest;
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestCompactionReplacement replacement;
};

// Copyable owning result of one acquire load. Every returned descriptor, byte view, and head pin
// remains valid for this object's lifetime. Readers must not combine fields from separate values.
class DatabaseStorageSnapshot {
public:
  DatabaseStorageSnapshot() = delete;
  DatabaseStorageSnapshot(const DatabaseStorageSnapshot&) noexcept = default;
  DatabaseStorageSnapshot& operator=(const DatabaseStorageSnapshot&) noexcept = default;
  DatabaseStorageSnapshot(DatabaseStorageSnapshot&&) noexcept = default;
  DatabaseStorageSnapshot& operator=(DatabaseStorageSnapshot&&) noexcept = default;
  ~DatabaseStorageSnapshot() = default;

  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] const DatabaseId& database_id() const noexcept;
  [[nodiscard]] const wal::WalId& wal_id() const noexcept;
  [[nodiscard]] const WalCheckpoint& reclaim_checkpoint() const noexcept;
  [[nodiscard]] common::ByteView manifest_bytes() const noexcept;
  [[nodiscard]] std::span<const TabletDescriptor> durable_tablets() const noexcept;
  [[nodiscard]] std::span<const PartDescriptor> parts() const noexcept;
  [[nodiscard]] std::span<const RetryDescriptor> retries() const noexcept;
  [[nodiscard]] std::span<const PublishedTabletStorage> tablets() const noexcept;
  [[nodiscard]] std::span<const ingest::SealedGenerationRetirementReceipt>
  retirement_receipts() const noexcept;
  [[nodiscard]] const PublishedTabletStorage*
  find_tablet(const schema::TabletId& tablet_id) const noexcept;
  [[nodiscard]] std::size_t visible_head_row_count() const noexcept;
  [[nodiscard]] DatabaseStorageRetentionToken retention_token() const noexcept;

private:
  explicit DatabaseStorageSnapshot(
      std::shared_ptr<const detail::DatabaseStoragePublication> publication) noexcept;

  std::shared_ptr<const detail::DatabaseStoragePublication> publication_;

  friend class detail::DatabaseStoragePublisherImpl;
};

// Single-writer aggregate publication owner. Readers acquire one immutable shared_ptr epoch.
// publish_tablet_snapshot refreshes one tablet under the same durable generation. publish_manifest
// may be called only after durable installation and atomically substitutes exact installed parts
// for exact sealed heads. Neither method performs filesystem or WAL mutation.
class DatabaseStoragePublisher {
public:
  DatabaseStoragePublisher() = delete;
  ~DatabaseStoragePublisher();

  DatabaseStoragePublisher(const DatabaseStoragePublisher&) = delete;
  DatabaseStoragePublisher& operator=(const DatabaseStoragePublisher&) = delete;
  DatabaseStoragePublisher(DatabaseStoragePublisher&&) noexcept;
  DatabaseStoragePublisher& operator=(DatabaseStoragePublisher&&) noexcept;

  [[nodiscard]] static common::Result<DatabaseStoragePublisher>
  create(std::shared_ptr<const LoadedManifestGeneration> selected_manifest,
         std::span<const DatabaseStorageTabletInput> tablets);

  [[nodiscard]] common::Result<DatabaseStorageSnapshot> snapshot() const;

  // Replaces or inserts one complete tablet epoch while retaining the selected durable manifest.
  // The tablet snapshot must contain only WAL rows later than its manifest durable boundary.
  [[nodiscard]] common::Result<DatabaseStorageSnapshot>
  publish_tablet_snapshot(const ingest::TabletSnapshot& tablet);

  // The selected generation must be the exact next generation. Every new part must name and
  // exactly cover one listed sealed head; all remaining heads must lie after new durable bounds.
  // Any failure after this method receives a durable successor fails this live owner closed.
  [[nodiscard]] common::Result<DatabaseStorageSnapshot>
  publish_manifest(const DurableManifestPublicationRequest& request);

  // Atomically selects one already-durable append-only compaction generation. Live heads are
  // unchanged; old snapshots retain their predecessor Manifest owner and exact input descriptors.
  // Any validation/allocation failure after receiving a durable successor fails this owner closed.
  [[nodiscard]] common::Result<DatabaseStorageSnapshot>
  publish_compaction_manifest(const DurableCompactionPublicationRequest& request);

  // Moves out every retirement proof issued since the previous drain, in publication order.
  // An undrained proof retains no predecessor and therefore cannot itself delay reclamation.
  [[nodiscard]] common::Result<std::vector<RetiredPartSet>> drain_retired_part_sets();

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  using PublicationHook = void (*)(void*) noexcept;

  explicit DatabaseStoragePublisher(
      std::unique_ptr<detail::DatabaseStoragePublisherImpl> implementation) noexcept;
  [[nodiscard]] static common::Result<DatabaseStoragePublisher>
  create_with_publication_hook(std::shared_ptr<const LoadedManifestGeneration> selected_manifest,
                               std::span<const DatabaseStorageTabletInput> tablets,
                               PublicationHook hook, void* hook_context);

  std::unique_ptr<detail::DatabaseStoragePublisherImpl> implementation_;

  friend class detail::DatabaseStoragePublisherTestAccess;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_PUBLICATION_HPP_
