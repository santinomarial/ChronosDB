#ifndef CHRONOS_MANIFEST_TEMPORAL_PUBLICATION_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_PUBLICATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/validation.hpp"

#include <cstdint>
#include <memory>
#include <span>

namespace chronos::manifest {

namespace detail {
class TemporalDatabaseStoragePublisherImpl;
}

struct DurableTemporalManifestPublicationRequest {
  // This owner must be loaded from ManifestStorage after its generation crossed the Manifest
  // directory-sync boundary. Publication retains it as the complete new reader epoch.
  std::shared_ptr<const LoadedTemporalManifestGeneration> selected_manifest;
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestDecodeLimits decode_limits;
};

// A copyable owning acquire-load of exactly one immutable Manifest v2 epoch.
class TemporalDatabaseStorageSnapshot {
public:
  TemporalDatabaseStorageSnapshot() = delete;
  TemporalDatabaseStorageSnapshot(const TemporalDatabaseStorageSnapshot&) noexcept = default;
  TemporalDatabaseStorageSnapshot&
  operator=(const TemporalDatabaseStorageSnapshot&) noexcept = default;
  TemporalDatabaseStorageSnapshot(TemporalDatabaseStorageSnapshot&&) noexcept = default;
  TemporalDatabaseStorageSnapshot& operator=(TemporalDatabaseStorageSnapshot&&) noexcept = default;
  ~TemporalDatabaseStorageSnapshot() = default;

  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] const DatabaseId& database_id() const noexcept;
  [[nodiscard]] common::ByteView manifest_bytes() const noexcept;
  [[nodiscard]] std::span<const TemporalTabletDescriptor> tablets() const noexcept;
  [[nodiscard]] std::span<const TemporalPartDescriptor> parts() const noexcept;
  [[nodiscard]] std::span<const TemporalRetryDescriptor> retries() const noexcept;
  [[nodiscard]] std::shared_ptr<const LoadedTemporalManifestGeneration>
  selected_manifest() const noexcept;

private:
  explicit TemporalDatabaseStorageSnapshot(
      std::shared_ptr<const LoadedTemporalManifestGeneration> selected) noexcept;

  std::shared_ptr<const LoadedTemporalManifestGeneration> selected_;

  friend class TemporalDatabaseStoragePublisher;
  friend class detail::TemporalDatabaseStoragePublisherImpl;
};

struct DurableTemporalSourceRetirementPublicationRequest {
  // This owner must be the exact durable successor selected after source-retirement installation.
  std::shared_ptr<const LoadedTemporalManifestGeneration> selected_manifest;
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestDecodeLimits decode_limits;
  const RaftTabletSourceRetirementRequest* source_retirement{};
};

struct PublishedTemporalSourceRetirement {
  TemporalDatabaseStorageSnapshot snapshot;
  TemporalRetiredPartSet retirement;
};

// Single-writer publication owner. Readers acquire one complete generation with acquire ordering;
// the writer release-publishes only an exact already-durable successor. A failure after receiving
// such a successor fails the live owner closed so restart recovery can select durable truth.
class TemporalDatabaseStoragePublisher {
public:
  TemporalDatabaseStoragePublisher() = delete;
  ~TemporalDatabaseStoragePublisher();
  TemporalDatabaseStoragePublisher(const TemporalDatabaseStoragePublisher&) = delete;
  TemporalDatabaseStoragePublisher& operator=(const TemporalDatabaseStoragePublisher&) = delete;
  TemporalDatabaseStoragePublisher(TemporalDatabaseStoragePublisher&&) noexcept;
  TemporalDatabaseStoragePublisher& operator=(TemporalDatabaseStoragePublisher&&) noexcept;

  [[nodiscard]] static common::Result<TemporalDatabaseStoragePublisher>
  create(std::shared_ptr<const LoadedTemporalManifestGeneration> selected,
         std::span<const TabletSchemaBinding> schema_bindings, ManifestDecodeLimits limits = {});

  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot> snapshot() const;
  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot>
  publish_manifest(const DurableTemporalManifestPublicationRequest& request);

  // Atomically selects one exact already-durable source-retirement successor and returns the only
  // proof that can later authorize its removed parts for reader-pinned reclamation.
  [[nodiscard]] common::Result<PublishedTemporalSourceRetirement>
  publish_source_retirement_manifest(
      const DurableTemporalSourceRetirementPublicationRequest& request);

  // Used by a coordinator that has crossed the durable Manifest boundary but cannot complete or
  // revalidate live publication. Subsequent snapshots fail until restart recovery reconstructs
  // the owner from durable truth.
  void fail_closed_after_durable_successor() noexcept;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  explicit TemporalDatabaseStoragePublisher(
      std::unique_ptr<detail::TemporalDatabaseStoragePublisherImpl> implementation) noexcept;
  std::unique_ptr<detail::TemporalDatabaseStoragePublisherImpl> implementation_;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_PUBLICATION_HPP_
