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

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  explicit TemporalDatabaseStoragePublisher(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_PUBLICATION_HPP_
