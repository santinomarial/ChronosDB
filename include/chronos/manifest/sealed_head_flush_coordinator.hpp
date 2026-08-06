#ifndef CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_COORDINATOR_HPP_
#define CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_COORDINATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/manifest/validation.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace chronos::manifest {

struct SealedHeadFlushOperation {
  cseg::PartId part_id;
  common::Uuid part_nonce;
  common::Uuid manifest_nonce;
  cseg::PageCompression compression{cseg::PageCompression::kNone};
  std::span<const RetryDescriptor> new_retries;
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestDecodeLimits manifest_decode_limits;
  ReferencedPartValidationLimits part_validation_limits;
};

struct SealedHeadFlushCompletion {
  cseg::PartId part_id;
  std::uint64_t manifest_generation{};
  std::uint64_t queue_sequence{};
  std::uint32_t row_count{};
  bool resumed_durable_manifest{false};

  friend bool operator==(const SealedHeadFlushCompletion&,
                         const SealedHeadFlushCompletion&) = default;
};

struct SealedHeadFlushCoordinatorMetrics {
  std::uint64_t attempts{};
  std::uint64_t empty_polls{};
  std::uint64_t failures{};
  std::uint64_t completed{};
  std::uint64_t resumed_durable_manifests{};
  std::uint64_t encoded_rows{};
  std::uint64_t encoded_bytes{};
  bool failed{false};

  friend bool operator==(const SealedHeadFlushCoordinatorMetrics&,
                         const SealedHeadFlushCoordinatorMetrics&) = default;
};

// Single-threaded storage-owner coordinator. The queue, storage, publisher, and TabletState passed
// to try_flush_one must not be mutated by another writer during a call. The queue is shared-owned;
// storage and publisher must outlive this object. Failures before Manifest directory sync leave the
// exact work retryable. Once a successor Manifest is known durable, an unexpected failure fails the
// coordinator and tablet closed so restart recovery can reconcile the durable generation.
class SealedHeadFlushCoordinator {
public:
  SealedHeadFlushCoordinator() = delete;
  ~SealedHeadFlushCoordinator() = default;

  SealedHeadFlushCoordinator(const SealedHeadFlushCoordinator&) = delete;
  SealedHeadFlushCoordinator& operator=(const SealedHeadFlushCoordinator&) = delete;
  SealedHeadFlushCoordinator(SealedHeadFlushCoordinator&&) noexcept = default;
  SealedHeadFlushCoordinator& operator=(SealedHeadFlushCoordinator&&) noexcept = default;

  [[nodiscard]] static common::Result<SealedHeadFlushCoordinator>
  create(std::shared_ptr<ingest::SealedHeadFlushQueue> queue, ManifestStorage& storage,
         DatabaseStoragePublisher& publisher);

  // Acquires at most one FIFO item. An empty optional means no ready work. Fresh file identities
  // are caller-provided because identity generation is an outer storage policy. A retry after an
  // installed successor uses the same part identity and resumes publication without reinstalling.
  [[nodiscard]] common::Result<std::optional<SealedHeadFlushCompletion>>
  try_flush_one(ingest::TabletState& tablet, const SealedHeadFlushOperation& operation);

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] SealedHeadFlushCoordinatorMetrics metrics() const noexcept;

private:
  SealedHeadFlushCoordinator(std::shared_ptr<ingest::SealedHeadFlushQueue> queue,
                             ManifestStorage& storage,
                             DatabaseStoragePublisher& publisher) noexcept;

  std::shared_ptr<ingest::SealedHeadFlushQueue> queue_;
  ManifestStorage* storage_{};
  DatabaseStoragePublisher* publisher_{};
  common::Status poison_status_;
  SealedHeadFlushCoordinatorMetrics metrics_;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_COORDINATOR_HPP_
