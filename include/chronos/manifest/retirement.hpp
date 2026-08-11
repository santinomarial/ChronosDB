#ifndef CHRONOS_MANIFEST_RETIREMENT_HPP_
#define CHRONOS_MANIFEST_RETIREMENT_HPP_

#include "chronos/cseg/types.hpp"
#include "chronos/manifest/temporal_codec.hpp"

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::manifest {

namespace detail {
class DatabaseStoragePublication;
class DatabaseStoragePublisherImpl;
class ManifestStorageTestAccess;
class PartRetentionPin;
class TemporalDatabaseStoragePublisherImpl;
} // namespace detail

class DatabaseStorageSnapshot;
class LoadedTemporalManifestGeneration;

// Copyable ownership pin for one exact aggregate publication epoch. Consumers that retain
// descriptors or may open their files must retain either the snapshot or a token copied from it.
class DatabaseStorageRetentionToken {
public:
  DatabaseStorageRetentionToken() = delete;
  DatabaseStorageRetentionToken(const DatabaseStorageRetentionToken&) noexcept = default;
  DatabaseStorageRetentionToken& operator=(const DatabaseStorageRetentionToken&) noexcept = default;
  DatabaseStorageRetentionToken(DatabaseStorageRetentionToken&&) noexcept = default;
  DatabaseStorageRetentionToken& operator=(DatabaseStorageRetentionToken&&) noexcept = default;
  ~DatabaseStorageRetentionToken() = default;

  [[nodiscard]] std::uint64_t generation() const noexcept;

private:
  explicit DatabaseStorageRetentionToken(
      std::shared_ptr<const detail::DatabaseStoragePublication> publication) noexcept;

  std::shared_ptr<const detail::DatabaseStoragePublication> publication_;

  friend class DatabaseStorageSnapshot;
};

struct RetiredPartFile {
  cseg::PartId part_id;
  std::uint64_t file_length{};

  friend bool operator==(const RetiredPartFile&, const RetiredPartFile&) = default;
};

// Move-only proof issued by successful compaction publication. Every publication epoch that names
// an input shares that input's private lifetime pin. Reclamation remains pending while any such pin
// is live; after all weak pins expire they can never be reacquired, so reclamation may proceed.
class RetiredPartSet {
public:
  RetiredPartSet() = delete;
  RetiredPartSet(const RetiredPartSet&) = delete;
  RetiredPartSet& operator=(const RetiredPartSet&) = delete;
  RetiredPartSet(RetiredPartSet&&) noexcept = default;
  RetiredPartSet& operator=(RetiredPartSet&&) noexcept = default;
  ~RetiredPartSet() = default;

  [[nodiscard]] std::uint64_t predecessor_generation() const noexcept;
  [[nodiscard]] std::span<const RetiredPartFile> parts() const noexcept;
  [[nodiscard]] bool is_pinned() const noexcept;

private:
  RetiredPartSet(std::uint64_t predecessor_generation, std::vector<RetiredPartFile> parts,
                 std::vector<std::weak_ptr<const detail::PartRetentionPin>> part_pins) noexcept;

  std::uint64_t predecessor_generation_{};
  std::vector<RetiredPartFile> parts_;
  std::vector<std::weak_ptr<const detail::PartRetentionPin>> part_pins_;

  friend class detail::DatabaseStoragePublisherImpl;
  friend class detail::ManifestStorageTestAccess;
};

// Move-only proof issued only by an authorized Manifest v2 source-retirement publication. Weak
// owners cover every still-live published generation that names any removed descriptor: while one
// remains live a reader may still open a retired file. Once all weak owners expire they cannot be
// reacquired, and a separate storage operation may revalidate and reclaim the exact files.
class TemporalRetiredPartSet {
public:
  TemporalRetiredPartSet() = delete;
  TemporalRetiredPartSet(const TemporalRetiredPartSet&) = delete;
  TemporalRetiredPartSet& operator=(const TemporalRetiredPartSet&) = delete;
  TemporalRetiredPartSet(TemporalRetiredPartSet&&) noexcept = default;
  TemporalRetiredPartSet& operator=(TemporalRetiredPartSet&&) noexcept = default;
  ~TemporalRetiredPartSet() = default;

  [[nodiscard]] std::uint64_t predecessor_generation() const noexcept;
  [[nodiscard]] std::span<const TemporalPartDescriptor> parts() const noexcept;
  [[nodiscard]] bool is_pinned() const noexcept;

private:
  TemporalRetiredPartSet(
      std::uint64_t predecessor_generation, std::vector<TemporalPartDescriptor> parts,
      std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> generation_pins) noexcept;

  std::uint64_t predecessor_generation_{};
  std::vector<TemporalPartDescriptor> parts_;
  std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> generation_pins_;

  friend class detail::TemporalDatabaseStoragePublisherImpl;
  friend class detail::ManifestStorageTestAccess;
  friend class ManifestStorage;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_RETIREMENT_HPP_
