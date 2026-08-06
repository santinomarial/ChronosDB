#ifndef CHRONOS_MANIFEST_RETIREMENT_HPP_
#define CHRONOS_MANIFEST_RETIREMENT_HPP_

#include "chronos/cseg/types.hpp"

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::manifest {

namespace detail {
class DatabaseStoragePublication;
class DatabaseStoragePublisherImpl;
class ManifestStorageTestAccess;
} // namespace detail

class DatabaseStorageSnapshot;

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

// Move-only proof issued by successful compaction publication. A live predecessor pin makes the
// record pending. Once all pins expire they can never be reacquired, so reclamation may proceed.
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
                 std::weak_ptr<const detail::DatabaseStoragePublication> predecessor) noexcept;

  std::uint64_t predecessor_generation_{};
  std::vector<RetiredPartFile> parts_;
  std::weak_ptr<const detail::DatabaseStoragePublication> predecessor_;

  friend class detail::DatabaseStoragePublisherImpl;
  friend class detail::ManifestStorageTestAccess;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_RETIREMENT_HPP_
