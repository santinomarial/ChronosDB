#ifndef CHRONOS_IO_POSIX_IO_HPP_
#define CHRONOS_IO_POSIX_IO_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::io {

namespace detail {
class PosixHandleFactory;
class ProcessLockReservation;
class PosixSyscalls;
} // namespace detail

enum class FileOpenMode : std::uint8_t {
  kReadOnly,
  kReadWrite,
};

struct RenameRequest {
  std::string_view old_name;
  std::string_view new_name;
};

enum class DirectoryEntryType : std::uint8_t {
  kRegularFile,
  kDirectory,
  kSymlink,
  kOther,
};

struct DirectoryEntry {
  std::string name;
  DirectoryEntryType type{DirectoryEntryType::kOther};

  friend bool operator==(const DirectoryEntry&, const DirectoryEntry&) = default;
};

// PosixFile exclusively owns one descriptor for a verified regular file. It is movable but not
// copyable and is not internally synchronized. Explicit-offset operations do not mutate a shared
// file cursor. Destruction closes best-effort; callers use close() when a close error is material.
class PosixFile {
public:
  PosixFile() noexcept = default;
  ~PosixFile();

  PosixFile(const PosixFile&) = delete;
  PosixFile& operator=(const PosixFile&) = delete;
  PosixFile(PosixFile&& other) noexcept;
  PosixFile& operator=(PosixFile&& other) noexcept;

  [[nodiscard]] bool is_open() const noexcept;

  // Reads until destination is full or EOF is reached. A successful short count means EOF. On an
  // error after a partial transfer, that prefix remains in destination and the returned diagnostic
  // reports the completed byte count.
  [[nodiscard]] common::Result<std::size_t> read_at(std::uint64_t offset,
                                                    common::MutableByteView destination) const;

  // Writes every source byte, retrying EINTR and short writes. A hard error after a prefix has been
  // written is still an error; a WAL writer must poison its append state in that case.
  [[nodiscard]] common::Status write_all_at(std::uint64_t offset, common::ByteView source) const;

  [[nodiscard]] common::Result<std::uint64_t> size() const;

  // Truncation is deliberately non-growing for WAL tail repair. new_size greater than the current
  // size is rejected before ftruncate is called.
  [[nodiscard]] common::Status truncate(std::uint64_t new_size) const;

  [[nodiscard]] common::Status sync_data() const;
  [[nodiscard]] common::Status sync_all() const;
  [[nodiscard]] common::Status close();

private:
  PosixFile(int descriptor, detail::PosixSyscalls& syscalls) noexcept;
  void close_best_effort() noexcept;

  int descriptor_{-1};
  detail::PosixSyscalls* syscalls_{nullptr};

  friend class PosixDirectory;
  friend class detail::PosixHandleFactory;
};

// PosixAdvisoryLock exclusively owns its descriptor. The caller must make it the only descriptor
// ChronosDB opens for a WAL LOCK inode. The nonblocking whole-file fcntl lock remains held until
// close or destruction. As POSIX record locks are process-associated, independently opening and
// closing the same inode can release it.
class PosixAdvisoryLock {
public:
  PosixAdvisoryLock() noexcept;
  ~PosixAdvisoryLock();

  PosixAdvisoryLock(const PosixAdvisoryLock&) = delete;
  PosixAdvisoryLock& operator=(const PosixAdvisoryLock&) = delete;
  PosixAdvisoryLock(PosixAdvisoryLock&& other) noexcept;
  PosixAdvisoryLock& operator=(PosixAdvisoryLock&& other) noexcept;

  [[nodiscard]] bool is_held() const noexcept;
  [[nodiscard]] common::Status close();

private:
  PosixAdvisoryLock(int descriptor, detail::PosixSyscalls& syscalls) noexcept;
  PosixAdvisoryLock(int descriptor, detail::PosixSyscalls& syscalls,
                    std::unique_ptr<detail::ProcessLockReservation> reservation) noexcept;
  void close_best_effort() noexcept;

  int descriptor_{-1};
  detail::PosixSyscalls* syscalls_{nullptr};
  std::unique_ptr<detail::ProcessLockReservation> reservation_;

  friend class PosixDirectory;
  friend class detail::PosixHandleFactory;
};

// PosixDirectory owns an opened directory descriptor. All entry operations require a single
// basename and execute relative to this descriptor, preventing path traversal and keeping rename
// source and destination in the same directory.
class PosixDirectory {
public:
  PosixDirectory() noexcept = default;
  ~PosixDirectory();

  PosixDirectory(const PosixDirectory&) = delete;
  PosixDirectory& operator=(const PosixDirectory&) = delete;
  PosixDirectory(PosixDirectory&& other) noexcept;
  PosixDirectory& operator=(PosixDirectory&& other) noexcept;

  [[nodiscard]] static common::Result<PosixDirectory> open(std::string_view path);
  [[nodiscard]] bool is_open() const noexcept;

  // Child-directory operations are descriptor-relative and never follow a final-component
  // symlink. Creation is exclusive; callers synchronize this directory after establishing a
  // durable child name.
  [[nodiscard]] common::Status create_exclusive_directory(std::string_view name,
                                                          std::uint16_t permissions = 0700U) const;
  [[nodiscard]] common::Result<PosixDirectory> open_directory(std::string_view name) const;

  [[nodiscard]] common::Result<PosixFile> open_regular_file(std::string_view name,
                                                            FileOpenMode mode) const;
  [[nodiscard]] common::Result<PosixFile>
  create_exclusive_regular_file(std::string_view name, std::uint16_t permissions = 0600U) const;
  [[nodiscard]] common::Result<PosixAdvisoryLock>
  acquire_exclusive_lock(std::string_view name, std::uint16_t permissions = 0600U) const;
  // Acquires the same lock without O_CREAT. Read-only diagnostic tools use this to avoid changing
  // the directory when LOCK is absent.
  [[nodiscard]] common::Result<PosixAdvisoryLock>
  acquire_existing_exclusive_lock(std::string_view name) const;

  // Returns an owning snapshot of every entry except . and .., sorted by bytewise name. Entry types
  // are obtained without following symlinks. Callers that require an authoritative mutation
  // decision must hold the directory's exclusive writer lock and reject concurrent out-of-band
  // modification.
  [[nodiscard]] common::Result<std::vector<DirectoryEntry>> list_entries() const;

  // Removes one directory-relative non-directory entry. Callers classify it without following
  // symlinks while holding their ownership lock and synchronize the directory after a cleanup
  // batch.
  [[nodiscard]] common::Status remove_file(std::string_view name) const;

  // The operation is atomic, confined to this directory, and never replaces an existing target.
  // A platform without an atomic no-replace primitive returns kNotSupported.
  [[nodiscard]] common::Status rename_no_replace(const RenameRequest& request) const;

  [[nodiscard]] common::Status sync() const;
  [[nodiscard]] common::Status close();

private:
  PosixDirectory(int descriptor, detail::PosixSyscalls& syscalls) noexcept;
  [[nodiscard]] static common::Result<PosixDirectory> open_with(std::string_view path,
                                                                detail::PosixSyscalls& syscalls);
  void close_best_effort() noexcept;

  int descriptor_{-1};
  detail::PosixSyscalls* syscalls_{nullptr};

  friend class detail::PosixHandleFactory;
};

} // namespace chronos::io

#endif // CHRONOS_IO_POSIX_IO_HPP_
