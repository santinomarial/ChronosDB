#ifndef CHRONOS_IO_POSIX_IO_HPP_
#define CHRONOS_IO_POSIX_IO_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace chronos::io {

namespace detail {
class PosixHandleFactory;
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
  [[nodiscard]] common::Status write_all_at(std::uint64_t offset,
                                            common::ByteView source) const;

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

// PosixAdvisoryLock exclusively owns the only descriptor ChronosDB may use for a WAL LOCK inode.
// The nonblocking whole-file fcntl lock remains held until close or destruction. As POSIX record
// locks are process-associated, independently opening and closing the same inode can release it.
class PosixAdvisoryLock {
public:
  PosixAdvisoryLock() noexcept = default;
  ~PosixAdvisoryLock();

  PosixAdvisoryLock(const PosixAdvisoryLock&) = delete;
  PosixAdvisoryLock& operator=(const PosixAdvisoryLock&) = delete;
  PosixAdvisoryLock(PosixAdvisoryLock&& other) noexcept;
  PosixAdvisoryLock& operator=(PosixAdvisoryLock&& other) noexcept;

  [[nodiscard]] bool is_held() const noexcept;
  [[nodiscard]] common::Status close();

private:
  PosixAdvisoryLock(int descriptor, detail::PosixSyscalls& syscalls) noexcept;
  void close_best_effort() noexcept;

  int descriptor_{-1};
  detail::PosixSyscalls* syscalls_{nullptr};

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

  [[nodiscard]] common::Result<PosixFile> open_regular_file(std::string_view name,
                                                            FileOpenMode mode) const;
  [[nodiscard]] common::Result<PosixFile>
  create_exclusive_regular_file(std::string_view name,
                                std::uint16_t permissions = 0600U) const;
  [[nodiscard]] common::Result<PosixAdvisoryLock>
  acquire_exclusive_lock(std::string_view name, std::uint16_t permissions = 0600U) const;

  // The operation is atomic, confined to this directory, and never replaces an existing target.
  // A platform without an atomic no-replace primitive returns kNotSupported.
  [[nodiscard]] common::Status rename_no_replace(const RenameRequest& request) const;

  [[nodiscard]] common::Status sync() const;
  [[nodiscard]] common::Status close();

private:
  PosixDirectory(int descriptor, detail::PosixSyscalls& syscalls) noexcept;
  [[nodiscard]] static common::Result<PosixDirectory>
  open_with(std::string_view path, detail::PosixSyscalls& syscalls);
  void close_best_effort() noexcept;

  int descriptor_{-1};
  detail::PosixSyscalls* syscalls_{nullptr};

  friend class detail::PosixHandleFactory;
};

} // namespace chronos::io

#endif // CHRONOS_IO_POSIX_IO_HPP_
