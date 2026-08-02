#ifndef CHRONOS_IO_POSIX_SYSCALLS_HPP_
#define CHRONOS_IO_POSIX_SYSCALLS_HPP_

#include "chronos/common/result.hpp"
#include "chronos/io/posix_io.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

#include <sys/stat.h>
#include <sys/types.h>

namespace chronos::io::detail {

struct OpenAtRequest {
  int directory_descriptor;
  const char* name;
  int flags;
  mode_t permissions;
};

struct ReadAtRequest {
  int descriptor;
  void* destination;
  std::size_t size;
  off_t offset;
};

struct WriteAtRequest {
  int descriptor;
  const void* source;
  std::size_t size;
  off_t offset;
};

struct RenameAtRequest {
  int directory_descriptor;
  const char* old_name;
  const char* new_name;
};

// Raw syscall boundary used by production and deterministic failure-injection tests. It is not a
// filesystem abstraction: operations intentionally correspond one-for-one with the POSIX calls
// needed by the WAL durability protocol.
class PosixSyscalls {
public:
  virtual ~PosixSyscalls() = default;

  virtual int open_directory(const char* path, int flags) = 0;
  virtual int open_at(const OpenAtRequest& request) = 0;
  virtual ssize_t pread(const ReadAtRequest& request) = 0;
  virtual ssize_t pwrite(const WriteAtRequest& request) = 0;
  virtual int fstat(int descriptor, struct stat* metadata) = 0;
  virtual int ftruncate(int descriptor, off_t size) = 0;
  virtual int fdatasync(int descriptor) = 0;
  virtual int fsync(int descriptor) = 0;
  virtual int rename_no_replace(const RenameAtRequest& request) = 0;
  virtual int try_lock_exclusive(int descriptor) = 0;
  virtual int close(int descriptor) = 0;
};

[[nodiscard]] PosixSyscalls& system_posix_syscalls() noexcept;

class PosixHandleFactory {
public:
  [[nodiscard]] static common::Result<PosixDirectory>
  open_directory(std::string_view path, PosixSyscalls& syscalls) {
    return PosixDirectory::open_with(path, syscalls);
  }

  [[nodiscard]] static PosixFile file(int descriptor, PosixSyscalls& syscalls) noexcept {
    return PosixFile{descriptor, syscalls};
  }

  [[nodiscard]] static PosixDirectory directory(int descriptor,
                                                PosixSyscalls& syscalls) noexcept {
    return PosixDirectory{descriptor, syscalls};
  }

  [[nodiscard]] static PosixAdvisoryLock lock(int descriptor,
                                              PosixSyscalls& syscalls) noexcept {
    return PosixAdvisoryLock{descriptor, syscalls};
  }
};

} // namespace chronos::io::detail

#endif // CHRONOS_IO_POSIX_SYSCALLS_HPP_
