#include "chronos/io/posix_io.hpp"

#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <sys/stdio.h>
#elif defined(__linux__)
#include <linux/fs.h>
#include <sys/syscall.h>
#else
#error "chronos_io requires Linux or macOS"
#endif

namespace chronos::io {
namespace {

constexpr int kInvalidDescriptor = -1;

[[nodiscard]] common::Status invalid_argument(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status out_of_range(std::string message) {
  return common::Status{common::StatusCode::kOutOfRange, std::move(message)};
}

[[nodiscard]] common::Status closed_handle(std::string_view operation) {
  std::string message{operation};
  message.append(" requires an open POSIX handle");
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status errno_status(const char* operation, const int error_number,
                                          const std::size_t transferred = 0,
                                          const std::size_t requested = 0) {
  common::StatusCode code = common::StatusCode::kIoError;
  switch (error_number) {
  case ENOENT:
    code = common::StatusCode::kNotFound;
    break;
  case EEXIST:
    code = common::StatusCode::kAlreadyExists;
    break;
  case ENOSYS:
#if ENOTSUP != EOPNOTSUPP
  case ENOTSUP:
#endif
  case EOPNOTSUPP:
    code = common::StatusCode::kNotSupported;
    break;
  default:
    break;
  }

  std::string message{operation};
  message.append(" failed");
  if (requested != 0U) {
    message.append(" after ");
    message.append(std::to_string(transferred));
    message.append(" of ");
    message.append(std::to_string(requested));
    message.append(" bytes");
  }
  message.append(": ");
  message.append(std::error_code{error_number, std::generic_category()}.message());
  message.append(" (errno ");
  message.append(std::to_string(error_number));
  message.push_back(')');
  return common::Status{code, std::move(message)};
}

[[nodiscard]] common::Status validate_entry_name(const std::string_view name) {
  if (name.empty()) {
    return invalid_argument("directory entry name must not be empty");
  }
  if (name == "." || name == ".." || name.find('/') != std::string_view::npos ||
      name.find('\0') != std::string_view::npos) {
    return invalid_argument("directory entry must be a single non-dot basename");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::string> nul_terminated(const std::string_view text,
                                                         const char* description) {
  if (text.empty()) {
    std::string message{description};
    message.append(" must not be empty");
    return common::make_unexpected(invalid_argument(std::move(message)));
  }
  if (text.find('\0') != std::string_view::npos) {
    std::string message{description};
    message.append(" contains an embedded NUL byte");
    return common::make_unexpected(invalid_argument(std::move(message)));
  }
  return std::string{text};
}

[[nodiscard]] common::Result<off_t> checked_offset(const std::uint64_t offset) {
  constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
  if (offset > kMaximumOffset) {
    return common::make_unexpected(out_of_range("file offset cannot be represented by off_t"));
  }
  return static_cast<off_t>(offset);
}

[[nodiscard]] common::Result<off_t> checked_offset_with_progress(const std::uint64_t offset,
                                                                 const std::size_t progress) {
  constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
  if (offset > kMaximumOffset || progress > kMaximumOffset - offset) {
    return common::make_unexpected(out_of_range("file transfer offset exceeds off_t range"));
  }
  return static_cast<off_t>(offset + static_cast<std::uint64_t>(progress));
}

[[nodiscard]] common::Status validate_transfer_range(const std::uint64_t offset,
                                                     const std::size_t size) {
  const common::Result<off_t> converted_offset = checked_offset(offset);
  if (!converted_offset.has_value()) {
    return converted_offset.error();
  }
  if (size == 0U) {
    return common::Status::ok();
  }
  constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
  const std::size_t last_index = size - 1U;
  if (last_index > kMaximumOffset - offset) {
    return out_of_range("file transfer range exceeds off_t bounds");
  }
  return common::Status::ok();
}

[[nodiscard]] std::size_t syscall_chunk_size(const std::size_t remaining) noexcept {
  constexpr auto kMaximumTransfer = static_cast<std::size_t>(std::numeric_limits<ssize_t>::max());
  return std::min(remaining, kMaximumTransfer);
}

[[nodiscard]] common::Status validate_permissions(const std::uint16_t permissions) {
  if ((permissions & static_cast<std::uint16_t>(~0777U)) != 0U) {
    return invalid_argument("file permissions contain bits outside 0777");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_regular_file(detail::PosixSyscalls& syscalls,
                                                   const int descriptor) {
  struct stat metadata {};
  int result = 0;
  do {
    result = syscalls.fstat(descriptor, &metadata);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("fstat regular file", errno);
  }
  if (!S_ISREG(metadata.st_mode)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "opened directory entry is not a regular file"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_directory(detail::PosixSyscalls& syscalls,
                                                const int descriptor) {
  struct stat metadata {};
  int result = 0;
  do {
    result = syscalls.fstat(descriptor, &metadata);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("fstat directory", errno);
  }
  if (!S_ISDIR(metadata.st_mode)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "opened path is not a directory"};
  }
  return common::Status::ok();
}

void close_after_failed_open(detail::PosixSyscalls& syscalls, const int descriptor) noexcept {
  if (descriptor != kInvalidDescriptor) {
    static_cast<void>(syscalls.close(descriptor));
  }
}

} // namespace

namespace detail {
namespace {

class SystemPosixSyscalls final : public PosixSyscalls {
public:
  int open_directory(const char* const path, const int flags) noexcept override {
    return ::open(path, flags);
  }

  int open_at(const int directory_descriptor, const char* const name, const int flags,
              const mode_t permissions) noexcept override {
    return ::openat(directory_descriptor, name, flags, permissions);
  }

  ssize_t pread(const int descriptor, void* const destination, const std::size_t size,
                const off_t offset) noexcept override {
    return ::pread(descriptor, destination, size, offset);
  }

  ssize_t pwrite(const int descriptor, const void* const source, const std::size_t size,
                 const off_t offset) noexcept override {
    return ::pwrite(descriptor, source, size, offset);
  }

  int fstat(const int descriptor, struct stat* const metadata) noexcept override {
    return ::fstat(descriptor, metadata);
  }

  int ftruncate(const int descriptor, const off_t size) noexcept override {
    return ::ftruncate(descriptor, size);
  }

  int fdatasync(const int descriptor) noexcept override {
#if defined(__APPLE__)
    // Darwin does not expose fdatasync through its public libc headers. fsync is the documented
    // portable fallback and is at least as strong with respect to file metadata; this does not
    // advertise a macOS power-loss guarantee.
    return ::fsync(descriptor);
#elif defined(__linux__)
    return ::fdatasync(descriptor);
#endif
  }

  int fsync(const int descriptor) noexcept override {
    return ::fsync(descriptor);
  }

  int rename_no_replace(const int directory_descriptor, const char* const old_name,
                        const char* const new_name) noexcept override {
#if defined(__APPLE__)
    return ::renameatx_np(directory_descriptor, old_name, directory_descriptor, new_name,
                          RENAME_EXCL);
#elif defined(__linux__)
    return static_cast<int>(::syscall(SYS_renameat2, directory_descriptor, old_name,
                                      directory_descriptor, new_name, RENAME_NOREPLACE));
#endif
  }

  int try_lock_exclusive(const int descriptor) noexcept override {
    struct flock lock {};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    return ::fcntl(descriptor, F_SETLK, &lock);
  }

  int close(const int descriptor) noexcept override {
    return ::close(descriptor);
  }
};

} // namespace

PosixSyscalls& system_posix_syscalls() noexcept {
  static SystemPosixSyscalls syscalls;
  return syscalls;
}

} // namespace detail

PosixFile::PosixFile(const int descriptor, detail::PosixSyscalls& syscalls) noexcept
    : descriptor_(descriptor), syscalls_(&syscalls) {}

PosixFile::~PosixFile() {
  close_best_effort();
}

PosixFile::PosixFile(PosixFile&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, kInvalidDescriptor)),
      syscalls_(std::exchange(other.syscalls_, nullptr)) {}

PosixFile& PosixFile::operator=(PosixFile&& other) noexcept {
  if (this != &other) {
    close_best_effort();
    descriptor_ = std::exchange(other.descriptor_, kInvalidDescriptor);
    syscalls_ = std::exchange(other.syscalls_, nullptr);
  }
  return *this;
}

bool PosixFile::is_open() const noexcept {
  return descriptor_ != kInvalidDescriptor;
}

common::Result<std::size_t> PosixFile::read_at(const std::uint64_t offset,
                                              const common::MutableByteView destination) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("read_at"));
  }
  const common::Status range_status = validate_transfer_range(offset, destination.size());
  if (!range_status.is_ok()) {
    return common::make_unexpected(range_status);
  }

  std::size_t transferred = 0;
  while (transferred < destination.size()) {
    const common::Result<off_t> current_offset = checked_offset_with_progress(offset, transferred);
    if (!current_offset.has_value()) {
      return common::make_unexpected(current_offset.error());
    }
    const std::size_t request_size = syscall_chunk_size(destination.size() - transferred);
    const ssize_t result = syscalls_->pread(descriptor_, destination.data() + transferred,
                                            request_size, *current_offset);
    if (result > 0) {
      transferred += static_cast<std::size_t>(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    const int error_number = errno;
    if (error_number == EINTR) {
      continue;
    }
    return common::make_unexpected(
        errno_status("pread", error_number, transferred, destination.size()));
  }
  return transferred;
}

common::Status PosixFile::write_all_at(const std::uint64_t offset,
                                       const common::ByteView source) const {
  if (!is_open()) {
    return closed_handle("write_all_at");
  }
  const common::Status range_status = validate_transfer_range(offset, source.size());
  if (!range_status.is_ok()) {
    return range_status;
  }

  std::size_t transferred = 0;
  while (transferred < source.size()) {
    const common::Result<off_t> current_offset = checked_offset_with_progress(offset, transferred);
    if (!current_offset.has_value()) {
      return current_offset.error();
    }
    const std::size_t request_size = syscall_chunk_size(source.size() - transferred);
    const ssize_t result =
        syscalls_->pwrite(descriptor_, source.data() + transferred, request_size, *current_offset);
    if (result > 0) {
      transferred += static_cast<std::size_t>(result);
      continue;
    }
    if (result == 0) {
      return common::Status{common::StatusCode::kIoError,
                            "pwrite made no progress before the source was complete"};
    }
    const int error_number = errno;
    if (error_number == EINTR) {
      continue;
    }
    return errno_status("pwrite", error_number, transferred, source.size());
  }
  return common::Status::ok();
}

common::Result<std::uint64_t> PosixFile::size() const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("size"));
  }
  struct stat metadata {};
  int result = 0;
  do {
    result = syscalls_->fstat(descriptor_, &metadata);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return common::make_unexpected(errno_status("fstat file size", errno));
  }
  if (!S_ISREG(metadata.st_mode)) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                   "file handle is not a regular file"});
  }
  if (metadata.st_size < 0) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kIoError, "regular file reported a negative size"});
  }
  return static_cast<std::uint64_t>(metadata.st_size);
}

common::Status PosixFile::truncate(const std::uint64_t new_size) const {
  if (!is_open()) {
    return closed_handle("truncate");
  }
  const common::Result<off_t> converted_size = checked_offset(new_size);
  if (!converted_size.has_value()) {
    return converted_size.error();
  }
  const common::Result<std::uint64_t> current_size = size();
  if (!current_size.has_value()) {
    return current_size.error();
  }
  if (new_size > *current_size) {
    return invalid_argument("truncate cannot grow a regular file");
  }

  int result = 0;
  do {
    result = syscalls_->ftruncate(descriptor_, *converted_size);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("ftruncate", errno);
  }
  return common::Status::ok();
}

common::Status PosixFile::sync_data() const {
  if (!is_open()) {
    return closed_handle("sync_data");
  }
  int result = 0;
  do {
    result = syscalls_->fdatasync(descriptor_);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("fdatasync", errno);
  }
  return common::Status::ok();
}

common::Status PosixFile::sync_all() const {
  if (!is_open()) {
    return closed_handle("sync_all");
  }
  int result = 0;
  do {
    result = syscalls_->fsync(descriptor_);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("fsync regular file", errno);
  }
  return common::Status::ok();
}

common::Status PosixFile::close() {
  if (!is_open()) {
    return common::Status::ok();
  }
  const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
  detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
  if (syscalls->close(descriptor) == -1) {
    return errno_status("close regular file", errno);
  }
  return common::Status::ok();
}

void PosixFile::close_best_effort() noexcept {
  if (is_open()) {
    const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
    detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
    static_cast<void>(syscalls->close(descriptor));
  }
}

PosixAdvisoryLock::PosixAdvisoryLock(const int descriptor,
                                     detail::PosixSyscalls& syscalls) noexcept
    : descriptor_(descriptor), syscalls_(&syscalls) {}

PosixAdvisoryLock::~PosixAdvisoryLock() {
  close_best_effort();
}

PosixAdvisoryLock::PosixAdvisoryLock(PosixAdvisoryLock&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, kInvalidDescriptor)),
      syscalls_(std::exchange(other.syscalls_, nullptr)) {}

PosixAdvisoryLock& PosixAdvisoryLock::operator=(PosixAdvisoryLock&& other) noexcept {
  if (this != &other) {
    close_best_effort();
    descriptor_ = std::exchange(other.descriptor_, kInvalidDescriptor);
    syscalls_ = std::exchange(other.syscalls_, nullptr);
  }
  return *this;
}

bool PosixAdvisoryLock::is_held() const noexcept {
  return descriptor_ != kInvalidDescriptor;
}

common::Status PosixAdvisoryLock::close() {
  if (!is_held()) {
    return common::Status::ok();
  }
  const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
  detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
  if (syscalls->close(descriptor) == -1) {
    return errno_status("close advisory lock", errno);
  }
  return common::Status::ok();
}

void PosixAdvisoryLock::close_best_effort() noexcept {
  if (is_held()) {
    const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
    detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
    static_cast<void>(syscalls->close(descriptor));
  }
}

PosixDirectory::PosixDirectory(const int descriptor, detail::PosixSyscalls& syscalls) noexcept
    : descriptor_(descriptor), syscalls_(&syscalls) {}

PosixDirectory::~PosixDirectory() {
  close_best_effort();
}

PosixDirectory::PosixDirectory(PosixDirectory&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, kInvalidDescriptor)),
      syscalls_(std::exchange(other.syscalls_, nullptr)) {}

PosixDirectory& PosixDirectory::operator=(PosixDirectory&& other) noexcept {
  if (this != &other) {
    close_best_effort();
    descriptor_ = std::exchange(other.descriptor_, kInvalidDescriptor);
    syscalls_ = std::exchange(other.syscalls_, nullptr);
  }
  return *this;
}

common::Result<PosixDirectory> PosixDirectory::open(const std::string_view path) {
  return open_with(path, detail::system_posix_syscalls());
}

common::Result<PosixDirectory> PosixDirectory::open_with(const std::string_view path,
                                                        detail::PosixSyscalls& syscalls) {
  const common::Result<std::string> owned_path = nul_terminated(path, "directory path");
  if (!owned_path.has_value()) {
    return common::make_unexpected(owned_path.error());
  }
  constexpr int kOpenFlags = O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW;
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls.open_directory(owned_path->c_str(), kOpenFlags);
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    return common::make_unexpected(errno_status("open directory", errno));
  }

  const common::Status validation = validate_directory(syscalls, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(syscalls, descriptor);
    return common::make_unexpected(validation);
  }
  return PosixDirectory{descriptor, syscalls};
}

bool PosixDirectory::is_open() const noexcept {
  return descriptor_ != kInvalidDescriptor;
}

common::Result<PosixFile>
PosixDirectory::open_regular_file(const std::string_view name, const FileOpenMode mode) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("open_regular_file"));
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return common::make_unexpected(name_status);
  }
  const std::string owned_name{name};
  const int access = mode == FileOpenMode::kReadOnly ? O_RDONLY : O_RDWR;
  const int flags = access | O_CLOEXEC | O_NOFOLLOW;
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls_->open_at(descriptor_, owned_name.c_str(), flags, 0);
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    return common::make_unexpected(errno_status("openat regular file", errno));
  }

  const common::Status validation = validate_regular_file(*syscalls_, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(*syscalls_, descriptor);
    return common::make_unexpected(validation);
  }
  return PosixFile{descriptor, *syscalls_};
}

common::Result<PosixFile> PosixDirectory::create_exclusive_regular_file(
    const std::string_view name, const std::uint16_t permissions) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("create_exclusive_regular_file"));
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return common::make_unexpected(name_status);
  }
  const common::Status permission_status = validate_permissions(permissions);
  if (!permission_status.is_ok()) {
    return common::make_unexpected(permission_status);
  }

  const std::string owned_name{name};
  constexpr int kFlags = O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC | O_NOFOLLOW;
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls_->open_at(descriptor_, owned_name.c_str(), kFlags,
                                    static_cast<mode_t>(permissions));
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    return common::make_unexpected(errno_status("openat exclusive regular file", errno));
  }

  const common::Status validation = validate_regular_file(*syscalls_, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(*syscalls_, descriptor);
    return common::make_unexpected(validation);
  }
  return PosixFile{descriptor, *syscalls_};
}

common::Result<PosixAdvisoryLock>
PosixDirectory::acquire_exclusive_lock(const std::string_view name,
                                       const std::uint16_t permissions) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("acquire_exclusive_lock"));
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return common::make_unexpected(name_status);
  }
  const common::Status permission_status = validate_permissions(permissions);
  if (!permission_status.is_ok()) {
    return common::make_unexpected(permission_status);
  }

  const std::string owned_name{name};
  constexpr int kFlags = O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW;
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls_->open_at(descriptor_, owned_name.c_str(), kFlags,
                                    static_cast<mode_t>(permissions));
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    return common::make_unexpected(errno_status("openat advisory lock", errno));
  }

  const common::Status validation = validate_regular_file(*syscalls_, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(*syscalls_, descriptor);
    return common::make_unexpected(validation);
  }

  int lock_result = 0;
  do {
    lock_result = syscalls_->try_lock_exclusive(descriptor);
  } while (lock_result == -1 && errno == EINTR);
  if (lock_result == -1) {
    const int error_number = errno;
    close_after_failed_open(*syscalls_, descriptor);
    if (error_number == EACCES || error_number == EAGAIN) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "exclusive advisory lock is held by another process"});
    }
    return common::make_unexpected(errno_status("fcntl exclusive advisory lock", error_number));
  }
  return PosixAdvisoryLock{descriptor, *syscalls_};
}

common::Status PosixDirectory::rename_no_replace(const std::string_view old_name,
                                                 const std::string_view new_name) const {
  if (!is_open()) {
    return closed_handle("rename_no_replace");
  }
  const common::Status old_status = validate_entry_name(old_name);
  if (!old_status.is_ok()) {
    return old_status;
  }
  const common::Status new_status = validate_entry_name(new_name);
  if (!new_status.is_ok()) {
    return new_status;
  }
  const std::string owned_old_name{old_name};
  const std::string owned_new_name{new_name};

  int result = 0;
  do {
    result = syscalls_->rename_no_replace(descriptor_, owned_old_name.c_str(),
                                          owned_new_name.c_str());
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("same-directory no-replace rename", errno);
  }
  return common::Status::ok();
}

common::Status PosixDirectory::sync() const {
  if (!is_open()) {
    return closed_handle("directory sync");
  }
  int result = 0;
  do {
    result = syscalls_->fsync(descriptor_);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return errno_status("fsync directory", errno);
  }
  return common::Status::ok();
}

common::Status PosixDirectory::close() {
  if (!is_open()) {
    return common::Status::ok();
  }
  const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
  detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
  if (syscalls->close(descriptor) == -1) {
    return errno_status("close directory", errno);
  }
  return common::Status::ok();
}

void PosixDirectory::close_best_effort() noexcept {
  if (is_open()) {
    const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
    detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
    static_cast<void>(syscalls->close(descriptor));
  }
}

} // namespace chronos::io
