#include "chronos/io/posix_io.hpp"

#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <dirent.h>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <system_error>
#include <unistd.h>
#include <utility>

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

struct TransferProgress {
  std::size_t transferred;
  std::size_t requested;
};

[[nodiscard]] common::Status errno_status(const char* operation, const int error_number,
                                          const TransferProgress* const progress = nullptr) {
  common::StatusCode code = common::StatusCode::kIoError;
  switch (error_number) {
  case ENOENT:
    code = common::StatusCode::kNotFound;
    break;
  case EEXIST:
    code = common::StatusCode::kAlreadyExists;
    break;
  case EDQUOT:
  case EMFILE:
  case ENFILE:
  case ENOSPC:
    code = common::StatusCode::kResourceExhausted;
    break;
  case EFBIG:
  case EOVERFLOW:
    code = common::StatusCode::kOutOfRange;
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
  if (progress != nullptr) {
    message.append(" after ");
    message.append(std::to_string(progress->transferred));
    message.append(" of ");
    message.append(std::to_string(progress->requested));
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

struct TransferRange {
  std::uint64_t offset;
  std::size_t size;
};

[[nodiscard]] common::Status validate_transfer_range(const TransferRange& range) {
  const common::Result<off_t> converted_offset = checked_offset(range.offset);
  if (!converted_offset.has_value()) {
    return converted_offset.error();
  }
  if (range.size == 0U) {
    return common::Status::ok();
  }
  constexpr auto kMaximumOffset = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());
  const std::size_t last_index = range.size - 1U;
  if (last_index > kMaximumOffset - range.offset) {
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
    return common::Status{common::StatusCode::kInvalidArgument, "opened path is not a directory"};
  }
  return common::Status::ok();
}

void close_after_failed_open(detail::PosixSyscalls& syscalls, const int descriptor) {
  if (descriptor != kInvalidDescriptor) {
    static_cast<void>(syscalls.close(descriptor));
  }
}

void ignore_destructor_close_exception() noexcept {
  // A test double may allocate while recording close. Destruction has no error channel and the
  // production syscall implementation is noexcept, so an injected exception is intentionally
  // discarded after ownership has already been invalidated.
}

} // namespace

namespace detail {

struct LockIdentity {
  dev_t device;
  ino_t inode;
  std::string name;

  friend bool operator<(const LockIdentity& left, const LockIdentity& right) noexcept {
    if (left.device != right.device) {
      return left.device < right.device;
    }
    if (left.inode != right.inode) {
      return left.inode < right.inode;
    }
    return left.name < right.name;
  }
};

[[nodiscard]] std::mutex& process_lock_mutex() {
  static std::mutex mutex;
  return mutex;
}

[[nodiscard]] std::set<LockIdentity>& process_lock_identities() {
  static std::set<LockIdentity> identities;
  return identities;
}

[[nodiscard]] pid_t& process_lock_pid() {
  static pid_t process_id = ::getpid();
  return process_id;
}

void refresh_process_lock_registry_after_fork() {
  const pid_t current_process = ::getpid();
  if (process_lock_pid() != current_process) {
    process_lock_identities().clear();
    process_lock_pid() = current_process;
  }
}

class ProcessLockReservation {
public:
  explicit ProcessLockReservation(LockIdentity identity) : identity_(std::move(identity)) {}

  ~ProcessLockReservation() {
    if (!armed_) {
      return;
    }
    const std::scoped_lock lock{process_lock_mutex()};
    refresh_process_lock_registry_after_fork();
    process_lock_identities().erase(identity_);
  }

  ProcessLockReservation(const ProcessLockReservation&) = delete;
  ProcessLockReservation& operator=(const ProcessLockReservation&) = delete;
  ProcessLockReservation(ProcessLockReservation&&) = delete;
  ProcessLockReservation& operator=(ProcessLockReservation&&) = delete;

  void arm() noexcept {
    armed_ = true;
  }

private:
  LockIdentity identity_;
  bool armed_{false};
};

[[nodiscard]] common::Result<std::unique_ptr<ProcessLockReservation>>
reserve_process_lock(PosixSyscalls& syscalls, const int directory_descriptor,
                     const std::string_view name) {
  struct stat metadata {};
  int result = 0;
  do {
    result = syscalls.fstat(directory_descriptor, &metadata);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    return common::make_unexpected(errno_status("fstat advisory-lock directory", errno));
  }

  const LockIdentity identity{
      .device = metadata.st_dev, .inode = metadata.st_ino, .name = std::string{name}};
  const std::scoped_lock lock{process_lock_mutex()};
  refresh_process_lock_registry_after_fork();
  if (process_lock_identities().contains(identity)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable, "exclusive advisory lock is held by this process"});
  }
  std::unique_ptr<ProcessLockReservation> reservation =
      std::make_unique<ProcessLockReservation>(identity);
  process_lock_identities().insert(identity);
  reservation->arm();
  return reservation;
}

namespace {

class SystemPosixSyscalls final : public PosixSyscalls {
public:
  int open_directory(const char* const path, const int flags) noexcept override {
    return ::open(path, flags);
  }

  int open_at(const OpenAtRequest& request) noexcept override {
    return ::openat(request.directory_descriptor, request.name, request.flags, request.permissions);
  }

  ssize_t pread(const ReadAtRequest& request) noexcept override {
    return ::pread(request.descriptor, request.destination, request.size, request.offset);
  }

  ssize_t pwrite(const WriteAtRequest& request) noexcept override {
    return ::pwrite(request.descriptor, request.source, request.size, request.offset);
  }

  int fstat(const int descriptor, struct stat* const metadata) noexcept override {
    return ::fstat(descriptor, metadata);
  }

  int ftruncate(const TruncateRequest& request) noexcept override {
    return ::ftruncate(request.descriptor, request.size);
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

  int rename_no_replace(const RenameAtRequest& request) noexcept override {
#if defined(__APPLE__)
    return ::renameatx_np(request.directory_descriptor, request.old_name,
                          request.directory_descriptor, request.new_name, RENAME_EXCL);
#elif defined(__linux__)
    return static_cast<int>(::syscall(SYS_renameat2, request.directory_descriptor, request.old_name,
                                      request.directory_descriptor, request.new_name,
                                      RENAME_NOREPLACE));
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

  int list_directory_entries(const int descriptor, std::vector<DirectoryEntry>& entries) override {
    int stream_descriptor = -1;
    do {
      stream_descriptor = ::openat(descriptor, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    } while (stream_descriptor == -1 && errno == EINTR);
    if (stream_descriptor == -1) {
      return -1;
    }

    DIR* const stream = ::fdopendir(stream_descriptor);
    if (stream == nullptr) {
      const int error_number = errno;
      static_cast<void>(::close(stream_descriptor));
      errno = error_number;
      return -1;
    }
    std::unique_ptr<DIR, decltype(&::closedir)> owned_stream{stream, &::closedir};

    while (true) {
      errno = 0;
      dirent* const entry = ::readdir(owned_stream.get());
      if (entry == nullptr) {
        if (errno == EINTR) {
          continue;
        }
        if (errno != 0) {
          const int error_number = errno;
          owned_stream.reset();
          errno = error_number;
          return -1;
        }
        break;
      }
      const std::string_view name{entry->d_name};
      if (name == "." || name == "..") {
        continue;
      }

      struct stat metadata {};
      int stat_result = 0;
      do {
        stat_result = ::fstatat(descriptor, entry->d_name, &metadata, AT_SYMLINK_NOFOLLOW);
      } while (stat_result == -1 && errno == EINTR);
      if (stat_result == -1) {
        const int error_number = errno;
        owned_stream.reset();
        errno = error_number;
        return -1;
      }

      DirectoryEntryType type = DirectoryEntryType::kOther;
      if (S_ISREG(metadata.st_mode)) {
        type = DirectoryEntryType::kRegularFile;
      } else if (S_ISDIR(metadata.st_mode)) {
        type = DirectoryEntryType::kDirectory;
      } else if (S_ISLNK(metadata.st_mode)) {
        type = DirectoryEntryType::kSymlink;
      }
      entries.push_back(DirectoryEntry{.name = std::string{name}, .type = type});
    }

    if (::closedir(owned_stream.release()) == -1) {
      return -1;
    }
    return 0;
  }

  int unlink_at(const int directory_descriptor, const char* const name) override {
    return ::unlinkat(directory_descriptor, name, 0);
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
  common::Status range_status =
      validate_transfer_range(TransferRange{.offset = offset, .size = destination.size()});
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
    const ssize_t result = syscalls_->pread(detail::ReadAtRequest{
        .descriptor = descriptor_,
        .destination = destination.data() + transferred,
        .size = request_size,
        .offset = *current_offset,
    });
    if (result > 0) {
      const auto completed = static_cast<std::size_t>(result);
      if (completed > request_size) {
        return common::make_unexpected(common::Status{common::StatusCode::kIoError,
                                                      "pread reported more bytes than requested"});
      }
      transferred += completed;
      continue;
    }
    if (result == 0) {
      break;
    }
    const int error_number = errno;
    if (error_number == EINTR) {
      continue;
    }
    const TransferProgress progress{.transferred = transferred, .requested = destination.size()};
    return common::make_unexpected(errno_status("pread", error_number, &progress));
  }
  return transferred;
}

common::Status PosixFile::write_all_at(const std::uint64_t offset,
                                       const common::ByteView source) const {
  if (!is_open()) {
    return closed_handle("write_all_at");
  }
  common::Status range_status =
      validate_transfer_range(TransferRange{.offset = offset, .size = source.size()});
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
    const ssize_t result = syscalls_->pwrite(detail::WriteAtRequest{
        .descriptor = descriptor_,
        .source = source.data() + transferred,
        .size = request_size,
        .offset = *current_offset,
    });
    if (result > 0) {
      const auto completed = static_cast<std::size_t>(result);
      if (completed > request_size) {
        return common::Status{common::StatusCode::kIoError,
                              "pwrite reported more bytes than requested"};
      }
      transferred += completed;
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
    const TransferProgress progress{.transferred = transferred, .requested = source.size()};
    return errno_status("pwrite", error_number, &progress);
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
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "file handle is not a regular file"});
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
    result = syscalls_->ftruncate(
        detail::TruncateRequest{.descriptor = descriptor_, .size = *converted_size});
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
    try {
      static_cast<void>(syscalls->close(descriptor));
    } catch (...) {
      ignore_destructor_close_exception();
    }
  }
}

PosixAdvisoryLock::PosixAdvisoryLock() noexcept = default;

PosixAdvisoryLock::PosixAdvisoryLock(const int descriptor, detail::PosixSyscalls& syscalls) noexcept
    : descriptor_(descriptor), syscalls_(&syscalls) {}

PosixAdvisoryLock::PosixAdvisoryLock(
    const int descriptor, detail::PosixSyscalls& syscalls,
    std::unique_ptr<detail::ProcessLockReservation> reservation) noexcept
    : descriptor_(descriptor), syscalls_(&syscalls), reservation_(std::move(reservation)) {}

PosixAdvisoryLock::~PosixAdvisoryLock() {
  close_best_effort();
}

PosixAdvisoryLock::PosixAdvisoryLock(PosixAdvisoryLock&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, kInvalidDescriptor)),
      syscalls_(std::exchange(other.syscalls_, nullptr)),
      reservation_(std::move(other.reservation_)) {}

PosixAdvisoryLock& PosixAdvisoryLock::operator=(PosixAdvisoryLock&& other) noexcept {
  if (this != &other) {
    close_best_effort();
    descriptor_ = std::exchange(other.descriptor_, kInvalidDescriptor);
    syscalls_ = std::exchange(other.syscalls_, nullptr);
    reservation_ = std::move(other.reservation_);
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
  const int result = syscalls->close(descriptor);
  reservation_.reset();
  if (result == -1) {
    return errno_status("close advisory lock", errno);
  }
  return common::Status::ok();
}

void PosixAdvisoryLock::close_best_effort() noexcept {
  if (is_held()) {
    const int descriptor = std::exchange(descriptor_, kInvalidDescriptor);
    detail::PosixSyscalls* const syscalls = std::exchange(syscalls_, nullptr);
    try {
      static_cast<void>(syscalls->close(descriptor));
    } catch (...) {
      ignore_destructor_close_exception();
    }
    reservation_.reset();
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

common::Result<PosixFile> PosixDirectory::open_regular_file(const std::string_view name,
                                                            const FileOpenMode mode) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("open_regular_file"));
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return common::make_unexpected(name_status);
  }
  const std::string owned_name{name};
  int access = 0;
  switch (mode) {
  case FileOpenMode::kReadOnly:
    access = O_RDONLY;
    break;
  case FileOpenMode::kReadWrite:
    access = O_RDWR;
    break;
  default:
    return common::make_unexpected(invalid_argument("unknown regular-file open mode"));
  }
  const int flags = access | O_CLOEXEC | O_NOFOLLOW;
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls_->open_at(detail::OpenAtRequest{
        .directory_descriptor = descriptor_,
        .name = owned_name.c_str(),
        .flags = flags,
        .permissions = 0,
    });
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

common::Result<PosixFile>
PosixDirectory::create_exclusive_regular_file(const std::string_view name,
                                              const std::uint16_t permissions) const {
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
    descriptor = syscalls_->open_at(detail::OpenAtRequest{
        .directory_descriptor = descriptor_,
        .name = owned_name.c_str(),
        .flags = kFlags,
        .permissions = static_cast<mode_t>(permissions),
    });
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
  common::Result<std::unique_ptr<detail::ProcessLockReservation>> reservation =
      detail::reserve_process_lock(*syscalls_, descriptor_, owned_name);
  if (!reservation.has_value()) {
    return common::make_unexpected(reservation.error());
  }
  int descriptor = kInvalidDescriptor;
  do {
    descriptor = syscalls_->open_at(detail::OpenAtRequest{
        .directory_descriptor = descriptor_,
        .name = owned_name.c_str(),
        .flags = kFlags,
        .permissions = static_cast<mode_t>(permissions),
    });
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    const int error_number = errno;
    reservation->reset();
    return common::make_unexpected(errno_status("openat advisory lock", error_number));
  }

  const common::Status validation = validate_regular_file(*syscalls_, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(*syscalls_, descriptor);
    reservation->reset();
    return common::make_unexpected(validation);
  }

  int lock_result = 0;
  do {
    lock_result = syscalls_->try_lock_exclusive(descriptor);
  } while (lock_result == -1 && errno == EINTR);
  if (lock_result == -1) {
    const int error_number = errno;
    close_after_failed_open(*syscalls_, descriptor);
    reservation->reset();
    if (error_number == EACCES || error_number == EAGAIN) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "exclusive advisory lock is held by another process"});
    }
    return common::make_unexpected(errno_status("fcntl exclusive advisory lock", error_number));
  }
  return PosixAdvisoryLock{descriptor, *syscalls_, std::move(*reservation)};
}

common::Result<PosixAdvisoryLock>
PosixDirectory::acquire_existing_exclusive_lock(const std::string_view name) const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("acquire_existing_exclusive_lock"));
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return common::make_unexpected(name_status);
  }

  const std::string owned_name{name};
  common::Result<std::unique_ptr<detail::ProcessLockReservation>> reservation =
      detail::reserve_process_lock(*syscalls_, descriptor_, owned_name);
  if (!reservation.has_value()) {
    return common::make_unexpected(reservation.error());
  }
  int descriptor = kInvalidDescriptor;
  constexpr int kFlags = O_RDWR | O_CLOEXEC | O_NOFOLLOW;
  do {
    descriptor = syscalls_->open_at(detail::OpenAtRequest{
        .directory_descriptor = descriptor_,
        .name = owned_name.c_str(),
        .flags = kFlags,
        .permissions = 0,
    });
  } while (descriptor == kInvalidDescriptor && errno == EINTR);
  if (descriptor == kInvalidDescriptor) {
    const int error_number = errno;
    reservation->reset();
    return common::make_unexpected(errno_status("openat existing advisory lock", error_number));
  }

  const common::Status validation = validate_regular_file(*syscalls_, descriptor);
  if (!validation.is_ok()) {
    close_after_failed_open(*syscalls_, descriptor);
    reservation->reset();
    return common::make_unexpected(validation);
  }

  int lock_result = 0;
  do {
    lock_result = syscalls_->try_lock_exclusive(descriptor);
  } while (lock_result == -1 && errno == EINTR);
  if (lock_result == -1) {
    const int error_number = errno;
    close_after_failed_open(*syscalls_, descriptor);
    reservation->reset();
    if (error_number == EACCES || error_number == EAGAIN) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kUnavailable, "exclusive advisory lock is held by another process"});
    }
    return common::make_unexpected(errno_status("fcntl exclusive advisory lock", error_number));
  }
  return PosixAdvisoryLock{descriptor, *syscalls_, std::move(*reservation)};
}

common::Result<std::vector<DirectoryEntry>> PosixDirectory::list_entries() const {
  if (!is_open()) {
    return common::make_unexpected(closed_handle("list_entries"));
  }
  std::vector<DirectoryEntry> entries;
  try {
    if (syscalls_->list_directory_entries(descriptor_, entries) == -1) {
      return common::make_unexpected(errno_status("list directory entries", errno));
    }
    std::sort(entries.begin(), entries.end(), [](const auto& left, const auto& right) {
      return std::lexicographical_compare(
          left.name.begin(), left.name.end(), right.name.begin(), right.name.end(),
          [](const char left_byte, const char right_byte) {
            return static_cast<unsigned char>(left_byte) < static_cast<unsigned char>(right_byte);
          });
    });
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cannot allocate directory-entry snapshot"});
  }
  return entries;
}

common::Status PosixDirectory::remove_file(const std::string_view name) const {
  if (!is_open()) {
    return closed_handle("remove_file");
  }
  const common::Status name_status = validate_entry_name(name);
  if (!name_status.is_ok()) {
    return name_status;
  }
  const std::string owned_name{name};
  int result = 0;
  do {
    result = syscalls_->unlink_at(descriptor_, owned_name.c_str());
  } while (result == -1 && errno == EINTR);
  return result == 0 ? common::Status::ok() : errno_status("unlinkat WAL entry", errno);
}

common::Status PosixDirectory::rename_no_replace(const RenameRequest& request) const {
  if (!is_open()) {
    return closed_handle("rename_no_replace");
  }
  common::Status old_status = validate_entry_name(request.old_name);
  if (!old_status.is_ok()) {
    return old_status;
  }
  common::Status new_status = validate_entry_name(request.new_name);
  if (!new_status.is_ok()) {
    return new_status;
  }
  const std::string owned_old_name{request.old_name};
  const std::string owned_new_name{request.new_name};

  int result = 0;
  do {
    result = syscalls_->rename_no_replace(detail::RenameAtRequest{
        .directory_descriptor = descriptor_,
        .old_name = owned_old_name.c_str(),
        .new_name = owned_new_name.c_str(),
    });
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    if (errno == EINVAL) {
      return common::Status{common::StatusCode::kNotSupported,
                            "filesystem does not support atomic no-replace rename"};
    }
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
    try {
      static_cast<void>(syscalls->close(descriptor));
    } catch (...) {
      ignore_destructor_close_exception();
    }
  }
}

} // namespace chronos::io
