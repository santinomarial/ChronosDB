#include "chronos/common/rotating_log_sink.hpp"

#include "chronos/common/checked_math.hpp"

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/file.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::common {
namespace {

[[nodiscard]] Status invalid(std::string message) {
  return {StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] Status io_error(const std::string_view operation, const int error_number) {
  std::string message{operation};
  message.append(" failed (errno ");
  message.append(std::to_string(error_number));
  message.push_back(')');
  return {StatusCode::kIoError, std::move(message)};
}

void close_descriptor(const int descriptor) noexcept {
  if (descriptor >= 0)
    static_cast<void>(::close(descriptor));
}

[[nodiscard]] std::string archive_path(const std::string& path, const std::size_t index) {
  return path + '.' + std::to_string(index);
}

} // namespace

class RotatingJsonLogSink::Impl final {
public:
  explicit Impl(RotatingJsonLogSinkConfig config) : config_(std::move(config)) {}

  ~Impl() {
    if (output_ != nullptr)
      static_cast<void>(std::fclose(output_));
    close_descriptor(lock_descriptor_);
  }

  [[nodiscard]] Status initialize() {
    Status locked = acquire_lock();
    if (!locked.is_ok())
      return locked;
    return open_active();
  }

  [[nodiscard]] Status write(const LogRecord& record) {
    auto encoded = encode_json_log(record);
    if (!encoded.has_value())
      return encoded.error();
    if (encoded->size() >= config_.maximum_file_bytes)
      return {StatusCode::kResourceExhausted, "encoded log line exceeds the file-size bound"};
    try {
      encoded->push_back('\n');
    } catch (const std::bad_alloc&) {
      return {StatusCode::kResourceExhausted, "encoded log line allocation failed"};
    } catch (const std::length_error&) {
      return invalid("encoded log line exceeds process limits");
    }

    try {
      const std::lock_guard lock{mutex_};
      try {
        if (!terminal_error_.is_ok())
          return terminal_error_;
        const auto next_size =
            checked_add(current_size_, static_cast<std::uint64_t>(encoded->size()));
        if (!next_size.has_value())
          return fail({StatusCode::kResourceExhausted, "log file size overflows"});
        if (current_size_ != 0U && *next_size > config_.maximum_file_bytes) {
          const Status rotated = rotate();
          if (!rotated.is_ok())
            return fail(rotated);
          if (output_ == nullptr) {
            return fail({StatusCode::kInternal, "log rotation completed without an active stream"});
          }
        }
        const std::size_t written = std::fwrite(encoded->data(), 1U, encoded->size(), output_);
        if (written != encoded->size() || std::fflush(output_) != 0)
          return fail(io_error("rotating log write", errno == 0 ? EIO : errno));
        current_size_ += static_cast<std::uint64_t>(encoded->size());
        return Status::ok();
      } catch (const std::bad_alloc&) {
        return fail({StatusCode::kResourceExhausted, "log rotation allocation failed"});
      } catch (const std::length_error&) {
        return fail(invalid("log rotation path exceeds process limits"));
      }
    } catch (const std::system_error&) {
      return {StatusCode::kInternal, "rotating log synchronization failed"};
    }
  }

private:
  [[nodiscard]] Status fail(Status status) {
    terminal_error_ = status;
    return status;
  }

  [[nodiscard]] Status acquire_lock() {
    std::string lock_path;
    try {
      lock_path = config_.path + ".lock";
    } catch (const std::bad_alloc&) {
      return {StatusCode::kResourceExhausted, "log lock path allocation failed"};
    }
    lock_descriptor_ = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (lock_descriptor_ < 0)
      return io_error("log lock open", errno);
    struct stat metadata {};
    if (::fstat(lock_descriptor_, &metadata) != 0)
      return io_error("log lock stat", errno);
    if (!S_ISREG(metadata.st_mode))
      return invalid("log lock path is not a regular file");
    if (::flock(lock_descriptor_, LOCK_EX | LOCK_NB) != 0)
      return io_error("log lock acquisition", errno);
    return Status::ok();
  }

  [[nodiscard]] Status open_active() {
    const int descriptor =
        ::open(config_.path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (descriptor < 0)
      return io_error("active log open", errno);
    struct stat metadata {};
    if (::fstat(descriptor, &metadata) != 0) {
      const int error_number = errno;
      close_descriptor(descriptor);
      return io_error("active log stat", error_number);
    }
    if (!S_ISREG(metadata.st_mode) || metadata.st_size < 0) {
      close_descriptor(descriptor);
      return invalid("active log path is not a regular file");
    }
    output_ = ::fdopen(descriptor, "a");
    if (output_ == nullptr) {
      const int error_number = errno;
      close_descriptor(descriptor);
      return io_error("active log stream open", error_number);
    }
    current_size_ = static_cast<std::uint64_t>(metadata.st_size);
    return Status::ok();
  }

  [[nodiscard]] Status rotate() {
    if (std::fclose(output_) != 0) {
      output_ = nullptr;
      return io_error("active log close before rotation", errno);
    }
    output_ = nullptr;
    if (config_.retained_file_count == 0U) {
      if (::unlink(config_.path.c_str()) != 0 && errno != ENOENT)
        return io_error("active log removal", errno);
    } else {
      const std::string oldest = archive_path(config_.path, config_.retained_file_count);
      if (::unlink(oldest.c_str()) != 0 && errno != ENOENT)
        return io_error("oldest log removal", errno);
      for (std::size_t index = config_.retained_file_count; index > 1U; --index) {
        const std::string source = archive_path(config_.path, index - 1U);
        const std::string destination = archive_path(config_.path, index);
        if (::rename(source.c_str(), destination.c_str()) != 0 && errno != ENOENT)
          return io_error("log archive rename", errno);
      }
      const std::string newest = archive_path(config_.path, 1U);
      if (::rename(config_.path.c_str(), newest.c_str()) != 0)
        return io_error("active log rotation", errno);
    }
    return open_active();
  }

  RotatingJsonLogSinkConfig config_;
  int lock_descriptor_{-1};
  std::FILE* output_{};
  std::uint64_t current_size_{};
  std::mutex mutex_;
  Status terminal_error_;
};

Result<std::unique_ptr<RotatingJsonLogSink>>
RotatingJsonLogSink::open(RotatingJsonLogSinkConfig config) {
  if (config.path.empty())
    return make_unexpected(invalid("log path is empty"));
  if (config.maximum_file_bytes == 0U)
    return make_unexpected(invalid("maximum log file bytes must be nonzero"));
  if (config.retained_file_count > kMaximumRetainedLogFiles)
    return make_unexpected(invalid("retained log file count exceeds the bound"));
  try {
    auto implementation = std::make_unique<Impl>(std::move(config));
    const Status initialized = implementation->initialize();
    if (!initialized.is_ok())
      return make_unexpected(initialized);
    return std::unique_ptr<RotatingJsonLogSink>{new RotatingJsonLogSink{std::move(implementation)}};
  } catch (const std::bad_alloc&) {
    return make_unexpected(Status{StatusCode::kResourceExhausted, "log sink allocation failed"});
  } catch (const std::length_error&) {
    return make_unexpected(invalid("log sink path exceeds process limits"));
  }
}

RotatingJsonLogSink::RotatingJsonLogSink(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

RotatingJsonLogSink::~RotatingJsonLogSink() = default;

Status RotatingJsonLogSink::write(const LogRecord& record) {
  return implementation_->write(record);
}

} // namespace chronos::common
