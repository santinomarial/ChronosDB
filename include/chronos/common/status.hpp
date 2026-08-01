#ifndef CHRONOS_COMMON_STATUS_HPP_
#define CHRONOS_COMMON_STATUS_HPP_

#include <cstdint>
#include <string>
#include <string_view>

namespace chronos::common {

enum class StatusCode : std::uint8_t {
  kOk = 0,
  kCancelled,
  kInvalidArgument,
  kOutOfRange,
  kNotFound,
  kAlreadyExists,
  kCorruption,
  kIoError,
  kResourceExhausted,
  kUnavailable,
  kNotSupported,
  kInternal,
};

[[nodiscard]] std::string_view status_code_name(StatusCode code) noexcept;

class Status {
public:
  // Default construction deliberately represents success.
  Status() noexcept = default;
  Status(StatusCode code, std::string message);

  [[nodiscard]] static Status ok() noexcept;
  [[nodiscard]] bool is_ok() const noexcept;
  [[nodiscard]] StatusCode code() const noexcept;
  [[nodiscard]] const std::string& message() const noexcept;
  [[nodiscard]] std::string to_string() const;

  friend bool operator==(const Status&, const Status&) = default;

private:
  StatusCode code_{StatusCode::kOk};
  std::string message_;
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_STATUS_HPP_
