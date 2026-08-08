#include "chronos/common/status.hpp"

#include <utility>

namespace chronos::common {

std::string_view status_code_name(const StatusCode code) noexcept {
  switch (code) {
  case StatusCode::kOk:
    return "ok";
  case StatusCode::kCancelled:
    return "cancelled";
  case StatusCode::kInvalidArgument:
    return "invalid_argument";
  case StatusCode::kOutOfRange:
    return "out_of_range";
  case StatusCode::kNotFound:
    return "not_found";
  case StatusCode::kAlreadyExists:
    return "already_exists";
  case StatusCode::kCorruption:
    return "corruption";
  case StatusCode::kIoError:
    return "io_error";
  case StatusCode::kResourceExhausted:
    return "resource_exhausted";
  case StatusCode::kUnavailable:
    return "unavailable";
  case StatusCode::kNotSupported:
    return "not_supported";
  case StatusCode::kUnauthenticated:
    return "unauthenticated";
  case StatusCode::kInternal:
    return "internal";
  }
  return "unknown";
}

Status::Status(const StatusCode code, std::string message) : code_(code) {
  if (code_ == StatusCode::kOk) {
    return;
  }
  message_ = message.empty() ? std::string{status_code_name(code_)} : std::move(message);
}

Status Status::ok() noexcept {
  return {};
}

bool Status::is_ok() const noexcept {
  return code_ == StatusCode::kOk;
}

StatusCode Status::code() const noexcept {
  return code_;
}

const std::string& Status::message() const noexcept {
  return message_;
}

std::string Status::to_string() const {
  if (is_ok()) {
    return std::string{status_code_name(code_)};
  }
  std::string output{status_code_name(code_)};
  output.append(": ");
  output.append(message_);
  return output;
}

} // namespace chronos::common
