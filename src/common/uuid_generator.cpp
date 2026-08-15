#include "chronos/common/uuid_generator.hpp"

#include "chronos/common/status.hpp"

#include <cerrno>
#include <cstddef>
#include <string>
#include <system_error>
#include <utility>

#if defined(__APPLE__)
#include <cstdlib>
#elif defined(__linux__)
#include <sys/random.h>
#else
#error "SystemUuidGenerator requires Linux or macOS"
#endif

namespace chronos::common {
namespace {

#if defined(__linux__)
[[nodiscard]] Status entropy_error(const int error_number) {
  std::string message{"system entropy read failed: "};
  message.append(std::error_code{error_number, std::generic_category()}.message());
  message.append(" (errno ");
  message.append(std::to_string(error_number));
  message.push_back(')');
  return Status{StatusCode::kIoError, std::move(message)};
}
#endif

} // namespace

Result<Uuid::Bytes> SystemUuidEntropySource::read() {
  Uuid::Bytes bytes{};
#if defined(__APPLE__)
  ::arc4random_buf(bytes.data(), bytes.size());
#elif defined(__linux__)
  std::size_t completed = 0U;
  while (completed < bytes.size()) {
    const ssize_t result = ::getrandom(bytes.data() + completed, bytes.size() - completed, 0U);
    if (result > 0) {
      completed += static_cast<std::size_t>(result);
      continue;
    }
    if (result == -1 && errno == EINTR)
      continue;
    return make_unexpected(entropy_error(result == 0 ? EIO : errno));
  }
#endif
  return bytes;
}

SystemUuidEntropySource& system_uuid_entropy_source() noexcept {
  static SystemUuidEntropySource source;
  return source;
}

SystemUuidGenerator::SystemUuidGenerator() noexcept
    : SystemUuidGenerator(system_uuid_entropy_source()) {}

SystemUuidGenerator::SystemUuidGenerator(UuidEntropySource& entropy) noexcept
    : entropy_(&entropy) {}

Result<Uuid> SystemUuidGenerator::generate() {
  constexpr std::size_t kMaximumNilRetries = 8U;
  for (std::size_t attempt = 0U; attempt < kMaximumNilRetries; ++attempt) {
    auto bytes = entropy_->read();
    if (!bytes.has_value())
      return make_unexpected(bytes.error());
    Uuid id{*bytes};
    if (!id.is_nil())
      return id;
  }
  return make_unexpected(
      Status{StatusCode::kInternal, "system entropy repeatedly produced a nil UUID"});
}

} // namespace chronos::common
