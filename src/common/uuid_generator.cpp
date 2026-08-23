#include "chronos/common/uuid_generator.hpp"

#include "chronos/common/status.hpp"
#include "uuid_entropy_internal.hpp"

#include <cerrno>
#include <cstddef>
#include <span>
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

[[nodiscard]] Status entropy_error(const int error_number) {
  std::string message{"system entropy read failed: "};
  message.append(std::error_code{error_number, std::generic_category()}.message());
  message.append(" (errno ");
  message.append(std::to_string(error_number));
  message.push_back(')');
  return Status{StatusCode::kIoError, std::move(message)};
}

#if defined(__linux__)
class SystemUuidEntropyReader final : public detail::UuidEntropyReader {
public:
  [[nodiscard]] detail::UuidEntropyReadOutcome
  read(const std::span<std::byte> destination) noexcept override {
    const ssize_t result = ::getrandom(destination.data(), destination.size(), 0U);
    return {.byte_count = result, .error_number = result < 0 ? errno : 0};
  }
};
#endif

} // namespace

Result<Uuid::Bytes> detail::read_uuid_entropy_to_completion(UuidEntropyReader& reader) {
  Uuid::Bytes bytes{};
  std::size_t completed = 0U;
  while (completed < bytes.size()) {
    const std::span<std::byte> destination{bytes.data() + completed, bytes.size() - completed};
    const UuidEntropyReadOutcome outcome = reader.read(destination);
    if (outcome.byte_count > 0) {
      const auto progress = static_cast<std::size_t>(outcome.byte_count);
      if (progress > destination.size())
        return make_unexpected(entropy_error(EIO));
      completed += progress;
      continue;
    }
    if (outcome.byte_count == -1 && outcome.error_number == EINTR)
      continue;
    const int error_number =
        outcome.byte_count == -1 && outcome.error_number > 0 ? outcome.error_number : EIO;
    return make_unexpected(entropy_error(error_number));
  }
  return bytes;
}

Result<Uuid::Bytes> SystemUuidEntropySource::read() {
#if defined(__APPLE__)
  Uuid::Bytes bytes{};
  ::arc4random_buf(bytes.data(), bytes.size());
  return bytes;
#elif defined(__linux__)
  SystemUuidEntropyReader reader;
  return detail::read_uuid_entropy_to_completion(reader);
#endif
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
