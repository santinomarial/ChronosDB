#include "chronos/wal/wal_log_id_generator.hpp"

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
#error "SystemWalLogIdGenerator requires Linux or macOS"
#endif

namespace chronos::wal {
namespace {

#if defined(__linux__)
[[nodiscard]] common::Status entropy_error(const int error_number) {
  std::string message{"system entropy read failed: "};
  message.append(std::error_code{error_number, std::generic_category()}.message());
  message.append(" (errno ");
  message.append(std::to_string(error_number));
  message.push_back(')');
  return common::Status{common::StatusCode::kIoError, std::move(message)};
}
#endif

} // namespace

common::Result<WalId> SystemWalLogIdGenerator::generate() {
  constexpr std::size_t kMaximumZeroRetries = 8;
  for (std::size_t attempt = 0; attempt < kMaximumZeroRetries; ++attempt) {
    WalId id;
#if defined(__APPLE__)
    ::arc4random_buf(id.bytes.data(), id.bytes.size());
#elif defined(__linux__)
    std::size_t completed = 0;
    while (completed < id.bytes.size()) {
      const ssize_t result =
          ::getrandom(id.bytes.data() + completed, id.bytes.size() - completed, 0U);
      if (result > 0) {
        completed += static_cast<std::size_t>(result);
        continue;
      }
      if (result == -1 && errno == EINTR) {
        continue;
      }
      return common::make_unexpected(entropy_error(result == 0 ? EIO : errno));
    }
#endif
    if (id.is_valid()) {
      return id;
    }
  }
  return common::make_unexpected(common::Status{
      common::StatusCode::kInternal, "system entropy repeatedly produced an invalid zero WAL ID"});
}

} // namespace chronos::wal
