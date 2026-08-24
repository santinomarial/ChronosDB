#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <sys/types.h>
#include <unistd.h>

namespace {

[[nodiscard]] std::uint64_t configured_failure_ordinal() noexcept {
  const char* const value = std::getenv("CHRONOS_TEST_GETRANDOM_FAILURE_ORDINAL");
  if (value == nullptr || value[0] == '\0')
    return 0U;

  std::uint64_t ordinal{};
  for (const char* cursor = value; *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9')
      return 0U;
    const std::uint64_t digit = static_cast<std::uint64_t>(*cursor - '0');
    if (ordinal > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U)
      return 0U;
    ordinal = (ordinal * 10U) + digit;
  }
  return ordinal;
}

} // namespace

// GNU ld's --wrap contract requires these reserved external symbol spellings.
// NOLINTNEXTLINE(bugprone-reserved-identifier)
extern "C" ssize_t __real_getrandom(void* destination, std::size_t size, unsigned int flags);

// NOLINTNEXTLINE(bugprone-reserved-identifier)
extern "C" ssize_t __wrap_getrandom(void* destination, const std::size_t size,
                                    const unsigned int flags) {
  constexpr std::size_t kUuidBytes = 16U;
  const char* const trigger = std::getenv("CHRONOS_TEST_GETRANDOM_FAILURE_TRIGGER");
  const std::uint64_t failing_qualified_call = configured_failure_ordinal();
  if (trigger == nullptr || trigger[0] == '\0' || size != kUuidBytes || flags != 0U ||
      failing_qualified_call == 0U || ::access(trigger, F_OK) != 0) {
    return __real_getrandom(destination, size, flags);
  }

  static std::atomic<std::uint64_t> qualified_calls{};
  // The counter selects exactly one injected call and publishes no data, so relaxed ordering is
  // sufficient. Atomicity prevents multiple callers from claiming the same ordinal.
  const std::uint64_t call = qualified_calls.fetch_add(1U, std::memory_order_relaxed) + 1U;
  if (call != failing_qualified_call)
    return __real_getrandom(destination, size, flags);
  errno = EIO;
  return -1;
}
