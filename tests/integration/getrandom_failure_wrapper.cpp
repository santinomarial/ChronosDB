#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <sys/types.h>
#include <unistd.h>

// GNU ld's --wrap contract requires these reserved external symbol spellings.
// NOLINTNEXTLINE(bugprone-reserved-identifier)
extern "C" ssize_t __real_getrandom(void* destination, std::size_t size, unsigned int flags);

// NOLINTNEXTLINE(bugprone-reserved-identifier)
extern "C" ssize_t __wrap_getrandom(void* destination, const std::size_t size,
                                    const unsigned int flags) {
  constexpr std::size_t kUuidBytes = 16U;
  constexpr std::uint64_t kFailingQualifiedCall = 5U;
  const char* const trigger = std::getenv("CHRONOS_TEST_GETRANDOM_FAILURE_TRIGGER");
  if (trigger == nullptr || trigger[0] == '\0' || size != kUuidBytes || flags != 0U ||
      ::access(trigger, F_OK) != 0) {
    return __real_getrandom(destination, size, flags);
  }

  static std::atomic<std::uint64_t> qualified_calls{};
  // The counter selects exactly one injected call and publishes no data, so relaxed ordering is
  // sufficient. Atomicity prevents multiple callers from claiming the same ordinal.
  const std::uint64_t call = qualified_calls.fetch_add(1U, std::memory_order_relaxed) + 1U;
  if (call != kFailingQualifiedCall)
    return __real_getrandom(destination, size, flags);
  errno = EIO;
  return -1;
}
