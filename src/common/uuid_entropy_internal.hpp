#ifndef CHRONOS_COMMON_UUID_ENTROPY_INTERNAL_HPP_
#define CHRONOS_COMMON_UUID_ENTROPY_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

#include <cstddef>
#include <span>

namespace chronos::common::detail {

struct UuidEntropyReadOutcome {
  std::ptrdiff_t byte_count{};
  int error_number{};
};

// Narrow getrandom-shaped boundary for deterministic qualification of the Linux completion loop.
// Implementations must return -1 with an error number, zero, or a positive count no larger than
// destination.size(). Positive bytes must be initialized before return.
class UuidEntropyReader {
public:
  UuidEntropyReader() = default;
  virtual ~UuidEntropyReader() = default;

  UuidEntropyReader(const UuidEntropyReader&) = delete;
  UuidEntropyReader& operator=(const UuidEntropyReader&) = delete;
  UuidEntropyReader(UuidEntropyReader&&) = delete;
  UuidEntropyReader& operator=(UuidEntropyReader&&) = delete;

  [[nodiscard]] virtual UuidEntropyReadOutcome read(std::span<std::byte> destination) noexcept = 0;
};

[[nodiscard]] Result<Uuid::Bytes> read_uuid_entropy_to_completion(UuidEntropyReader& reader);

} // namespace chronos::common::detail

#endif // CHRONOS_COMMON_UUID_ENTROPY_INTERNAL_HPP_
