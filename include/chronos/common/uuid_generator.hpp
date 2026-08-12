#ifndef CHRONOS_COMMON_UUID_GENERATOR_HPP_
#define CHRONOS_COMMON_UUID_GENERATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

namespace chronos::common {

class UuidGenerator {
public:
  UuidGenerator() = default;
  virtual ~UuidGenerator() = default;

  UuidGenerator(const UuidGenerator&) = delete;
  UuidGenerator& operator=(const UuidGenerator&) = delete;
  UuidGenerator(UuidGenerator&&) = delete;
  UuidGenerator& operator=(UuidGenerator&&) = delete;

  [[nodiscard]] virtual Result<Uuid> generate() = 0;
};

// Stateless operating-system entropy adapter. Each successful call returns a nonnil UUID; the
// bytes remain uninterpreted and are suitable for ChronosDB durable identities.
class SystemUuidGenerator final : public UuidGenerator {
public:
  [[nodiscard]] Result<Uuid> generate() override;
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_UUID_GENERATOR_HPP_
