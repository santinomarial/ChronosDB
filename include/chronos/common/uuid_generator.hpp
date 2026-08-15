#ifndef CHRONOS_COMMON_UUID_GENERATOR_HPP_
#define CHRONOS_COMMON_UUID_GENERATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

namespace chronos::common {

class UuidEntropySource {
public:
  UuidEntropySource() = default;
  virtual ~UuidEntropySource() = default;

  UuidEntropySource(const UuidEntropySource&) = delete;
  UuidEntropySource& operator=(const UuidEntropySource&) = delete;
  UuidEntropySource(UuidEntropySource&&) = delete;
  UuidEntropySource& operator=(UuidEntropySource&&) = delete;

  [[nodiscard]] virtual Result<Uuid::Bytes> read() = 0;
};

// Thread-safe operating-system entropy adapter. Linux reads getrandom(2) to completion and macOS
// uses arc4random_buf(3). The returned bytes have no UUID version/variant interpretation.
class SystemUuidEntropySource final : public UuidEntropySource {
public:
  [[nodiscard]] Result<Uuid::Bytes> read() override;
};

// Returns the stateless system adapter with process lifetime.
[[nodiscard]] SystemUuidEntropySource& system_uuid_entropy_source() noexcept;

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

// OS-backed UUID generator. Each successful call returns a nonnil UUID; the bytes remain
// uninterpreted and are suitable for ChronosDB durable identities.
class SystemUuidGenerator final : public UuidGenerator {
public:
  SystemUuidGenerator() noexcept;

  // Borrows an entropy source that must outlive this generator and every concurrent generate call.
  // A mutable injected source supplies its own synchronization when calls can overlap.
  explicit SystemUuidGenerator(UuidEntropySource& entropy) noexcept;

  [[nodiscard]] Result<Uuid> generate() override;

private:
  UuidEntropySource* entropy_;
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_UUID_GENERATOR_HPP_
