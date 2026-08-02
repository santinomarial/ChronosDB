#ifndef CHRONOS_WAL_WAL_LOG_ID_GENERATOR_HPP_
#define CHRONOS_WAL_WAL_LOG_ID_GENERATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/wal/types.hpp"

namespace chronos::wal {

class WalLogIdGenerator {
public:
  virtual ~WalLogIdGenerator() = default;

  WalLogIdGenerator() = default;
  WalLogIdGenerator(const WalLogIdGenerator&) = delete;
  WalLogIdGenerator& operator=(const WalLogIdGenerator&) = delete;
  WalLogIdGenerator(WalLogIdGenerator&&) = delete;
  WalLogIdGenerator& operator=(WalLogIdGenerator&&) = delete;

  // Implementations must return a nonzero identity suitable for an independently created history.
  [[nodiscard]] virtual common::Result<WalId> generate() = 0;
};

class SystemWalLogIdGenerator final : public WalLogIdGenerator {
public:
  [[nodiscard]] common::Result<WalId> generate() override;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_LOG_ID_GENERATOR_HPP_
