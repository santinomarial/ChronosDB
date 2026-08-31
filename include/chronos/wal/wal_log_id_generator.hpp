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

  // Returns one candidate for one independently created history. Implementations must return a
  // nonzero identity. WalWriter validates that contract but does not retry a rejected candidate:
  // its locked empty directory contains no prior WAL identity against which a collision can be
  // detected, while entropy-level nil retries belong inside the production generator.
  [[nodiscard]] virtual common::Result<WalId> generate() = 0;
};

class SystemWalLogIdGenerator final : public WalLogIdGenerator {
public:
  [[nodiscard]] common::Result<WalId> generate() override;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_LOG_ID_GENERATOR_HPP_
