#ifndef CHRONOS_WAL_WAL_REPLAY_SINK_HPP_
#define CHRONOS_WAL_WAL_REPLAY_SINK_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/types.hpp"

namespace chronos::wal {

// The payload view is valid only for the duration of the callback. preflight() must validate
// semantic support without publishing state. replay() is called only after the complete physical
// WAL and every preflight callback have succeeded. A replay failure requires the caller to discard
// its fresh/resettable target state.
struct WalReplayRecord {
  RecordHeader header;
  PhysicalWalPosition record_start;
  PhysicalWalPosition record_end;
  common::ByteView payload;
};

class WalReplaySink {
public:
  virtual ~WalReplaySink() = default;

  WalReplaySink() = default;
  WalReplaySink(const WalReplaySink&) = delete;
  WalReplaySink& operator=(const WalReplaySink&) = delete;
  WalReplaySink(WalReplaySink&&) = delete;
  WalReplaySink& operator=(WalReplaySink&&) = delete;

  [[nodiscard]] virtual common::Status preflight(const WalReplayRecord& record) = 0;
  [[nodiscard]] virtual common::Status replay(const WalReplayRecord& record) = 0;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_REPLAY_SINK_HPP_
