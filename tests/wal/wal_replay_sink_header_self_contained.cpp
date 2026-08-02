#include "chronos/wal/wal_replay_sink.hpp"

#include <type_traits>

static_assert(std::is_abstract_v<chronos::wal::WalReplaySink>);
