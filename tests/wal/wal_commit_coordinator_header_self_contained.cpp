#include "chronos/wal/wal_commit_coordinator.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::wal::WalCommitCoordinator>);
static_assert(!std::is_copy_constructible_v<chronos::wal::WalCommitCompletion>);
