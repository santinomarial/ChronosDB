#ifndef CHRONOS_WAL_WAL_COMMIT_COORDINATOR_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_COMMIT_COORDINATOR_INTERNAL_HPP_

#include "chronos/wal/wal_commit_coordinator.hpp"

namespace chronos::wal::detail {

class WalCommitCoordinatorTestAccess {
public:
  [[nodiscard]] static common::Result<WalCommitCoordinator>
  start(WalWriter writer, const WalCommitCoordinatorConfig& config,
        void (*worker_start_hook)(void*), void* worker_start_context) {
    return WalCommitCoordinator::start_with_worker_hook(std::move(writer), config,
                                                        worker_start_hook, worker_start_context);
  }
};

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_COMMIT_COORDINATOR_INTERNAL_HPP_
