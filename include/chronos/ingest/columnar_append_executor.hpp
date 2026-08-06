#ifndef CHRONOS_INGEST_COLUMNAR_APPEND_EXECUTOR_HPP_
#define CHRONOS_INGEST_COLUMNAR_APPEND_EXECUTOR_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/wal/wal_commit_coordinator.hpp"

#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::ingest {

struct ColumnarAppendExecutionInput {
  ClientId client_id;
  ClientBatchId client_batch_id;
  std::shared_ptr<const columnar::OwnedColumnarBatch> batch;
  wal::WalDurabilityMode durability{wal::WalDurabilityMode::kAsync};
};

enum class ColumnarAppendExecutionKind : std::uint8_t {
  kApplied = 1,
  kMatchingRetry = 2,
};

struct ColumnarAppendExecutionResult {
  ColumnarAppendExecutionKind kind{ColumnarAppendExecutionKind::kApplied};
  std::shared_ptr<const ColumnarAppendRetryOutcome> outcome;
  wal::WalDurabilityMode requested_durability{wal::WalDurabilityMode::kAsync};

  // Present only for kApplied. A matching retry performs no WAL operation and returns the exact
  // previously published logical outcome instead.
  std::optional<wal::WalCommitResult> wal_commit;
};

// Executes one already routed, schema-resolved append on the tablet's single shard owner. The call
// blocks for the WAL coordinator completion, publishes tablet state, and commits the exact outcome
// into the global retry directory before returning success. It performs no routing, authorization,
// event-time policy, per-row deduplication, schema activation, replay, or transport acknowledgment.
[[nodiscard]] common::Result<ColumnarAppendExecutionResult>
execute_columnar_append(const ColumnarAppendExecutionInput& input, RetryDirectory& retry_directory,
                        TabletState& tablet, wal::WalCommitCoordinator& wal_coordinator);

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_EXECUTOR_HPP_
