#ifndef CHRONOS_QUERY_TEMPORAL_COMMAND_EXECUTOR_HPP_
#define CHRONOS_QUERY_TEMPORAL_COMMAND_EXECUTOR_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/committed_temporal_command.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/wal/wal_commit_coordinator.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

struct TemporalCommandExecutionInput {
  std::shared_ptr<const columnar::OwnedColumnarBatch> batch;
  std::vector<TemporalMutationDescriptor> mutations;
  std::int64_t system_commit_time_ns{};
  wal::WalDurabilityMode durability{wal::WalDurabilityMode::kAsync};
};

struct TemporalCommandExecutionResult {
  wal::WalCommitResult wal_commit;
  CommittedTemporalCommandResult application;
};

// Executes one schema-resolved temporal mutation batch on its single table/shard writer. It fully
// materializes and validates the next transition before WAL admission, waits for the requested
// durability boundary, then publishes using the actual WAL identity and record sequence. The
// caller must serialize this function with every other write to provider. Any failure after WAL
// admission fails provider closed because recovery must reconcile whether the command is durable.
[[nodiscard]] common::Result<TemporalCommandExecutionResult>
execute_temporal_command(const TemporalCommandExecutionInput& input,
                         TemporalSnapshotProvider& provider,
                         wal::WalCommitCoordinator& wal_coordinator);

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_COMMAND_EXECUTOR_HPP_
