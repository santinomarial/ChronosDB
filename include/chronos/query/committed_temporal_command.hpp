#ifndef CHRONOS_QUERY_COMMITTED_TEMPORAL_COMMAND_HPP_
#define CHRONOS_QUERY_COMMITTED_TEMPORAL_COMMAND_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstdint>

namespace chronos::query {

struct CommittedTemporalCommandResult {
  std::uint64_t system_commit_position{};
  std::uint32_t applied_mutation_count{};

  friend bool operator==(const CommittedTemporalCommandResult&,
                         const CommittedTemporalCommandResult&) = default;
};

// Applies one already durable and committed Temporal Mutation Command. This boundary performs no
// WAL I/O. It validates the embedded batch against the retained schema, copies every physical cell
// into scalar-owned state, binds source identity/order from the enclosing WAL record, and publishes
// the complete mutation batch atomically through the provider.
[[nodiscard]] common::Result<CommittedTemporalCommandResult> apply_committed_temporal_command(
    const DecodedTemporalCommandView& command, const schema::TableSchema& retained_schema,
    std::uint64_t system_commit_position, wal::WalId wal_id, TemporalSnapshotProvider& provider);

} // namespace chronos::query

#endif // CHRONOS_QUERY_COMMITTED_TEMPORAL_COMMAND_HPP_
