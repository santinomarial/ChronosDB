#ifndef CHRONOS_INGEST_COMMITTED_COLUMNAR_APPEND_HPP_
#define CHRONOS_INGEST_COMMITTED_COLUMNAR_APPEND_HPP_

#include "chronos/common/result.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstdint>
#include <memory>

namespace chronos::ingest {

enum class CommittedColumnarAppendKind : std::uint8_t {
  kApplied = 1,
  kMatchingRetry = 2,
};

struct CommittedColumnarAppendResult {
  CommittedColumnarAppendKind kind{CommittedColumnarAppendKind::kApplied};
  TabletSnapshot snapshot;
  std::shared_ptr<const ColumnarAppendRetryOutcome> outcome;
};

// Applies one already durable and committed COLUMNAR_APPEND command. The decoded view and schema
// must describe the same command. This boundary performs no WAL or Raft I/O; it atomically updates
// tablet rows and retry state at the supplied source-specific commit position. The legacy
// mark_wal_started() method names on reservations mean "the external commit gate has crossed" here.
[[nodiscard]] common::Result<CommittedColumnarAppendResult>
apply_committed_columnar_append(const DecodedColumnarAppendView& command,
                                std::shared_ptr<const schema::TableSchema> retained_schema,
                                head::HeadCommitPosition position, RetryDirectory& retry_directory,
                                TabletState& tablet, ColumnarAppendDecodeLimits limits = {});

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_COMMITTED_COLUMNAR_APPEND_HPP_
