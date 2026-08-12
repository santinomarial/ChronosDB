#ifndef CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
#define CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/common/uuid_generator.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/single_node_database.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::service {

struct NativeProtocolServiceLimits {
  network::ProtocolLimits protocol{};
  ingest::ColumnarAppendDecodeLimits columnar_append{};
  query::SqlParserLimits sql_parser{};
  query::SqlBinderLimits sql_binder{};
  query::SqlInsertBinderLimits sql_insert{};
  query::PhysicalSelectLoweringLimits physical_lowering{};
  query::SnapshotTabletPipelineLimits tablet_pipeline{};
  query::TabletStatePipelineLimits replicated_tablet_pipeline{};
  columnar::ColumnarBatchLimits insert_batch{};
  network::QueryResultLimits query_result{};
  std::size_t maximum_query_memory_bytes{64U * 1024U * 1024U};
  std::uint64_t maximum_result_rows{1'048'576U};
  std::size_t maximum_result_batches{1024U};
  std::size_t maximum_response_payload_bytes{64U * 1024U * 1024U};
  std::uint64_t ddl_retry_retention_positions{1'000'000U};
};

struct NativeProtocolResponseSequence {
  std::vector<network::NetworkTask> responses;
  std::uint64_t result_rows{};
  std::size_t payload_bytes{};
};

using NativeIdentityGenerator = common::UuidGenerator;

// Thread-affine synchronous translation between an already accepted native request and one
// database owner. The replicated constructor serves current local-applied SELECT only; replicated
// ingest remains asynchronous and CREATE/INSERT/ASOF fail explicitly. Returned tasks retain the
// connection/principal routing envelope.
// Ingest returns one terminal response; query returns a bounded result sequence ending in QUERY_END
// or one terminal ERROR. Queueing and socket backpressure remain owned by the reactor worker.
class NativeProtocolService {
public:
  explicit NativeProtocolService(SingleNodeDatabase& database,
                                 NativeProtocolServiceLimits limits = {}) noexcept;
  NativeProtocolService(SingleNodeDatabase& database, NativeIdentityGenerator& identities,
                        NativeProtocolServiceLimits limits = {}) noexcept;
  explicit NativeProtocolService(ReplicatedIngestDatabase& database,
                                 NativeProtocolServiceLimits limits = {}) noexcept;

  [[nodiscard]] common::Result<network::NetworkTask> execute_ingest(network::NetworkTask request);
  [[nodiscard]] common::Result<NativeProtocolResponseSequence>
  execute_query(network::NetworkTask request);

private:
  SingleNodeDatabase* database_{};
  ReplicatedIngestDatabase* replicated_database_{};
  NativeIdentityGenerator* identities_{};
  NativeProtocolServiceLimits limits_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
