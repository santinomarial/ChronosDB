#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_OPERATION_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_OPERATION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/raft/async_durable_runtime.hpp"

#include <memory>
#include <optional>
#include <vector>

namespace chronos::service {

struct ReplicatedIngestResult {
  network::IngestOutcome outcome{network::IngestOutcome::kApplied};
  std::uint32_t applied_row_count{};
  raft::QuorumSyncReceipt receipt;
};

// Move-only nonblocking owner for one protocol-v2 replicated append. The embedding polls from its
// service/result loop; destruction cancels receipt waiting but cannot undo an admitted Raft entry.
class ReplicatedIngestOperation {
public:
  ReplicatedIngestOperation() = delete;
  ~ReplicatedIngestOperation();
  ReplicatedIngestOperation(const ReplicatedIngestOperation&) = delete;
  ReplicatedIngestOperation& operator=(const ReplicatedIngestOperation&) = delete;
  ReplicatedIngestOperation(ReplicatedIngestOperation&&) noexcept;
  ReplicatedIngestOperation& operator=(ReplicatedIngestOperation&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedIngestOperation>
  submit(raft::GroupId group_id, raft::Term required_leader_term,
         std::vector<std::byte> encoded_columnar_append,
         raft::AsyncDurableMultiRaftRuntime& runtime,
         ingest::AsyncRaftTabletApplication& application,
         ingest::ColumnarAppendDecodeLimits decode_limits = {});

  [[nodiscard]] common::Result<std::optional<ReplicatedIngestResult>> poll();

private:
  class Impl;
  explicit ReplicatedIngestOperation(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_replicated_ingest_acknowledgement(const ReplicatedIngestResult& result);

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_OPERATION_HPP_
