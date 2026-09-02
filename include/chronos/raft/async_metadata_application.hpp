#ifndef CHRONOS_RAFT_ASYNC_METADATA_APPLICATION_HPP_
#define CHRONOS_RAFT_ASYNC_METADATA_APPLICATION_HPP_

#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"

#include <memory>
#include <optional>
#include <span>

namespace chronos::raft {

struct AsyncRaftMetadataApplicationConfig {
  GroupId group_id;
  std::optional<MetadataSnapshotStorage> snapshot_storage{std::nullopt};
  MetadataLimits state_limits{};
  MetadataCommandCodecLimits codec_limits{};
  SchemaDefinitionCodecLimits schema_codec_limits{};
};

// Worker extension that recovers and applies the one authoritative metadata Raft group on the
// durable worker. Readers pin immutable owning catalog projections; the mutable state machine and
// synchronous runtime never escape that worker.
class AsyncRaftMetadataApplication final : public AsyncDurableRaftWorkerExtension {
public:
  AsyncRaftMetadataApplication(const AsyncRaftMetadataApplication&) = delete;
  AsyncRaftMetadataApplication& operator=(const AsyncRaftMetadataApplication&) = delete;
  ~AsyncRaftMetadataApplication() override;

  [[nodiscard]] static common::Result<std::shared_ptr<AsyncRaftMetadataApplication>>
  create(AsyncRaftMetadataApplicationConfig config);

  [[nodiscard]] const GroupId& group_id() const noexcept;
  [[nodiscard]] common::Result<std::shared_ptr<const MetadataCatalogSnapshot>>
  catalog_snapshot() const;
  [[nodiscard]] bool initialized() const;
  [[nodiscard]] bool failed() const;
  [[nodiscard]] common::Status failure_status() const;

  [[nodiscard]] common::Status initialize(DurableMultiRaftRuntime& runtime) override;
  [[nodiscard]] common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime& runtime,
                std::span<const DurableRaftRequest> requests) override;
  [[nodiscard]] common::Status
  complete_batch(DurableMultiRaftRuntime& runtime,
                 std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                 std::span<const DurableRaftResult> results) override;
  [[nodiscard]] common::Status shutdown(DurableMultiRaftRuntime& runtime) override;

private:
  class Impl;
  explicit AsyncRaftMetadataApplication(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_ASYNC_METADATA_APPLICATION_HPP_
