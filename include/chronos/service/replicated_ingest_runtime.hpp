#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_RUNTIME_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/async_metadata_application.hpp"
#include "chronos/service/replicated_ingest_coordinator.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::service {

enum class ReplicatedIngestRuntimeShutdownStage : std::uint8_t {
  kCoordinatorReleased,
  kWorkerStopped,
};

// Borrowed only for one synchronous shutdown(observer) call. The coordinator-release callback
// precedes durable-worker drain; the worker-stopped callback follows log and extension shutdown.
class ReplicatedIngestRuntimeShutdownObserver {
public:
  virtual ~ReplicatedIngestRuntimeShutdownObserver() = default;
  virtual void on_shutdown_stage(ReplicatedIngestRuntimeShutdownStage stage) noexcept = 0;
};

struct ReplicatedIngestRuntimeConfig {
  raft::NodeId local_node_id{};
  raft::RaftPersistentLogConfig log;
  std::vector<raft::RaftGroupConfiguration> groups;
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  raft::AsyncRaftMetadataApplicationConfig metadata;
  raft::AsyncDurableMultiRaftLimits runtime_limits;
  ingest::AsyncRaftTabletApplicationLimits application_limits;
  ReplicatedIngestCoordinatorLimits coordinator_limits;
};

// Complete owning lifecycle for replicated ingest on one node. The durable runtime owns the
// extension set on its worker; this outer owner retains each direct child and destroys the
// coordinator before draining and shutting down that worker. Accessor pointers remain valid only
// while is_running() is true and must not outlive this object.
class ReplicatedIngestRuntime {
public:
  ReplicatedIngestRuntime() = delete;
  ~ReplicatedIngestRuntime();
  ReplicatedIngestRuntime(const ReplicatedIngestRuntime&) = delete;
  ReplicatedIngestRuntime& operator=(const ReplicatedIngestRuntime&) = delete;
  ReplicatedIngestRuntime(ReplicatedIngestRuntime&&) noexcept;
  ReplicatedIngestRuntime& operator=(ReplicatedIngestRuntime&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedIngestRuntime>
  create_new(ReplicatedIngestRuntimeConfig config);
  [[nodiscard]] static common::Result<ReplicatedIngestRuntime>
  open_existing(ReplicatedIngestRuntimeConfig config,
                raft::RaftPersistentLogOpenOptions open_options = {});

  [[nodiscard]] raft::AsyncDurableMultiRaftRuntime* runtime() noexcept;
  [[nodiscard]] ingest::AsyncRaftTabletApplication* tablet_application() noexcept;
  [[nodiscard]] raft::AsyncRaftMetadataApplication* metadata_application() noexcept;
  [[nodiscard]] ReplicatedIngestCoordinator* coordinator() noexcept;
  [[nodiscard]] bool is_running() const noexcept;
  [[nodiscard]] common::Status shutdown();
  [[nodiscard]] common::Status shutdown(ReplicatedIngestRuntimeShutdownObserver& observer);

private:
  [[nodiscard]] common::Status shutdown_with(ReplicatedIngestRuntimeShutdownObserver* observer);
  class Impl;
  explicit ReplicatedIngestRuntime(std::unique_ptr<Impl> impl) noexcept;
  [[nodiscard]] static common::Result<ReplicatedIngestRuntime>
  start(ReplicatedIngestRuntimeConfig config,
        std::optional<raft::RaftPersistentLogOpenOptions> open_options);
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_RUNTIME_HPP_
