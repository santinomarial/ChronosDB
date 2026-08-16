#ifndef CHRONOS_LIVE_RAFT_SUBSCRIPTION_RECLAIMER_HPP_
#define CHRONOS_LIVE_RAFT_SUBSCRIPTION_RECLAIMER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/live/subscription_retention.hpp"
#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::live {

struct RaftSubscriptionReclamationSource {
  schema::TabletId tablet_id;
  raft::GroupId group_id;
  std::uint64_t placement_epoch{};
};

struct RaftSubscriptionSourceReclaimerConfig {
  std::vector<RaftSubscriptionReclamationSource> sources;
  raft::AsyncDurableMultiRaftRuntime* runtime{};
  const ingest::AsyncRaftTabletApplication* application{};
  std::size_t maximum_sources{1024U};
};

// Physical reclaimer for homogeneous Raft-backed subscription sources. Calls must not originate
// on the durable Raft worker because reclaim() waits for worker-owned observations and maintenance.
// Every requested logical index is first matched to an applied immutable tablet publication and a
// durable Raft application-snapshot boundary. Only after the complete batch passes does one
// node-wide checkpoint synchronize all resident group states and reclaim older shared segments.
class RaftSubscriptionSourceReclaimer final : public SubscriptionSourceReclaimer {
public:
  RaftSubscriptionSourceReclaimer() = delete;
  ~RaftSubscriptionSourceReclaimer() override;
  RaftSubscriptionSourceReclaimer(const RaftSubscriptionSourceReclaimer&) = delete;
  RaftSubscriptionSourceReclaimer& operator=(const RaftSubscriptionSourceReclaimer&) = delete;
  RaftSubscriptionSourceReclaimer(RaftSubscriptionSourceReclaimer&&) noexcept;
  RaftSubscriptionSourceReclaimer& operator=(RaftSubscriptionSourceReclaimer&&) noexcept;

  [[nodiscard]] static common::Result<RaftSubscriptionSourceReclaimer>
  create(RaftSubscriptionSourceReclaimerConfig config);

  [[nodiscard]] common::Status
  reclaim(std::span<const SubscriptionSourceReclamation> requests) override;

private:
  class Impl;
  explicit RaftSubscriptionSourceReclaimer(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_RAFT_SUBSCRIPTION_RECLAIMER_HPP_
