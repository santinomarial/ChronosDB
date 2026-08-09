#ifndef CHRONOS_QUERY_DISTRIBUTED_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

enum class DistributedReadConsistency : std::uint8_t {
  kLeaderLinearizable = 1,
  kFollowerBoundedStale = 2,
  kLocalEventual = 3,
};

struct DistributedTablet {
  schema::TabletId tablet_id;
  std::int64_t minimum_event_time{};
  std::int64_t maximum_event_time{};
  std::uint64_t leader_node{};
  std::uint64_t local_applied_position{};
};

struct DistributedEventTimePredicate {
  std::optional<std::int64_t> lower_inclusive;
  std::optional<std::int64_t> upper_exclusive;
};

struct DistributedPlanLimits {
  std::size_t maximum_tablets{4096U};
  std::size_t maximum_fragments{4096U};
};

struct DistributedAggregatePlan {
  common::Uuid query_id;
  DistributedReadConsistency consistency{DistributedReadConsistency::kLeaderLinearizable};
  std::vector<DistributedTablet> fragments;
  bool scan_pushdown{true};
  bool filter_pushdown{true};
  bool projection_pushdown{true};
  bool partial_aggregate_pushdown{true};
};

[[nodiscard]] common::Result<DistributedAggregatePlan> plan_distributed_aggregation(
    common::Uuid query_id, const std::vector<DistributedTablet>& tablets,
    const DistributedEventTimePredicate& predicate,
    DistributedReadConsistency consistency = DistributedReadConsistency::kLeaderLinearizable,
    DistributedPlanLimits limits = {});

struct MergeableAggregateState {
  std::uint64_t count{};
  double sum{};
  std::optional<double> minimum;
  std::optional<double> maximum;
  double mean{};
  double m2{};

  [[nodiscard]] common::Status add(double value);
  [[nodiscard]] common::Status merge(const MergeableAggregateState& other);
  [[nodiscard]] std::optional<double> variance_population() const noexcept;
};

struct ExchangeMessage {
  common::Uuid query_id;
  schema::TabletId tablet_id;
  std::uint64_t sequence{};
  MergeableAggregateState partial;
  bool terminal{};
};

struct ExchangeLimits {
  std::size_t maximum_messages{1024U};
  std::size_t maximum_bytes{4U * 1024U * 1024U};
};

// Mutex-protected bounded MPMC handoff for coordinator/worker fragments. Saturation is explicit;
// cancellation clears queued work and wakes ownership through ordinary method return, not a
// detached producer.
class BoundedExchange {
public:
  BoundedExchange() = delete;
  ~BoundedExchange();
  BoundedExchange(const BoundedExchange&) = delete;
  BoundedExchange& operator=(const BoundedExchange&) = delete;
  BoundedExchange(BoundedExchange&&) noexcept;
  BoundedExchange& operator=(BoundedExchange&&) noexcept;

  [[nodiscard]] static common::Result<BoundedExchange> create(common::Uuid query_id,
                                                              ExchangeLimits limits = {});
  [[nodiscard]] common::Status push(ExchangeMessage message);
  [[nodiscard]] common::Result<std::optional<ExchangeMessage>> try_pop();
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] bool cancelled() const noexcept;
  [[nodiscard]] std::size_t queued_messages() const noexcept;

private:
  class Impl;
  explicit BoundedExchange(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

class DistributedAggregateCoordinator {
public:
  DistributedAggregateCoordinator() = delete;
  ~DistributedAggregateCoordinator();
  DistributedAggregateCoordinator(const DistributedAggregateCoordinator&) = delete;
  DistributedAggregateCoordinator& operator=(const DistributedAggregateCoordinator&) = delete;
  DistributedAggregateCoordinator(DistributedAggregateCoordinator&&) noexcept;
  DistributedAggregateCoordinator& operator=(DistributedAggregateCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<DistributedAggregateCoordinator>
  create(DistributedAggregatePlan plan);
  [[nodiscard]] common::Status accept(const ExchangeMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Result<MergeableAggregateState> finish() const;

private:
  class Impl;
  explicit DistributedAggregateCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_HPP_
