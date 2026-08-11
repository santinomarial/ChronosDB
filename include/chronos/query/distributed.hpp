#ifndef CHRONOS_QUERY_DISTRIBUTED_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
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
  std::uint64_t known_leader_commit_position{};
};

struct DistributedReadPolicy {
  DistributedReadConsistency consistency{DistributedReadConsistency::kLeaderLinearizable};
  std::optional<std::uint64_t> maximum_staleness_positions;
};

struct DistributedReadAdmission {
  schema::TabletId tablet_id;
  std::uint64_t serving_node{};
  std::uint64_t applied_position{};
  std::uint64_t observed_leader_commit_position{};
  std::optional<raft::ReadBarrier> linearizable_barrier;
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
  DistributedReadPolicy read_policy;
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

[[nodiscard]] common::Result<DistributedAggregatePlan>
plan_distributed_aggregation(common::Uuid query_id, const std::vector<DistributedTablet>& tablets,
                             const DistributedEventTimePredicate& predicate,
                             DistributedReadPolicy read_policy, DistributedPlanLimits limits = {});

[[nodiscard]] common::Status
validate_distributed_read_admission(const DistributedAggregatePlan& plan,
                                    const DistributedReadAdmission& admission);

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

namespace distributed_format {
inline constexpr std::uint16_t kExchangeMessageMajor = 1U;
inline constexpr std::uint16_t kExchangeMessageMinor = 0U;
inline constexpr std::size_t kExchangeMessageLength = 128U;
} // namespace distributed_format

class EncodedExchangeMessage {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedExchangeMessage(
      std::array<std::byte, distributed_format::kExchangeMessageLength> bytes) noexcept;

  std::array<std::byte, distributed_format::kExchangeMessageLength> bytes_{};

  friend common::Result<EncodedExchangeMessage> encode_exchange_message(const ExchangeMessage&);
};

// Canonical fixed-width worker/coordinator frame. Integrity and version fields are checked before
// any aggregate state is interpreted; exact decoding rejects truncation and trailing bytes.
[[nodiscard]] common::Result<EncodedExchangeMessage>
encode_exchange_message(const ExchangeMessage& message);
[[nodiscard]] common::Result<ExchangeMessage> decode_exchange_message_exact(common::ByteView bytes);

struct ExchangeFrameReadStep {
  std::size_t consumed_bytes{};
  std::optional<ExchangeMessage> message;
};

// Constant-storage stream decoder. One call consumes at most the bytes needed for one frame, so a
// caller can retain and resubmit any coalesced suffix. A decode failure is sticky.
class ExchangeFrameReader {
public:
  ExchangeFrameReader() = default;
  ExchangeFrameReader(const ExchangeFrameReader&) = delete;
  ExchangeFrameReader& operator=(const ExchangeFrameReader&) = delete;
  ExchangeFrameReader(ExchangeFrameReader&&) = delete;
  ExchangeFrameReader& operator=(ExchangeFrameReader&&) = delete;

  [[nodiscard]] common::Result<ExchangeFrameReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, distributed_format::kExchangeMessageLength> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<common::Status> failure_;
};

// Owns one encoded frame and exposes only its unwritten suffix. Short-write acknowledgement is
// checked before cursor mutation.
class ExchangeFrameWriteCursor {
public:
  ExchangeFrameWriteCursor() = delete;
  ExchangeFrameWriteCursor(const ExchangeFrameWriteCursor&) = delete;
  ExchangeFrameWriteCursor& operator=(const ExchangeFrameWriteCursor&) = delete;
  ExchangeFrameWriteCursor(ExchangeFrameWriteCursor&& other) noexcept;
  ExchangeFrameWriteCursor& operator=(ExchangeFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<ExchangeFrameWriteCursor>
  create(const ExchangeMessage& message);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit ExchangeFrameWriteCursor(EncodedExchangeMessage encoded) noexcept;

  EncodedExchangeMessage encoded_;
  std::size_t written_bytes_{};
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

inline constexpr std::size_t kMaximumDistributedCoordinatorMessages = 65'536U;

struct DistributedCoordinatorLimits {
  std::size_t maximum_messages_per_fragment{1024U};
  std::size_t maximum_total_messages{kMaximumDistributedCoordinatorMessages};
};

// Single-owner coordinator state machine. The owner must serialize accept, worker_failed, and
// finish; retained messages provide a bounded exact retry window until coordinator destruction.
class DistributedAggregateCoordinator {
public:
  DistributedAggregateCoordinator() = delete;
  ~DistributedAggregateCoordinator();
  DistributedAggregateCoordinator(const DistributedAggregateCoordinator&) = delete;
  DistributedAggregateCoordinator& operator=(const DistributedAggregateCoordinator&) = delete;
  DistributedAggregateCoordinator(DistributedAggregateCoordinator&&) noexcept;
  DistributedAggregateCoordinator& operator=(DistributedAggregateCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<DistributedAggregateCoordinator>
  create(DistributedAggregatePlan plan, std::vector<DistributedReadAdmission> admissions,
         DistributedCoordinatorLimits limits = {});
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
