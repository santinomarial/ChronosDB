#include "chronos/query/distributed.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <set>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status validate_read_policy(const DistributedReadPolicy& policy) {
  switch (policy.consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
    return policy.maximum_staleness_positions.has_value()
               ? invalid("linearizable reads cannot declare staleness")
               : common::Status::ok();
  case DistributedReadConsistency::kFollowerBoundedStale:
    return policy.maximum_staleness_positions.has_value()
               ? common::Status::ok()
               : invalid("bounded-stale reads require an explicit position bound");
  case DistributedReadConsistency::kLocalEventual:
    return policy.maximum_staleness_positions.has_value()
               ? invalid("local-eventual reads cannot declare staleness")
               : common::Status::ok();
  }
  return invalid("distributed read consistency mode is invalid");
}

[[nodiscard]] bool intersects(const DistributedTablet& tablet,
                              const DistributedEventTimePredicate& predicate) noexcept {
  if (predicate.lower_inclusive.has_value() &&
      tablet.maximum_event_time < *predicate.lower_inclusive) {
    return false;
  }
  if (predicate.upper_exclusive.has_value() &&
      tablet.minimum_event_time >= *predicate.upper_exclusive) {
    return false;
  }
  return true;
}

inline constexpr std::size_t kExchangeMessageCharge = sizeof(ExchangeMessage);
inline constexpr std::array<std::byte, 8U> kExchangeMessageMagic{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'X'},
    std::byte{'C'}, std::byte{'H'}, std::byte{'G'}, std::byte{'1'}};
inline constexpr std::uint32_t kTerminalFlag = 1U << 0U;
inline constexpr std::uint32_t kMinimumFlag = 1U << 1U;
inline constexpr std::uint32_t kMaximumFlag = 1U << 2U;
inline constexpr std::uint32_t kKnownExchangeFlags = kTerminalFlag | kMinimumFlag | kMaximumFlag;

[[nodiscard]] common::Status validate_exchange_message(const ExchangeMessage& message) {
  if (message.query_id.is_nil() || message.tablet_id.uuid().is_nil() || message.sequence == 0U)
    return invalid("exchange message identity or sequence is invalid");
  if (message.partial.minimum.has_value() != message.partial.maximum.has_value())
    return invalid("exchange aggregate extrema presence differs");
  if (message.partial.count == 0U) {
    if (message.partial.minimum.has_value() ||
        std::bit_cast<std::uint64_t>(message.partial.sum) != 0U ||
        std::bit_cast<std::uint64_t>(message.partial.mean) != 0U ||
        std::bit_cast<std::uint64_t>(message.partial.m2) != 0U) {
      return invalid("empty exchange aggregate state is not canonical");
    }
  } else if (!message.partial.minimum.has_value()) {
    return invalid("nonempty exchange aggregate state has no extrema");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status corruption(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] bool same_float_bits(const double left, const double right) noexcept {
  return std::bit_cast<std::uint64_t>(left) == std::bit_cast<std::uint64_t>(right);
}

[[nodiscard]] bool same_optional_float_bits(const std::optional<double>& left,
                                            const std::optional<double>& right) noexcept {
  return left.has_value() == right.has_value() &&
         (!left.has_value() || same_float_bits(*left, *right));
}

[[nodiscard]] bool same_exchange_message(const ExchangeMessage& left,
                                         const ExchangeMessage& right) noexcept {
  return left.query_id == right.query_id && left.tablet_id == right.tablet_id &&
         left.sequence == right.sequence && left.terminal == right.terminal &&
         left.partial.count == right.partial.count &&
         same_float_bits(left.partial.sum, right.partial.sum) &&
         same_optional_float_bits(left.partial.minimum, right.partial.minimum) &&
         same_optional_float_bits(left.partial.maximum, right.partial.maximum) &&
         same_float_bits(left.partial.mean, right.partial.mean) &&
         same_float_bits(left.partial.m2, right.partial.m2);
}

} // namespace

EncodedExchangeMessage::EncodedExchangeMessage(
    std::array<std::byte, distributed_format::kExchangeMessageLength> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedExchangeMessage::bytes() const noexcept {
  return bytes_;
}

common::Result<EncodedExchangeMessage> encode_exchange_message(const ExchangeMessage& message) {
  const common::Status validation = validate_exchange_message(message);
  if (!validation.is_ok())
    return common::make_unexpected(validation);
  std::array<std::byte, distributed_format::kExchangeMessageLength> bytes{};
  common::ByteWriter writer{bytes};
  common::Status status = writer.write_exact(kExchangeMessageMagic);
  if (status.is_ok())
    status = writer.write_u16_le(distributed_format::kExchangeMessageMajor);
  if (status.is_ok())
    status = writer.write_u16_le(distributed_format::kExchangeMessageMinor);
  if (status.is_ok())
    status = writer.write_u32_le(distributed_format::kExchangeMessageLength);
  if (status.is_ok())
    status = writer.write_exact(message.query_id.bytes());
  if (status.is_ok())
    status = writer.write_exact(message.tablet_id.bytes());
  if (status.is_ok())
    status = writer.write_u64_le(message.sequence);
  if (status.is_ok())
    status = writer.write_u64_le(message.partial.count);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.sum);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.minimum.value_or(0.0));
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.maximum.value_or(0.0));
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.mean);
  if (status.is_ok())
    status = writer.write_float64_le(message.partial.m2);
  std::uint32_t flags = message.terminal ? kTerminalFlag : 0U;
  if (message.partial.minimum.has_value())
    flags |= kMinimumFlag | kMaximumFlag;
  if (status.is_ok())
    status = writer.write_u32_le(flags);
  if (status.is_ok())
    status = writer.zero_fill(16U);
  if (!status.is_ok() || writer.offset() != distributed_format::kExchangeMessageLength - 4U) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "exchange frame layout does not match its frozen length"});
  }
  status = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInternal, "exchange frame checksum does not fit frozen layout"});
  }
  return EncodedExchangeMessage{std::move(bytes)};
}

common::Result<ExchangeMessage> decode_exchange_message_exact(const common::ByteView bytes) {
  if (bytes.size() != distributed_format::kExchangeMessageLength)
    return common::make_unexpected(corruption("exchange frame length is not exact"));
  if (!std::ranges::equal(bytes.first(kExchangeMessageMagic.size()), kExchangeMessageMagic))
    return common::make_unexpected(corruption("exchange frame magic is invalid"));
  const std::uint32_t expected_crc = common::crc32c(bytes.first(bytes.size() - 4U));
  common::ByteReader checksum_reader{bytes.last(4U)};
  const common::Result<std::uint32_t> stored_crc = checksum_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != expected_crc)
    return common::make_unexpected(corruption("exchange frame checksum is invalid"));

  common::ByteReader reader{bytes};
  if (!reader.skip(kExchangeMessageMagic.size()).is_ok())
    return common::make_unexpected(corruption("exchange frame header is truncated"));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto length = reader.read_u32_le();
  if (!major.has_value() || !minor.has_value() || !length.has_value())
    return common::make_unexpected(corruption("exchange frame header is truncated"));
  if (*major != distributed_format::kExchangeMessageMajor ||
      *minor != distributed_format::kExchangeMessageMinor) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported, "exchange frame version is unsupported"});
  }
  if (*length != distributed_format::kExchangeMessageLength)
    return common::make_unexpected(corruption("exchange frame encoded length is invalid"));
  const auto query_bytes = reader.read_exact(common::Uuid::kSize);
  const auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  const auto sequence = reader.read_u64_le();
  const auto count = reader.read_u64_le();
  const auto sum = reader.read_float64_le();
  const auto minimum = reader.read_float64_le();
  const auto maximum = reader.read_float64_le();
  const auto mean = reader.read_float64_le();
  const auto m2 = reader.read_float64_le();
  const auto flags = reader.read_u32_le();
  const auto reserved = reader.read_exact(16U);
  if (!query_bytes.has_value() || !tablet_bytes.has_value() || !sequence.has_value() ||
      !count.has_value() || !sum.has_value() || !minimum.has_value() || !maximum.has_value() ||
      !mean.has_value() || !m2.has_value() || !flags.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("exchange frame payload is truncated"));
  }
  if (reader.remaining() != 4U)
    return common::make_unexpected(corruption("exchange frame layout is invalid"));
  if ((*flags & ~kKnownExchangeFlags) != 0U ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(
        corruption("exchange frame flags or reserved bytes are invalid"));
  }
  if (((*flags & kMinimumFlag) != 0U) != ((*flags & kMaximumFlag) != 0U))
    return common::make_unexpected(corruption("exchange frame extrema flags disagree"));
  const bool has_extrema = (*flags & kMinimumFlag) != 0U;
  if (!has_extrema && (std::bit_cast<std::uint64_t>(*minimum) != 0U ||
                       std::bit_cast<std::uint64_t>(*maximum) != 0U)) {
    return common::make_unexpected(corruption("exchange frame absent extrema are not canonical"));
  }
  common::Uuid::Bytes query_id_bytes{};
  common::Uuid::Bytes tablet_id_bytes{};
  std::ranges::copy(*query_bytes, query_id_bytes.begin());
  std::ranges::copy(*tablet_bytes, tablet_id_bytes.begin());
  const common::Result<schema::TabletId> tablet_id = schema::TabletId::from_bytes(tablet_id_bytes);
  if (!tablet_id.has_value())
    return common::make_unexpected(corruption("exchange frame tablet identity is invalid"));
  ExchangeMessage message{
      .query_id = common::Uuid{query_id_bytes},
      .tablet_id = *tablet_id,
      .sequence = *sequence,
      .partial = {.count = *count,
                  .sum = *sum,
                  .minimum = has_extrema ? std::optional<double>{*minimum} : std::nullopt,
                  .maximum = has_extrema ? std::optional<double>{*maximum} : std::nullopt,
                  .mean = *mean,
                  .m2 = *m2},
      .terminal = (*flags & kTerminalFlag) != 0U};
  const common::Status validation = validate_exchange_message(message);
  if (!validation.is_ok())
    return common::make_unexpected(corruption("exchange frame aggregate state is invalid"));
  return message;
}

common::Result<ExchangeFrameReadStep> ExchangeFrameReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  const std::size_t consumed =
      std::min(bytes.size(), distributed_format::kExchangeMessageLength - buffered_bytes_);
  std::ranges::copy(bytes.first(consumed),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(buffered_bytes_));
  buffered_bytes_ += consumed;
  if (buffered_bytes_ != distributed_format::kExchangeMessageLength)
    return ExchangeFrameReadStep{.consumed_bytes = consumed};

  common::Result<ExchangeMessage> decoded = decode_exchange_message_exact(bytes_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  buffered_bytes_ = 0U;
  return ExchangeFrameReadStep{.consumed_bytes = consumed, .message = std::move(*decoded)};
}

std::size_t ExchangeFrameReader::buffered_bytes() const noexcept {
  return buffered_bytes_;
}

bool ExchangeFrameReader::failed() const noexcept {
  return failure_.has_value();
}

ExchangeFrameWriteCursor::ExchangeFrameWriteCursor(EncodedExchangeMessage encoded) noexcept
    : encoded_(std::move(encoded)) {}

ExchangeFrameWriteCursor::ExchangeFrameWriteCursor(ExchangeFrameWriteCursor&& other) noexcept
    : encoded_(std::move(other.encoded_)),
      written_bytes_(
          std::exchange(other.written_bytes_, distributed_format::kExchangeMessageLength)) {}

ExchangeFrameWriteCursor&
ExchangeFrameWriteCursor::operator=(ExchangeFrameWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_ = std::move(other.encoded_);
    written_bytes_ =
        std::exchange(other.written_bytes_, distributed_format::kExchangeMessageLength);
  }
  return *this;
}

common::Result<ExchangeFrameWriteCursor>
ExchangeFrameWriteCursor::create(const ExchangeMessage& message) {
  common::Result<EncodedExchangeMessage> encoded = encode_exchange_message(message);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return ExchangeFrameWriteCursor{std::move(*encoded)};
}

common::ByteView ExchangeFrameWriteCursor::pending_write() const noexcept {
  return encoded_.bytes().subspan(written_bytes_);
}

common::Status ExchangeFrameWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > distributed_format::kExchangeMessageLength - written_bytes_)
    return invalid("written byte count exceeds the distributed exchange frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t ExchangeFrameWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool ExchangeFrameWriteCursor::complete() const noexcept {
  return written_bytes_ == distributed_format::kExchangeMessageLength;
}

common::Result<DistributedAggregatePlan> plan_distributed_aggregation(
    const common::Uuid query_id, const std::vector<DistributedTablet>& tablets,
    const DistributedEventTimePredicate& predicate, const DistributedReadConsistency consistency,
    const DistributedPlanLimits limits) {
  return plan_distributed_aggregation(query_id, tablets, predicate,
                                      DistributedReadPolicy{consistency, std::nullopt}, limits);
}

common::Result<DistributedAggregatePlan> plan_distributed_aggregation(
    const common::Uuid query_id, const std::vector<DistributedTablet>& tablets,
    const DistributedEventTimePredicate& predicate, const DistributedReadPolicy read_policy,
    const DistributedPlanLimits limits) {
  if (query_id.is_nil() || limits.maximum_tablets == 0U || limits.maximum_fragments == 0U ||
      tablets.size() > limits.maximum_tablets ||
      (predicate.lower_inclusive.has_value() && predicate.upper_exclusive.has_value() &&
       *predicate.lower_inclusive >= *predicate.upper_exclusive)) {
    return common::make_unexpected(
        invalid("distributed query identity, limits, or predicate invalid"));
  }
  const common::Status policy_status = validate_read_policy(read_policy);
  if (!policy_status.is_ok())
    return common::make_unexpected(policy_status);
  std::set<schema::TabletId> identities;
  DistributedAggregatePlan plan{query_id, read_policy, {}};
  for (const DistributedTablet& tablet : tablets) {
    if (tablet.tablet_id.uuid().is_nil() || tablet.minimum_event_time > tablet.maximum_event_time ||
        tablet.leader_node == 0U || !identities.insert(tablet.tablet_id).second) {
      return common::make_unexpected(invalid("distributed tablet metadata is invalid"));
    }
    if (intersects(tablet, predicate)) {
      if (plan.fragments.size() >= limits.maximum_fragments) {
        return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                      "distributed fragment bound is exhausted"});
      }
      plan.fragments.push_back(tablet);
    }
  }
  return plan;
}

common::Status
validate_distributed_read_admission(const DistributedReadPolicy policy,
                                    const std::span<const DistributedTablet> fragments,
                                    const DistributedReadAdmission& admission) {
  const auto fragment =
      std::ranges::find(fragments, admission.tablet_id, &DistributedTablet::tablet_id);
  if (fragment == fragments.end() || admission.serving_node == 0U)
    return invalid("distributed read admission does not match a planned tablet");

  switch (policy.consistency) {
  case DistributedReadConsistency::kLeaderLinearizable:
    if (admission.serving_node != fragment->leader_node ||
        !admission.linearizable_barrier.has_value() || admission.linearizable_barrier->term == 0U ||
        admission.linearizable_barrier->context == 0U ||
        admission.linearizable_barrier->read_index == 0U ||
        admission.applied_position < admission.linearizable_barrier->read_index) {
      return common::Status{common::StatusCode::kUnavailable,
                            "leader-linearizable tablet has no applied Raft read barrier"};
    }
    return common::Status::ok();
  case DistributedReadConsistency::kFollowerBoundedStale: {
    if (!policy.maximum_staleness_positions.has_value() ||
        admission.linearizable_barrier.has_value() ||
        admission.observed_leader_commit_position < fragment->known_leader_commit_position) {
      return common::Status{common::StatusCode::kUnavailable,
                            "bounded-stale tablet has no current leader-commit observation"};
    }
    const std::uint64_t lag =
        admission.observed_leader_commit_position > admission.applied_position
            ? admission.observed_leader_commit_position - admission.applied_position
            : 0U;
    return lag <= *policy.maximum_staleness_positions
               ? common::Status::ok()
               : common::Status{common::StatusCode::kUnavailable,
                                "bounded-stale tablet exceeds its position lag"};
  }
  case DistributedReadConsistency::kLocalEventual:
    return admission.linearizable_barrier.has_value()
               ? invalid("local-eventual admission must not masquerade as a Raft proof")
               : common::Status::ok();
  }
  return invalid("distributed read consistency mode is invalid");
}

common::Status validate_distributed_read_admission(const DistributedAggregatePlan& plan,
                                                   const DistributedReadAdmission& admission) {
  return validate_distributed_read_admission(plan.read_policy, plan.fragments, admission);
}

common::Status MergeableAggregateState::add(const double value) {
  if (count == std::numeric_limits<std::uint64_t>::max()) {
    return common::Status{common::StatusCode::kOutOfRange, "distributed aggregate count overflows"};
  }
  ++count;
  sum += value;
  minimum = minimum.has_value() ? std::min(*minimum, value) : value;
  maximum = maximum.has_value() ? std::max(*maximum, value) : value;
  const double delta = value - mean;
  mean += delta / static_cast<double>(count);
  m2 += delta * (value - mean);
  return common::Status::ok();
}

common::Status MergeableAggregateState::merge(const MergeableAggregateState& other) {
  if (other.count == 0U)
    return common::Status::ok();
  if (!other.minimum.has_value() || !other.maximum.has_value() ||
      (count != 0U && (!minimum.has_value() || !maximum.has_value()))) {
    return invalid("distributed partial aggregate state is inconsistent");
  }
  if (count > std::numeric_limits<std::uint64_t>::max() - other.count) {
    return common::Status{common::StatusCode::kOutOfRange, "distributed aggregate count overflows"};
  }
  if (count == 0U) {
    *this = other;
    return common::Status::ok();
  }
  const std::uint64_t combined = count + other.count;
  const double delta = other.mean - mean;
  m2 += other.m2 + delta * delta * static_cast<double>(count) * static_cast<double>(other.count) /
                       static_cast<double>(combined);
  mean += delta * static_cast<double>(other.count) / static_cast<double>(combined);
  count = combined;
  sum += other.sum;
  minimum = std::min(*minimum, *other.minimum);
  maximum = std::max(*maximum, *other.maximum);
  return common::Status::ok();
}

std::optional<double> MergeableAggregateState::variance_population() const noexcept {
  return count == 0U ? std::nullopt : std::optional<double>{m2 / static_cast<double>(count)};
}

class BoundedExchange::Impl {
public:
  Impl(common::Uuid id, ExchangeLimits configured) : query_id(id), limits(configured) {}
  common::Uuid query_id;
  ExchangeLimits limits;
  mutable std::mutex mutex;
  std::deque<ExchangeMessage> messages;
  std::size_t charged_bytes{};
  bool is_cancelled{};
};

BoundedExchange::BoundedExchange(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
BoundedExchange::~BoundedExchange() = default;
BoundedExchange::BoundedExchange(BoundedExchange&&) noexcept = default;
BoundedExchange& BoundedExchange::operator=(BoundedExchange&&) noexcept = default;

common::Result<BoundedExchange> BoundedExchange::create(const common::Uuid query_id,
                                                        const ExchangeLimits limits) {
  if (query_id.is_nil() || limits.maximum_messages == 0U ||
      limits.maximum_bytes < kExchangeMessageCharge) {
    return common::make_unexpected(invalid("exchange identity or bounds are invalid"));
  }
  return BoundedExchange{std::make_unique<Impl>(query_id, limits)};
}

common::Status BoundedExchange::push(ExchangeMessage message) {
  std::scoped_lock lock{impl_->mutex};
  if (impl_->is_cancelled) {
    return common::Status{common::StatusCode::kCancelled, "exchange is cancelled"};
  }
  const common::Status validation = validate_exchange_message(message);
  if (!validation.is_ok())
    return validation;
  if (message.query_id != impl_->query_id)
    return invalid("exchange message belongs to another query");
  if (impl_->messages.size() >= impl_->limits.maximum_messages ||
      kExchangeMessageCharge > impl_->limits.maximum_bytes - impl_->charged_bytes) {
    return common::Status{common::StatusCode::kResourceExhausted, "exchange is backpressured"};
  }
  impl_->charged_bytes += kExchangeMessageCharge;
  impl_->messages.push_back(std::move(message));
  return common::Status::ok();
}

common::Result<std::optional<ExchangeMessage>> BoundedExchange::try_pop() {
  std::scoped_lock lock{impl_->mutex};
  if (impl_->is_cancelled) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCancelled, "exchange is cancelled"});
  }
  if (impl_->messages.empty())
    return std::optional<ExchangeMessage>{};
  ExchangeMessage message = std::move(impl_->messages.front());
  impl_->messages.pop_front();
  impl_->charged_bytes -= kExchangeMessageCharge;
  return std::optional<ExchangeMessage>{std::move(message)};
}

common::Status BoundedExchange::cancel() {
  std::scoped_lock lock{impl_->mutex};
  impl_->is_cancelled = true;
  impl_->messages.clear();
  impl_->charged_bytes = 0U;
  return common::Status::ok();
}

bool BoundedExchange::cancelled() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->is_cancelled;
}
std::size_t BoundedExchange::queued_messages() const noexcept {
  std::scoped_lock lock{impl_->mutex};
  return impl_->messages.size();
}

class DistributedAggregateCoordinator::Impl {
public:
  struct FragmentProgress {
    std::vector<ExchangeMessage> messages;
    MergeableAggregateState merged;
    bool terminal{};
  };

  Impl(DistributedAggregatePlan value, DistributedCoordinatorLimits configured)
      : plan(std::move(value)), limits(configured) {
    for (const auto& fragment : plan.fragments)
      fragments.emplace(fragment.tablet_id, FragmentProgress{});
  }
  DistributedAggregatePlan plan;
  DistributedCoordinatorLimits limits;
  std::map<schema::TabletId, FragmentProgress> fragments;
  std::size_t retained_messages{};
  std::optional<common::Status> failure;
};

DistributedAggregateCoordinator::DistributedAggregateCoordinator(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DistributedAggregateCoordinator::~DistributedAggregateCoordinator() = default;
DistributedAggregateCoordinator::DistributedAggregateCoordinator(
    DistributedAggregateCoordinator&&) noexcept = default;
DistributedAggregateCoordinator&
DistributedAggregateCoordinator::operator=(DistributedAggregateCoordinator&&) noexcept = default;

common::Result<DistributedAggregateCoordinator>
DistributedAggregateCoordinator::create(DistributedAggregatePlan plan,
                                        std::vector<DistributedReadAdmission> admissions,
                                        const DistributedCoordinatorLimits limits) {
  if (plan.query_id.is_nil()) {
    return common::make_unexpected(invalid("distributed coordinator query identity is invalid"));
  }
  if (limits.maximum_messages_per_fragment == 0U ||
      limits.maximum_messages_per_fragment > limits.maximum_total_messages ||
      limits.maximum_total_messages > kMaximumDistributedCoordinatorMessages ||
      limits.maximum_total_messages < plan.fragments.size()) {
    return common::make_unexpected(invalid("distributed coordinator limits are invalid"));
  }
  const common::Status policy_status = validate_read_policy(plan.read_policy);
  if (!policy_status.is_ok())
    return common::make_unexpected(policy_status);
  if (admissions.size() != plan.fragments.size()) {
    return common::make_unexpected(
        invalid("distributed coordinator requires one read admission per fragment"));
  }
  try {
    std::set<schema::TabletId> admitted;
    for (const DistributedReadAdmission& admission : admissions) {
      if (!admitted.insert(admission.tablet_id).second) {
        return common::make_unexpected(invalid("distributed read admission is duplicated"));
      }
      const common::Status validated = validate_distributed_read_admission(plan, admission);
      if (!validated.is_ok())
        return common::make_unexpected(validated);
    }
    return DistributedAggregateCoordinator{std::make_unique<Impl>(std::move(plan), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed coordinator allocation failed"});
  }
}

common::Status DistributedAggregateCoordinator::accept(const ExchangeMessage& message) {
  if (impl_->failure.has_value())
    return *impl_->failure;
  const common::Status validation = validate_exchange_message(message);
  if (!validation.is_ok())
    return validation;
  auto fragment = impl_->fragments.find(message.tablet_id);
  if (message.query_id != impl_->plan.query_id || fragment == impl_->fragments.end())
    return invalid("distributed fragment result does not belong to the plan");

  Impl::FragmentProgress& progress = fragment->second;
  if (message.sequence <= progress.messages.size()) {
    const ExchangeMessage& retained = progress.messages[message.sequence - 1U];
    return same_exchange_message(message, retained)
               ? common::Status::ok()
               : common::Status{common::StatusCode::kAlreadyExists,
                                "distributed fragment sequence conflicts with retained state"};
  }
  if (progress.terminal)
    return invalid("distributed fragment emitted after its terminal message");
  if (message.sequence != progress.messages.size() + 1U) {
    return common::Status{common::StatusCode::kUnavailable,
                          "distributed fragment sequence has a gap"};
  }
  if (progress.messages.size() == impl_->limits.maximum_messages_per_fragment ||
      impl_->retained_messages == impl_->limits.maximum_total_messages) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "distributed coordinator message history is exhausted"};
  }

  MergeableAggregateState merged = progress.merged;
  const common::Status merge_status = merged.merge(message.partial);
  if (!merge_status.is_ok())
    return merge_status;
  try {
    progress.messages.push_back(message);
  } catch (const std::bad_alloc&) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          "distributed coordinator message retention allocation failed"};
  }
  progress.merged = std::move(merged);
  progress.terminal = message.terminal;
  ++impl_->retained_messages;
  return common::Status::ok();
}

common::Status DistributedAggregateCoordinator::worker_failed(const schema::TabletId& tablet_id,
                                                              common::Status failure) {
  const auto fragment = impl_->fragments.find(tablet_id);
  if (fragment == impl_->fragments.end() || failure.is_ok()) {
    return invalid("distributed worker failure is invalid or belongs to another plan");
  }
  if (fragment->second.terminal)
    return common::Status::ok();
  if (impl_->failure.has_value())
    return *impl_->failure;
  impl_->failure = std::move(failure);
  return common::Status::ok();
}

common::Result<MergeableAggregateState> DistributedAggregateCoordinator::finish() const {
  if (impl_->failure.has_value())
    return common::make_unexpected(*impl_->failure);
  MergeableAggregateState merged;
  for (const auto& [tablet, progress] : impl_->fragments) {
    static_cast<void>(tablet);
    if (!progress.terminal) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                    "distributed query has incomplete fragments"});
    }
    const common::Status status = merged.merge(progress.merged);
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  return merged;
}

} // namespace chronos::query
