#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/distributed.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

using ExchangeBytes = std::array<std::byte, distributed_format::kExchangeMessageLength>;

void store_u16_le(ExchangeBytes& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(ExchangeBytes& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_exchange_crc(ExchangeBytes& bytes) {
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] ExchangeBytes copy_encoded(const EncodedExchangeMessage& encoded) {
  ExchangeBytes bytes{};
  std::ranges::copy(encoded.bytes(), bytes.begin());
  return bytes;
}

[[nodiscard]] std::vector<DistributedReadAdmission>
linearizable_admissions(const DistributedAggregatePlan& plan) {
  std::vector<DistributedReadAdmission> admissions;
  admissions.reserve(plan.fragments.size());
  for (const DistributedTablet& fragment : plan.fragments) {
    admissions.push_back({fragment.tablet_id, fragment.leader_node, fragment.local_applied_position,
                          fragment.local_applied_position,
                          raft::ReadBarrier{1U, static_cast<std::uint64_t>(admissions.size()) + 1U,
                                            fragment.local_applied_position}});
  }
  return admissions;
}

TEST(DistributedQueryTest, PrunesTabletsMergesPartialStateAndRequiresEveryFragment) {
  const common::Uuid query_id = uuid(1U);
  const std::vector<DistributedTablet> tablets{
      {tablet(2U), 0, 99, 1U, 10U},
      {tablet(3U), 100, 199, 2U, 10U},
      {tablet(4U), 200, 299, 3U, 10U},
  };
  auto plan =
      plan_distributed_aggregation(query_id, tablets, DistributedEventTimePredicate{50, 250});
  ASSERT_TRUE(plan.has_value()) << plan.error().to_string();
  ASSERT_EQ(plan->fragments.size(), 3U);
  auto pruned =
      plan_distributed_aggregation(query_id, tablets, DistributedEventTimePredicate{100, 200});
  ASSERT_TRUE(pruned.has_value());
  ASSERT_EQ(pruned->fragments.size(), 1U);
  EXPECT_EQ(pruned->fragments.front().tablet_id, tablet(3U));

  auto admissions = linearizable_admissions(*plan);
  auto coordinator =
      DistributedAggregateCoordinator::create(std::move(*plan), std::move(admissions));
  ASSERT_TRUE(coordinator.has_value());
  MergeableAggregateState first;
  ASSERT_TRUE(first.add(1.0).is_ok());
  ASSERT_TRUE(first.add(2.0).is_ok());
  MergeableAggregateState second;
  ASSERT_TRUE(second.add(3.0).is_ok());
  MergeableAggregateState third;
  ASSERT_TRUE(third.add(4.0).is_ok());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(2U), 1U, first, true}).is_ok());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(3U), 1U, second, true}).is_ok());
  EXPECT_FALSE(coordinator->finish().has_value());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(4U), 1U, third, true}).is_ok());
  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->count, 4U);
  EXPECT_DOUBLE_EQ(result->sum, 10.0);
  ASSERT_TRUE(result->minimum.has_value());
  ASSERT_TRUE(result->maximum.has_value());
  const std::optional<double> variance = result->variance_population();
  ASSERT_TRUE(variance.has_value());
  EXPECT_DOUBLE_EQ(result->minimum.value_or(std::numeric_limits<double>::quiet_NaN()), 1.0);
  EXPECT_DOUBLE_EQ(result->maximum.value_or(std::numeric_limits<double>::quiet_NaN()), 4.0);
  EXPECT_NEAR(variance.value_or(std::numeric_limits<double>::quiet_NaN()), 1.25, 1e-12);
}

TEST(DistributedQueryTest, BackpressureCancellationAndWorkerFailureFailClosed) {
  const common::Uuid query_id = uuid(1U);
  auto exchange = BoundedExchange::create(query_id, ExchangeLimits{1U, sizeof(ExchangeMessage)});
  ASSERT_TRUE(exchange.has_value());
  MergeableAggregateState partial;
  ASSERT_TRUE(partial.add(1.0).is_ok());
  EXPECT_TRUE(exchange->push({query_id, tablet(2U), 1U, partial, true}).is_ok());
  const auto full = exchange->push({query_id, tablet(2U), 2U, partial, true});
  EXPECT_EQ(full.code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(exchange->cancel().is_ok());
  EXPECT_EQ(exchange->queued_messages(), 0U);
  EXPECT_EQ(exchange->try_pop().error().code(), common::StatusCode::kCancelled);

  auto plan = plan_distributed_aggregation(query_id, {{tablet(2U), 0, 1, 1U, 1U}}, {});
  ASSERT_TRUE(plan.has_value());
  auto admissions = linearizable_admissions(*plan);
  auto coordinator =
      DistributedAggregateCoordinator::create(std::move(*plan), std::move(admissions));
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_TRUE(coordinator
                  ->worker_failed(tablet(2U),
                                  common::Status{common::StatusCode::kUnavailable, "worker lost"})
                  .is_ok());
  EXPECT_EQ(coordinator
                ->worker_failed(tablet(2U),
                                common::Status{common::StatusCode::kInternal, "later failure"})
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(coordinator->accept({query_id, tablet(2U), 1U, partial, true}).code(),
            common::StatusCode::kUnavailable);
  const auto result = coordinator->finish();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(result.error().message(), "worker lost");
  const auto repeated = coordinator->finish();
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error().code(), result.error().code());
  EXPECT_EQ(repeated.error().message(), result.error().message());
}

TEST(DistributedQueryTest, CoordinatorEnforcesSequenceDeduplicationAndTerminalOwnership) {
  const common::Uuid query_id = uuid(1U);
  auto plan = plan_distributed_aggregation(query_id, {{tablet(2U), 0, 1, 1U, 1U}}, {});
  ASSERT_TRUE(plan.has_value());
  auto coordinator = DistributedAggregateCoordinator::create(*plan, linearizable_admissions(*plan));
  ASSERT_TRUE(coordinator.has_value());

  MergeableAggregateState first;
  ASSERT_TRUE(first.add(1.0).is_ok());
  MergeableAggregateState second;
  ASSERT_TRUE(second.add(3.0).is_ok());
  const ExchangeMessage first_message{query_id, tablet(2U), 1U, first, false};
  const ExchangeMessage terminal_message{query_id, tablet(2U), 2U, second, true};

  EXPECT_EQ(coordinator->accept(terminal_message).code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(coordinator->finish().error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator->accept(first_message).is_ok());
  EXPECT_TRUE(coordinator->accept(first_message).is_ok());

  ExchangeMessage conflict = first_message;
  conflict.partial = second;
  EXPECT_EQ(coordinator->accept(conflict).code(), common::StatusCode::kAlreadyExists);
  EXPECT_TRUE(coordinator->accept(terminal_message).is_ok());
  EXPECT_TRUE(coordinator->accept(terminal_message).is_ok());

  ExchangeMessage after_terminal = terminal_message;
  after_terminal.sequence = 3U;
  EXPECT_EQ(coordinator->accept(after_terminal).code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(coordinator
                  ->worker_failed(tablet(2U),
                                  common::Status{common::StatusCode::kUnavailable, "socket lost"})
                  .is_ok());
  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->count, 2U);
  EXPECT_DOUBLE_EQ(result->sum, 4.0);
  ASSERT_TRUE(result->minimum.has_value());
  ASSERT_TRUE(result->maximum.has_value());
  EXPECT_DOUBLE_EQ(result->minimum.value_or(std::numeric_limits<double>::quiet_NaN()), 1.0);
  EXPECT_DOUBLE_EQ(result->maximum.value_or(std::numeric_limits<double>::quiet_NaN()), 3.0);
}

TEST(DistributedQueryTest, CoordinatorEnforcesFiniteRetryHistory) {
  const common::Uuid query_id = uuid(1U);
  auto plan = plan_distributed_aggregation(query_id, {{tablet(2U), 0, 1, 1U, 1U}}, {});
  ASSERT_TRUE(plan.has_value());
  const auto admissions = linearizable_admissions(*plan);
  EXPECT_EQ(DistributedAggregateCoordinator::create(
                *plan, admissions,
                DistributedCoordinatorLimits{1U, kMaximumDistributedCoordinatorMessages + 1U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto coordinator = DistributedAggregateCoordinator::create(*plan, admissions,
                                                             DistributedCoordinatorLimits{1U, 1U});
  ASSERT_TRUE(coordinator.has_value());
  MergeableAggregateState partial;
  ASSERT_TRUE(partial.add(1.0).is_ok());
  EXPECT_TRUE(coordinator->accept({query_id, tablet(2U), 1U, partial, false}).is_ok());
  EXPECT_EQ(coordinator->accept({query_id, tablet(2U), 2U, partial, true}).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(coordinator->finish().error().code(), common::StatusCode::kUnavailable);
}

TEST(DistributedQueryTest, ExchangeFrameHasFrozenLayoutAndRoundTripsAggregateBits) {
  const ExchangeMessage message{
      .query_id = uuid(0x11U),
      .tablet_id = tablet(0x22U),
      .sequence = 0x0102030405060708ULL,
      .partial = {.count = 2U, .sum = 3.0, .minimum = 1.0, .maximum = 2.0, .mean = 1.5, .m2 = 0.5},
      .terminal = true};
  const auto encoded = encode_exchange_message(message);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->bytes().size(), distributed_format::kExchangeMessageLength);

  common::ByteReader reader{encoded->bytes()};
  const auto magic = reader.read_exact(8U);
  ASSERT_TRUE(magic.has_value());
  const std::array<std::byte, 8U> expected_magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                                 std::byte{'X'}, std::byte{'C'}, std::byte{'H'},
                                                 std::byte{'G'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(*magic, expected_magic));
  EXPECT_EQ(reader.read_u16_le().value(), distributed_format::kExchangeMessageMajor);
  EXPECT_EQ(reader.read_u16_le().value(), distributed_format::kExchangeMessageMinor);
  EXPECT_EQ(reader.read_u32_le().value(), distributed_format::kExchangeMessageLength);
  EXPECT_EQ(reader.read_exact(common::Uuid::kSize).value()[0], std::byte{0x11U});
  EXPECT_EQ(reader.read_exact(common::Uuid::kSize).value()[0], std::byte{0x22U});
  EXPECT_EQ(reader.read_u64_le().value(), message.sequence);
  EXPECT_EQ(reader.read_u64_le().value(), 2U);
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()),
            std::bit_cast<std::uint64_t>(3.0));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()),
            std::bit_cast<std::uint64_t>(1.0));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()),
            std::bit_cast<std::uint64_t>(2.0));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()),
            std::bit_cast<std::uint64_t>(1.5));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()),
            std::bit_cast<std::uint64_t>(0.5));
  EXPECT_EQ(reader.read_u32_le().value(), 0x7U);
  const auto reserved = reader.read_exact(16U);
  ASSERT_TRUE(reserved.has_value());
  EXPECT_TRUE(
      std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0}; }));
  EXPECT_EQ(reader.read_u32_le().value(), 0xe43b9302U);
  EXPECT_TRUE(reader.empty());

  const auto decoded = decode_exchange_message_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->query_id, message.query_id);
  EXPECT_EQ(decoded->tablet_id, message.tablet_id);
  EXPECT_EQ(decoded->sequence, message.sequence);
  EXPECT_EQ(decoded->partial.count, message.partial.count);
  EXPECT_EQ(std::bit_cast<std::uint64_t>(decoded->partial.sum),
            std::bit_cast<std::uint64_t>(message.partial.sum));
  ASSERT_TRUE(decoded->partial.minimum.has_value());
  ASSERT_TRUE(decoded->partial.maximum.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(
                decoded->partial.minimum.value_or(std::numeric_limits<double>::quiet_NaN())),
            std::bit_cast<std::uint64_t>(
                message.partial.minimum.value_or(std::numeric_limits<double>::quiet_NaN())));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(
                decoded->partial.maximum.value_or(std::numeric_limits<double>::quiet_NaN())),
            std::bit_cast<std::uint64_t>(
                message.partial.maximum.value_or(std::numeric_limits<double>::quiet_NaN())));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(decoded->partial.mean),
            std::bit_cast<std::uint64_t>(message.partial.mean));
  EXPECT_EQ(std::bit_cast<std::uint64_t>(decoded->partial.m2),
            std::bit_cast<std::uint64_t>(message.partial.m2));
  EXPECT_TRUE(decoded->terminal);
}

TEST(DistributedQueryTest, ExchangeFrameRejectsCorruptionUnknownVersionAndNoncanonicalBytes) {
  const ExchangeMessage empty{.query_id = uuid(1U),
                              .tablet_id = tablet(2U),
                              .sequence = 1U,
                              .partial = {},
                              .terminal = false};
  const auto encoded = encode_exchange_message(empty);
  ASSERT_TRUE(encoded.has_value());
  const ExchangeBytes canonical = copy_encoded(*encoded);

  EXPECT_EQ(decode_exchange_message_exact(common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(canonical.begin(), canonical.end());
  trailing.push_back(std::byte{0});
  EXPECT_EQ(decode_exchange_message_exact(trailing).error().code(),
            common::StatusCode::kCorruption);

  ExchangeBytes corrupt = canonical;
  corrupt[64U] ^= std::byte{1U};
  EXPECT_EQ(decode_exchange_message_exact(corrupt).error().code(), common::StatusCode::kCorruption);

  ExchangeBytes future = canonical;
  store_u16_le(future, 8U, distributed_format::kExchangeMessageMajor + 1U);
  rewrite_exchange_crc(future);
  EXPECT_EQ(decode_exchange_message_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  ExchangeBytes reserved = canonical;
  reserved[108U] = std::byte{1U};
  rewrite_exchange_crc(reserved);
  EXPECT_EQ(decode_exchange_message_exact(reserved).error().code(),
            common::StatusCode::kCorruption);

  ExchangeBytes absent_extrema = canonical;
  absent_extrema[72U] = std::byte{1U};
  rewrite_exchange_crc(absent_extrema);
  EXPECT_EQ(decode_exchange_message_exact(absent_extrema).error().code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedQueryTest, ExchangeBoundariesRejectInconsistentAggregateState) {
  const common::Uuid query_id = uuid(1U);
  ExchangeMessage invalid_message{.query_id = query_id,
                                  .tablet_id = tablet(2U),
                                  .sequence = 1U,
                                  .partial = {.count = 1U},
                                  .terminal = true};
  EXPECT_EQ(encode_exchange_message(invalid_message).error().code(),
            common::StatusCode::kInvalidArgument);

  auto exchange = BoundedExchange::create(query_id);
  ASSERT_TRUE(exchange.has_value());
  EXPECT_EQ(exchange->push(invalid_message).code(), common::StatusCode::kInvalidArgument);

  auto plan = plan_distributed_aggregation(query_id, {{tablet(2U), 0, 1, 1U, 1U}}, {});
  ASSERT_TRUE(plan.has_value());
  auto coordinator = DistributedAggregateCoordinator::create(*plan, linearizable_admissions(*plan));
  ASSERT_TRUE(coordinator.has_value());
  EXPECT_EQ(coordinator->accept(invalid_message).code(), common::StatusCode::kInvalidArgument);

  invalid_message.partial = {};
  invalid_message.partial.sum = -0.0;
  EXPECT_EQ(encode_exchange_message(invalid_message).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryTest, ExchangeFrameReaderHandlesEverySplitAndCoalescedFrames) {
  const ExchangeMessage first_message{
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .sequence = 3U,
      .partial = {.count = 1U, .sum = 4.0, .minimum = 4.0, .maximum = 4.0, .mean = 4.0},
      .terminal = false};
  ExchangeMessage second_message = first_message;
  second_message.sequence = 4U;
  second_message.terminal = true;
  const auto first_encoded = encode_exchange_message(first_message);
  const auto second_encoded = encode_exchange_message(second_message);
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());
  ExchangeMessage missing_message = first_message;
  missing_message.sequence = 0U;

  for (std::size_t split = 0U; split <= first_encoded->bytes().size(); ++split) {
    ExchangeFrameReader reader;
    const auto prefix = reader.consume(first_encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->message.has_value(), split == first_encoded->bytes().size())
        << "split=" << split;
    const auto suffix = reader.consume(first_encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, first_encoded->bytes().size() - split) << "split=" << split;
    ASSERT_TRUE(prefix->message.has_value() || suffix->message.has_value()) << "split=" << split;
    const ExchangeMessage decoded =
        prefix->message.value_or(suffix->message.value_or(missing_message));
    EXPECT_EQ(decoded.sequence, first_message.sequence) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  std::vector<std::byte> coalesced(first_encoded->bytes().begin(), first_encoded->bytes().end());
  coalesced.insert(coalesced.end(), second_encoded->bytes().begin(), second_encoded->bytes().end());
  ExchangeFrameReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->message.has_value());
  EXPECT_EQ(first->consumed_bytes, distributed_format::kExchangeMessageLength);
  const ExchangeMessage decoded_first = first->message.value_or(missing_message);
  EXPECT_EQ(decoded_first.sequence, first_message.sequence);
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->message.has_value());
  const ExchangeMessage decoded_second = second->message.value_or(missing_message);
  EXPECT_EQ(decoded_second.sequence, second_message.sequence);
  EXPECT_TRUE(decoded_second.terminal);
}

TEST(DistributedQueryTest, ExchangeFrameReaderFailureIsSticky) {
  const ExchangeMessage message{.query_id = uuid(1U),
                                .tablet_id = tablet(2U),
                                .sequence = 3U,
                                .partial = {},
                                .terminal = true};
  const auto encoded = encode_exchange_message(message);
  ASSERT_TRUE(encoded.has_value());
  ExchangeBytes corrupt = copy_encoded(*encoded);
  corrupt[64U] ^= std::byte{1U};

  ExchangeFrameReader reader;
  const auto rejected = reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.buffered_bytes(), distributed_format::kExchangeMessageLength);
  const auto retry = reader.consume(encoded->bytes());
  ASSERT_FALSE(retry.has_value());
  EXPECT_EQ(retry.error().code(), rejected.error().code());
  EXPECT_EQ(retry.error().message(), rejected.error().message());
}

TEST(DistributedQueryTest, ExchangeFrameWriteCursorOwnsAndAdvancesExactSuffix) {
  const ExchangeMessage message{.query_id = uuid(1U),
                                .tablet_id = tablet(2U),
                                .sequence = 3U,
                                .partial = {},
                                .terminal = true};
  const auto expected = encode_exchange_message(message);
  ASSERT_TRUE(expected.has_value());
  auto cursor = ExchangeFrameWriteCursor::create(message);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), expected->bytes()));
  EXPECT_FALSE(cursor->complete());

  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), expected->bytes().subspan(17U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  ExchangeFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  EXPECT_EQ(moved.written_bytes(), 17U);
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(moved.pending_write().empty());
  EXPECT_TRUE(moved.consume_written(0U).is_ok());

  ExchangeMessage invalid_message = message;
  invalid_message.sequence = 0U;
  EXPECT_EQ(ExchangeFrameWriteCursor::create(invalid_message).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryTest, EnforcesExplicitReadConsistencyEvidencePerFragment) {
  const common::Uuid query_id = uuid(5U);
  const DistributedTablet fragment{tablet(6U), 0, 99, 7U, 95U, 100U};
  EXPECT_EQ(plan_distributed_aggregation(query_id, {fragment}, {},
                                         DistributedReadConsistency::kFollowerBoundedStale)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto bounded = plan_distributed_aggregation(
      query_id, {fragment}, {},
      DistributedReadPolicy{DistributedReadConsistency::kFollowerBoundedStale, 5U});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  const DistributedReadAdmission within_bound{fragment.tablet_id, 8U, 95U, 100U, std::nullopt};
  EXPECT_TRUE(validate_distributed_read_admission(*bounded, within_bound).is_ok());
  DistributedReadAdmission too_stale = within_bound;
  too_stale.applied_position = 94U;
  EXPECT_EQ(validate_distributed_read_admission(*bounded, too_stale).code(),
            common::StatusCode::kUnavailable);
  DistributedReadAdmission old_observation = within_bound;
  old_observation.observed_leader_commit_position = 99U;
  EXPECT_EQ(validate_distributed_read_admission(*bounded, old_observation).code(),
            common::StatusCode::kUnavailable);
  EXPECT_FALSE(DistributedAggregateCoordinator::create(*bounded, {too_stale}).has_value());

  auto eventual = plan_distributed_aggregation(
      query_id, {fragment}, {},
      DistributedReadPolicy{DistributedReadConsistency::kLocalEventual, std::nullopt});
  ASSERT_TRUE(eventual.has_value());
  EXPECT_TRUE(DistributedAggregateCoordinator::create(
                  std::move(*eventual), {{fragment.tablet_id, 8U, 1U, 0U, std::nullopt}})
                  .has_value());

  auto linearizable = plan_distributed_aggregation(query_id, {fragment}, {});
  ASSERT_TRUE(linearizable.has_value());
  const DistributedReadAdmission unapplied{fragment.tablet_id, fragment.leader_node, 99U, 100U,
                                           raft::ReadBarrier{2U, 9U, 100U}};
  EXPECT_EQ(validate_distributed_read_admission(*linearizable, unapplied).code(),
            common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::query
