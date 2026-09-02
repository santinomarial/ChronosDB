#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <ranges>
#include <vector>

namespace chronos::query {
namespace {

using Frame = std::array<std::byte, grouped_float64_exchange_format::kFrameLength>;
using TerminalFrame = std::array<std::byte, grouped_exchange_terminal_format::kFrameLength>;

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

template <std::size_t Size>
void store_u16_le(std::array<std::byte, Size>& bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

template <std::size_t Size>
void store_u32_le(std::array<std::byte, Size>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

template <std::size_t Size>
void store_u64_le(std::array<std::byte, Size>& bytes, const std::size_t offset,
                  const std::uint64_t value) {
  for (std::size_t index = 0U; index < 8U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

template <std::size_t Size> void rewrite_crc(std::array<std::byte, Size>& bytes) {
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] Frame copy_encoded(const EncodedGroupedFloat64ExchangeMessage& encoded) {
  Frame bytes{};
  std::ranges::copy(encoded.bytes(), bytes.begin());
  return bytes;
}

[[nodiscard]] GroupedFloat64ExchangeMessage message(std::optional<double> key) {
  return {
      .query_id = uuid(0x11U),
      .tablet_id = tablet(0x22U),
      .sequence = 0x0102'0304'0506'0708ULL,
      .group_key = key,
      .partial = {.count = 2U, .sum = 3.0, .minimum = 1.0, .maximum = 2.0, .mean = 1.5, .m2 = 0.5},
      .terminal = true};
}

[[nodiscard]] MergeableAggregateState one_value(const double value) {
  return {.count = 1U, .sum = value, .minimum = value, .maximum = value, .mean = value, .m2 = 0.0};
}

TEST(DistributedGroupedExchangeTest, FreezesLayoutAndCanonicalizesFloatGroupingKeys) {
  const auto encoded = encode_grouped_float64_exchange_message(message(-0.0));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  common::ByteReader reader{encoded->bytes()};
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'X'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(reader.read_exact(8U).value(), magic));
  EXPECT_EQ(reader.read_u16_le().value(), grouped_float64_exchange_format::kMajor);
  EXPECT_EQ(reader.read_u16_le().value(), grouped_float64_exchange_format::kMinor);
  EXPECT_EQ(reader.read_u32_le().value(), grouped_float64_exchange_format::kFrameLength);
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x11U});
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x22U});
  EXPECT_EQ(reader.read_u64_le().value(), 0x0102'0304'0506'0708ULL);
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()), 0U);
  EXPECT_EQ(reader.read_u64_le().value(), 2U);
  EXPECT_EQ(reader.read_float64_le().value(), 3.0);
  EXPECT_EQ(reader.read_float64_le().value(), 1.0);
  EXPECT_EQ(reader.read_float64_le().value(), 2.0);
  EXPECT_EQ(reader.read_float64_le().value(), 1.5);
  EXPECT_EQ(reader.read_float64_le().value(), 0.5);
  EXPECT_EQ(reader.read_u32_le().value(), 0x0fU);
  const auto reserved = reader.read_exact(16U);
  ASSERT_TRUE(reserved.has_value());
  EXPECT_TRUE(
      std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0U}; }));
  const auto stored_crc = reader.read_u32_le();
  ASSERT_TRUE(stored_crc.has_value());
  EXPECT_EQ(*stored_crc, common::crc32c(encoded->bytes().first(encoded->bytes().size() - 4U)));
  EXPECT_TRUE(reader.empty());

  auto decoded = decode_grouped_float64_exchange_message_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_TRUE(decoded->group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(
                decoded->group_key.value_or(std::numeric_limits<double>::quiet_NaN())),
            0U);
  EXPECT_EQ(decoded->partial.count, 2U);
  EXPECT_EQ(decoded->partial.sum, 3.0);
  EXPECT_TRUE(decoded->terminal);

  const double payload_nan = std::bit_cast<double>(0xfff0'0000'0000'0001ULL);
  const auto nan_encoded = encode_grouped_float64_exchange_message(message(payload_nan));
  ASSERT_TRUE(nan_encoded.has_value());
  const auto nan_decoded = decode_grouped_float64_exchange_message_exact(nan_encoded->bytes());
  ASSERT_TRUE(nan_decoded.has_value());
  ASSERT_TRUE(nan_decoded->group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(nan_decoded->group_key.value_or(0.0)),
            grouped_float64_exchange_format::kCanonicalQuietNanBits);
  const double second_nan = std::bit_cast<double>(0x7ff8'0000'0000'1234ULL);
  const auto second_nan_encoded = encode_grouped_float64_exchange_message(message(second_nan));
  ASSERT_TRUE(second_nan_encoded.has_value());
  EXPECT_TRUE(std::ranges::equal(nan_encoded->bytes(), second_nan_encoded->bytes()));

  const auto null_encoded = encode_grouped_float64_exchange_message(message(std::nullopt));
  ASSERT_TRUE(null_encoded.has_value());
  const auto null_decoded = decode_grouped_float64_exchange_message_exact(null_encoded->bytes());
  ASSERT_TRUE(null_decoded.has_value());
  EXPECT_FALSE(null_decoded->group_key.has_value());
}

TEST(DistributedGroupedExchangeTest, RejectsDamageVersionsAndNoncanonicalRepresentations) {
  const auto encoded = encode_grouped_float64_exchange_message(message(1.0));
  ASSERT_TRUE(encoded.has_value());
  const Frame canonical = copy_encoded(*encoded);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(
                common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(canonical.begin(), canonical.end());
  trailing.push_back(std::byte{0U});
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(trailing).error().code(),
            common::StatusCode::kCorruption);

  Frame damaged = canonical;
  damaged[64U] ^= std::byte{1U};
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  Frame future = canonical;
  store_u16_le(future, 8U, grouped_float64_exchange_format::kMajor + 1U);
  rewrite_crc(future);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  Frame negative_zero = canonical;
  for (std::size_t index = 56U; index < 64U; ++index)
    negative_zero[index] = std::byte{0U};
  negative_zero[63U] = std::byte{0x80U};
  rewrite_crc(negative_zero);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(negative_zero).error().code(),
            common::StatusCode::kCorruption);

  Frame noncanonical_nan = canonical;
  store_u64_le(noncanonical_nan, 56U, 0x7ff8'0000'0000'1234ULL);
  rewrite_crc(noncanonical_nan);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(noncanonical_nan).error().code(),
            common::StatusCode::kCorruption);

  const auto null_encoded = encode_grouped_float64_exchange_message(message(std::nullopt));
  ASSERT_TRUE(null_encoded.has_value());
  Frame null_nonzero = copy_encoded(*null_encoded);
  null_nonzero[56U] = std::byte{1U};
  rewrite_crc(null_nonzero);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(null_nonzero).error().code(),
            common::StatusCode::kCorruption);

  Frame reserved = canonical;
  reserved[116U] = std::byte{1U};
  rewrite_crc(reserved);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(reserved).error().code(),
            common::StatusCode::kCorruption);

  GroupedFloat64ExchangeMessage invalid = message(1.0);
  invalid.partial = {.count = 0U, .sum = -0.0};
  EXPECT_EQ(encode_grouped_float64_exchange_message(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, OwnsEveryReadSplitAndShortWriteSuffix) {
  const GroupedFloat64ExchangeMessage first_message = message(1.0);
  GroupedFloat64ExchangeMessage second_message = message(std::nullopt);
  second_message.sequence = 2U;
  const auto first_encoded = encode_grouped_float64_exchange_message(first_message);
  const auto second_encoded = encode_grouped_float64_exchange_message(second_message);
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());

  for (std::size_t split = 0U; split <= first_encoded->bytes().size(); ++split) {
    GroupedFloat64ExchangeFrameReader reader;
    const auto prefix = reader.consume(first_encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->message.has_value(), split == first_encoded->bytes().size())
        << "split=" << split;
    if (split < first_encoded->bytes().size()) {
      EXPECT_EQ(prefix->message, std::nullopt) << "split=" << split;
    }
    const auto suffix = reader.consume(first_encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, first_encoded->bytes().size() - split) << "split=" << split;
    GroupedFloat64ExchangeMessage missing = first_message;
    missing.sequence = 0U;
    const GroupedFloat64ExchangeMessage decoded =
        prefix->message.value_or(suffix->message.value_or(missing));
    EXPECT_EQ(decoded.sequence, first_message.sequence) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  std::vector<std::byte> coalesced(first_encoded->bytes().begin(), first_encoded->bytes().end());
  coalesced.insert(coalesced.end(), second_encoded->bytes().begin(), second_encoded->bytes().end());
  GroupedFloat64ExchangeFrameReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->message.has_value());
  EXPECT_EQ(first->consumed_bytes, grouped_float64_exchange_format::kFrameLength);
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->message.has_value());
  const GroupedFloat64ExchangeMessage decoded_second = second->message.value_or(first_message);
  EXPECT_EQ(decoded_second.sequence, 2U);
  EXPECT_FALSE(decoded_second.group_key.has_value());

  Frame corrupt = copy_encoded(*first_encoded);
  corrupt[64U] ^= std::byte{1U};
  GroupedFloat64ExchangeFrameReader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  const auto repeated = failed_reader.consume(first_encoded->bytes());
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error(), rejected.error());

  auto cursor = GroupedFloat64ExchangeFrameWriteCursor::create(first_message);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes().subspan(17U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  GroupedFloat64ExchangeFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(moved.consume_written(0U).is_ok());

  GroupedFloat64ExchangeMessage invalid = first_message;
  invalid.sequence = 0U;
  EXPECT_EQ(GroupedFloat64ExchangeFrameWriteCursor::create(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, EncodesDistinctEmptyStreamTerminalWithoutInventingNullGroup) {
  const GroupedExchangeTerminalMessage terminal{
      .query_id = uuid(0x31U), .tablet_id = tablet(0x32U), .sequence = 7U};
  const auto encoded = encode_grouped_exchange_terminal_message(terminal);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  common::ByteReader reader{encoded->bytes()};
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'X'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'T'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(reader.read_exact(8U).value(), magic));
  EXPECT_EQ(reader.read_u16_le().value(), grouped_exchange_terminal_format::kMajor);
  EXPECT_EQ(reader.read_u16_le().value(), grouped_exchange_terminal_format::kMinor);
  EXPECT_EQ(reader.read_u32_le().value(), grouped_exchange_terminal_format::kFrameLength);
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x31U});
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x32U});
  EXPECT_EQ(reader.read_u64_le().value(), 7U);
  const auto reserved = reader.read_exact(4U);
  ASSERT_TRUE(reserved.has_value());
  EXPECT_TRUE(
      std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0U}; }));
  EXPECT_EQ(reader.read_u32_le().value(),
            common::crc32c(encoded->bytes().first(encoded->bytes().size() - 4U)));
  EXPECT_TRUE(reader.empty());

  const auto decoded = decode_grouped_exchange_terminal_message_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->query_id, terminal.query_id);
  EXPECT_EQ(decoded->tablet_id, terminal.tablet_id);
  EXPECT_EQ(decoded->sequence, terminal.sequence);

  TerminalFrame bytes{};
  std::ranges::copy(encoded->bytes(), bytes.begin());
  EXPECT_EQ(decode_grouped_exchange_terminal_message_exact(
                common::ByteView{bytes}.first(bytes.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(bytes.begin(), bytes.end());
  trailing.push_back(std::byte{0U});
  EXPECT_EQ(decode_grouped_exchange_terminal_message_exact(trailing).error().code(),
            common::StatusCode::kCorruption);
  TerminalFrame corrupt = bytes;
  corrupt[40U] ^= std::byte{1U};
  EXPECT_EQ(decode_grouped_exchange_terminal_message_exact(corrupt).error().code(),
            common::StatusCode::kCorruption);
  TerminalFrame future = bytes;
  store_u16_le(future, 8U, grouped_exchange_terminal_format::kMajor + 1U);
  rewrite_crc(future);
  EXPECT_EQ(decode_grouped_exchange_terminal_message_exact(future).error().code(),
            common::StatusCode::kNotSupported);
  TerminalFrame damaged = bytes;
  damaged[56U] = std::byte{1U};
  rewrite_crc(damaged);
  EXPECT_EQ(decode_grouped_exchange_terminal_message_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  GroupedExchangeTerminalMessage invalid = terminal;
  invalid.sequence = 0U;
  EXPECT_EQ(encode_grouped_exchange_terminal_message(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
  invalid = terminal;
  invalid.query_id = {};
  EXPECT_EQ(encode_grouped_exchange_terminal_message(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, OwnsEveryTerminalReadSplitAndShortWriteSuffix) {
  const GroupedExchangeTerminalMessage first_message{
      .query_id = uuid(0x31U), .tablet_id = tablet(0x32U), .sequence = 7U};
  GroupedExchangeTerminalMessage second_message = first_message;
  second_message.sequence = 8U;
  const auto first_encoded = encode_grouped_exchange_terminal_message(first_message);
  const auto second_encoded = encode_grouped_exchange_terminal_message(second_message);
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());

  for (std::size_t split = 0U; split <= first_encoded->bytes().size(); ++split) {
    GroupedExchangeTerminalFrameReader reader;
    const auto prefix = reader.consume(first_encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->message.has_value(), split == first_encoded->bytes().size())
        << "split=" << split;
    if (split < first_encoded->bytes().size()) {
      EXPECT_EQ(prefix->message, std::nullopt) << "split=" << split;
    }
    const auto suffix = reader.consume(first_encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, first_encoded->bytes().size() - split) << "split=" << split;
    GroupedExchangeTerminalMessage missing = first_message;
    missing.sequence = 0U;
    const GroupedExchangeTerminalMessage decoded =
        prefix->message.value_or(suffix->message.value_or(missing));
    EXPECT_EQ(decoded.sequence, first_message.sequence) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U);
  }

  std::vector<std::byte> coalesced(first_encoded->bytes().begin(), first_encoded->bytes().end());
  coalesced.insert(coalesced.end(), second_encoded->bytes().begin(), second_encoded->bytes().end());
  GroupedExchangeTerminalFrameReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->message.has_value());
  EXPECT_EQ(first->consumed_bytes, grouped_exchange_terminal_format::kFrameLength);
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->message.has_value());
  EXPECT_EQ(second->message.value_or(first_message).sequence, second_message.sequence);

  TerminalFrame corrupt{};
  std::ranges::copy(first_encoded->bytes(), corrupt.begin());
  corrupt[40U] ^= std::byte{1U};
  GroupedExchangeTerminalFrameReader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  const auto repeated = failed_reader.consume(first_encoded->bytes());
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error(), rejected.error());

  auto cursor = GroupedExchangeTerminalFrameWriteCursor::create(first_message);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(13U).is_ok());
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 30U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes().subspan(30U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 30U);
  GroupedExchangeTerminalFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(moved.consume_written(0U).is_ok());

  GroupedExchangeTerminalMessage invalid = first_message;
  invalid.sequence = 0U;
  EXPECT_EQ(GroupedExchangeTerminalFrameWriteCursor::create(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, CoordinatesCanonicalGroupsOnlyAfterEveryTabletTerminates) {
  const common::Uuid query_id = uuid(0x41U);
  auto coordinator =
      DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x42U), tablet(0x43U)});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();

  const GroupedFloat64ExchangeMessage negative_zero{.query_id = query_id,
                                                    .tablet_id = tablet(0x42U),
                                                    .sequence = 1U,
                                                    .group_key = -0.0,
                                                    .partial = one_value(1.0),
                                                    .terminal = false};
  EXPECT_TRUE(coordinator->accept(negative_zero).is_ok());
  GroupedFloat64ExchangeMessage positive_zero_retry = negative_zero;
  positive_zero_retry.group_key = 0.0;
  EXPECT_TRUE(coordinator->accept(positive_zero_retry).is_ok());
  GroupedFloat64ExchangeMessage conflicting_retry = positive_zero_retry;
  conflicting_retry.partial = one_value(9.0);
  EXPECT_EQ(coordinator->accept(conflicting_retry).code(), common::StatusCode::kAlreadyExists);

  EXPECT_EQ(coordinator
                ->accept({.query_id = query_id,
                          .tablet_id = tablet(0x42U),
                          .sequence = 3U,
                          .group_key = std::nullopt,
                          .partial = one_value(2.0),
                          .terminal = true})
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x42U),
                            .sequence = 2U,
                            .group_key = std::nullopt,
                            .partial = one_value(2.0),
                            .terminal = true})
                  .is_ok());
  EXPECT_EQ(coordinator->finish().error().code(), common::StatusCode::kUnavailable);

  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x43U),
                            .sequence = 1U,
                            .group_key = 0.0,
                            .partial = one_value(3.0),
                            .terminal = false})
                  .is_ok());
  const double payload_nan = std::bit_cast<double>(0xfff0'0000'0000'0001ULL);
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x43U),
                            .sequence = 2U,
                            .group_key = payload_nan,
                            .partial = one_value(4.0),
                            .terminal = true})
                  .is_ok());

  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 3U);
  EXPECT_FALSE((*result)[0].group_key.has_value());
  EXPECT_DOUBLE_EQ((*result)[0].aggregate.sum, 2.0);
  ASSERT_TRUE((*result)[1].group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(
                (*result)[1].group_key.value_or(std::numeric_limits<double>::quiet_NaN())),
            0U);
  EXPECT_EQ((*result)[1].aggregate.count, 2U);
  EXPECT_DOUBLE_EQ((*result)[1].aggregate.sum, 4.0);
  ASSERT_TRUE((*result)[2].group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>((*result)[2].group_key.value_or(0.0)),
            grouped_float64_exchange_format::kCanonicalQuietNanBits);
  EXPECT_DOUBLE_EQ((*result)[2].aggregate.sum, 4.0);

  GroupedFloat64ExchangeMessage after_terminal = positive_zero_retry;
  after_terminal.sequence = 3U;
  EXPECT_EQ(coordinator->accept(after_terminal).code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(
      coordinator
          ->worker_failed(tablet(0x42U), {common::StatusCode::kUnavailable, "late disconnect"})
          .is_ok());
}

TEST(DistributedGroupedExchangeTest, DistinguishesEmptyTerminalFromEveryRealGroup) {
  const common::Uuid query_id = uuid(0x51U);
  auto empty =
      DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x52U), tablet(0x53U)});
  ASSERT_TRUE(empty.has_value());
  const GroupedExchangeTerminalMessage first{
      .query_id = query_id, .tablet_id = tablet(0x52U), .sequence = 1U};
  const GroupedExchangeTerminalMessage second{
      .query_id = query_id, .tablet_id = tablet(0x53U), .sequence = 1U};
  EXPECT_TRUE(empty->accept_terminal(first).is_ok());
  EXPECT_TRUE(empty->accept_terminal(first).is_ok());
  EXPECT_TRUE(empty->accept_terminal(second).is_ok());
  const auto result = empty->finish();
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->empty());

  auto mixed = DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x52U)});
  ASSERT_TRUE(mixed.has_value());
  const GroupedFloat64ExchangeMessage null_group{.query_id = query_id,
                                                 .tablet_id = tablet(0x52U),
                                                 .sequence = 1U,
                                                 .group_key = std::nullopt,
                                                 .partial = one_value(1.0),
                                                 .terminal = false};
  EXPECT_TRUE(mixed->accept(null_group).is_ok());
  GroupedExchangeTerminalMessage late_terminal = first;
  late_terminal.sequence = 2U;
  EXPECT_EQ(mixed->accept_terminal(late_terminal).code(), common::StatusCode::kInvalidArgument);

  auto conflicting = DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x52U)});
  ASSERT_TRUE(conflicting.has_value());
  EXPECT_TRUE(conflicting->accept_terminal(first).is_ok());
  GroupedFloat64ExchangeMessage same_sequence = null_group;
  same_sequence.terminal = true;
  EXPECT_EQ(conflicting->accept(same_sequence).code(), common::StatusCode::kAlreadyExists);
}

TEST(DistributedGroupedExchangeTest, OrdersAndLimitsOnlyAfterGlobalGroupMerge) {
  const common::Uuid query_id = uuid(0x81U);
  auto coordinator = DistributedGroupedFloat64Coordinator::create(
      query_id, {tablet(0x82U), tablet(0x83U)}, {},
      {.direction = DistributedGroupedFloat64ResultDirection::kDescending,
       .null_placement = DistributedGroupedFloat64NullPlacement::kLast,
       .limit = 2U});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x82U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(1.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x82U),
                            .sequence = 2U,
                            .group_key = -2.0,
                            .partial = one_value(2.0),
                            .terminal = true})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x83U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(3.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x83U),
                            .sequence = 2U,
                            .group_key = std::numeric_limits<double>::quiet_NaN(),
                            .partial = one_value(4.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x83U),
                            .sequence = 3U,
                            .group_key = std::nullopt,
                            .partial = one_value(5.0),
                            .terminal = true})
                  .is_ok());

  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 2U);
  ASSERT_TRUE((*result)[0].group_key.has_value());
  EXPECT_TRUE(std::isnan((*result)[0].group_key.value_or(0.0)));
  EXPECT_DOUBLE_EQ((*result)[0].aggregate.sum, 4.0);
  ASSERT_TRUE((*result)[1].group_key.has_value());
  EXPECT_DOUBLE_EQ((*result)[1].group_key.value_or(std::numeric_limits<double>::quiet_NaN()), 1.0);
  EXPECT_EQ((*result)[1].aggregate.count, 2U);
  EXPECT_DOUBLE_EQ((*result)[1].aggregate.sum, 4.0);

  auto empty =
      DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x82U)}, {}, {.limit = 0U});
  ASSERT_TRUE(empty.has_value());
  EXPECT_TRUE(empty
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x82U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(1.0),
                            .terminal = true})
                  .is_ok());
  ASSERT_TRUE(empty->finish().has_value());
  EXPECT_TRUE(empty->finish()->empty());

  EXPECT_EQ(DistributedGroupedFloat64Coordinator::create(
                query_id, {tablet(0x82U)}, {},
                // Deliberately probes validation of an out-of-range wire-facing enum value.
                // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
                {.direction = static_cast<DistributedGroupedFloat64ResultDirection>(0U)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, OrdersByMergedAggregateWithDeterministicGroupTieBreak) {
  const common::Uuid query_id = uuid(0x91U);
  auto coordinator = DistributedGroupedFloat64Coordinator::create(
      query_id, {tablet(0x92U), tablet(0x93U)}, {},
      {.order_key = DistributedGroupedFloat64ResultOrderKey::kSum,
       .direction = DistributedGroupedFloat64ResultDirection::kDescending,
       .null_placement = DistributedGroupedFloat64NullPlacement::kLast,
       .limit = 2U});
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x92U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(2.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x92U),
                            .sequence = 2U,
                            .group_key = 3.0,
                            .partial = one_value(5.0),
                            .terminal = true})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x93U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(4.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x93U),
                            .sequence = 2U,
                            .group_key = 2.0,
                            .partial = one_value(6.0),
                            .terminal = true})
                  .is_ok());

  const auto result = coordinator->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 2U);
  EXPECT_EQ((*result)[0].group_key, 1.0);
  EXPECT_EQ((*result)[0].aggregate.count, 2U);
  EXPECT_EQ((*result)[0].aggregate.sum, 6.0);
  EXPECT_EQ((*result)[1].group_key, 2.0);
  EXPECT_EQ((*result)[1].aggregate.sum, 6.0);

  EXPECT_EQ(DistributedGroupedFloat64Coordinator::create(
                query_id, {tablet(0x92U)}, {},
                // Deliberately probes validation of an out-of-range wire-facing enum value.
                // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
                {.order_key = static_cast<DistributedGroupedFloat64ResultOrderKey>(0U)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedGroupedExchangeTest, BoundsHistoryAndRetainsFirstIncompleteWorkerFailure) {
  const common::Uuid query_id = uuid(0x61U);
  EXPECT_EQ(DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x62U), tablet(0x62U)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x62U), tablet(0x63U)},
                                                         DistributedCoordinatorLimits{1U, 1U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto bounded = DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x62U)},
                                                              DistributedCoordinatorLimits{1U, 1U});
  ASSERT_TRUE(bounded.has_value());
  EXPECT_TRUE(bounded
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x62U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(1.0),
                            .terminal = false})
                  .is_ok());
  EXPECT_EQ(bounded
                ->accept({.query_id = query_id,
                          .tablet_id = tablet(0x62U),
                          .sequence = 2U,
                          .group_key = 2.0,
                          .partial = one_value(2.0),
                          .terminal = true})
                .code(),
            common::StatusCode::kResourceExhausted);

  auto failed = DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x62U)});
  ASSERT_TRUE(failed.has_value());
  const common::Status first_failure{common::StatusCode::kUnavailable, "worker lost"};
  EXPECT_TRUE(failed->worker_failed(tablet(0x62U), first_failure).is_ok());
  const GroupedFloat64ExchangeMessage after_failure{.query_id = query_id,
                                                    .tablet_id = tablet(0x62U),
                                                    .sequence = 1U,
                                                    .group_key = 1.0,
                                                    .partial = one_value(1.0),
                                                    .terminal = true};
  EXPECT_EQ(failed->accept(after_failure), first_failure);
  EXPECT_EQ(
      failed->accept_terminal({.query_id = query_id, .tablet_id = tablet(0x62U), .sequence = 1U}),
      first_failure);
  EXPECT_EQ(failed->worker_failed(tablet(0x62U), {common::StatusCode::kInternal, "later failure"}),
            first_failure);
  const auto finished = failed->finish();
  ASSERT_FALSE(finished.has_value());
  EXPECT_EQ(finished.error(), first_failure);
  const auto repeated_finish = failed->finish();
  ASSERT_FALSE(repeated_finish.has_value());
  EXPECT_EQ(repeated_finish.error(), first_failure);
}

TEST(DistributedGroupedExchangeTest, FailsClosedWhenCrossTabletGroupCountOverflows) {
  const common::Uuid query_id = uuid(0x71U);
  auto coordinator =
      DistributedGroupedFloat64Coordinator::create(query_id, {tablet(0x72U), tablet(0x73U)});
  ASSERT_TRUE(coordinator.has_value());
  const MergeableAggregateState maximum{.count = std::numeric_limits<std::uint64_t>::max(),
                                        .sum = 1.0,
                                        .minimum = 1.0,
                                        .maximum = 1.0,
                                        .mean = 1.0,
                                        .m2 = 0.0};
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x72U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = maximum,
                            .terminal = true})
                  .is_ok());
  EXPECT_TRUE(coordinator
                  ->accept({.query_id = query_id,
                            .tablet_id = tablet(0x73U),
                            .sequence = 1U,
                            .group_key = 1.0,
                            .partial = one_value(1.0),
                            .terminal = true})
                  .is_ok());
  EXPECT_EQ(coordinator->finish().error().code(), common::StatusCode::kOutOfRange);
}

} // namespace
} // namespace chronos::query
