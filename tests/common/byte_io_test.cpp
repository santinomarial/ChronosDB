#include "chronos/common/byte_reader.hpp"

#include "chronos/common/byte_writer.hpp"

#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <string>

namespace chronos::common {
namespace {

constexpr std::size_t kMixedEncodedSize = 42;

Status write_mixed_values(ByteWriter& writer) {
  const std::array<Status, 10> statuses{
      writer.write_u8(0xa5U),
      writer.write_u16_le(0x1234U),
      writer.write_u32_le(0x89abcdefU),
      writer.write_u64_le(0x0123456789abcdefULL),
      writer.write_i8(-7),
      writer.write_i16_le(-1234),
      writer.write_i32_le(-12345678),
      writer.write_i64_le(std::numeric_limits<std::int64_t>::min()),
      writer.write_float32_le(1.5F),
      writer.write_float64_le(-2.25),
  };
  for (const Status& status : statuses) {
    if (!status.is_ok()) {
      return status;
    }
  }
  return Status::ok();
}

Status read_mixed_values(ByteReader& reader) {
  const auto u8 = reader.read_u8();
  if (!u8) {
    return u8.error();
  }
  const auto u16 = reader.read_u16_le();
  if (!u16) {
    return u16.error();
  }
  const auto u32 = reader.read_u32_le();
  if (!u32) {
    return u32.error();
  }
  const auto u64 = reader.read_u64_le();
  if (!u64) {
    return u64.error();
  }
  const auto i8 = reader.read_i8();
  if (!i8) {
    return i8.error();
  }
  const auto i16 = reader.read_i16_le();
  if (!i16) {
    return i16.error();
  }
  const auto i32 = reader.read_i32_le();
  if (!i32) {
    return i32.error();
  }
  const auto i64 = reader.read_i64_le();
  if (!i64) {
    return i64.error();
  }
  const auto float32 = reader.read_float32_le();
  if (!float32) {
    return float32.error();
  }
  const auto float64 = reader.read_float64_le();
  if (!float64) {
    return float64.error();
  }

  if (*u8 != 0xa5U || *u16 != 0x1234U || *u32 != 0x89abcdefU ||
      *u64 != 0x0123456789abcdefULL || *i8 != -7 || *i16 != -1234 || *i32 != -12345678 ||
      *i64 != std::numeric_limits<std::int64_t>::min() || *float32 != 1.5F || *float64 != -2.25) {
    return Status{StatusCode::kInternal, "mixed-value test decoded unexpected data"};
  }
  return Status::ok();
}

TEST(ByteIoTest, RoundTripsEveryPrimitiveAtExactCapacity) {
  std::array<std::byte, kMixedEncodedSize> buffer{};
  ByteWriter writer{buffer};
  const Status write_status = write_mixed_values(writer);
  ASSERT_TRUE(write_status.is_ok()) << write_status.to_string();
  EXPECT_EQ(writer.offset(), buffer.size());
  EXPECT_EQ(writer.remaining(), 0U);
  EXPECT_TRUE(writer.full());

  EXPECT_EQ(buffer[0], std::byte{0xa5});
  EXPECT_EQ(buffer[1], std::byte{0x34});
  EXPECT_EQ(buffer[2], std::byte{0x12});
  EXPECT_EQ(buffer[3], std::byte{0xef});
  EXPECT_EQ(buffer[4], std::byte{0xcd});

  ByteReader reader{buffer};
  const Status read_status = read_mixed_values(reader);
  ASSERT_TRUE(read_status.is_ok()) << read_status.to_string();
  EXPECT_EQ(reader.offset(), buffer.size());
  EXPECT_EQ(reader.remaining(), 0U);
  EXPECT_TRUE(reader.empty());
}

TEST(ByteIoTest, ZeroLengthOperationsSucceedWithoutTouchingOffsets) {
  ByteReader reader{ByteView{}};
  const auto read = reader.read_exact(0);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->empty());
  EXPECT_EQ(reader.offset(), 0U);
  EXPECT_TRUE(reader.empty());

  ByteWriter writer{MutableByteView{}};
  EXPECT_TRUE(writer.write_exact(ByteView{}).is_ok());
  EXPECT_TRUE(writer.zero_fill(0).is_ok());
  EXPECT_EQ(writer.offset(), 0U);
  EXPECT_TRUE(writer.full());
}

TEST(ByteIoTest, FailedReadDoesNotAdvance) {
  const std::array<std::byte, 3> buffer{std::byte{1}, std::byte{2}, std::byte{3}};
  ByteReader reader{buffer};
  ASSERT_TRUE(reader.skip(1).is_ok());
  const std::size_t before = reader.offset();
  const auto result = reader.read_u32_le();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), StatusCode::kOutOfRange);
  EXPECT_NE(result.error().message().find("read_u32_le"), std::string::npos);
  EXPECT_EQ(reader.offset(), before);
  EXPECT_EQ(reader.remaining(), 2U);
}

TEST(ByteIoTest, FailedPrimitiveWriteDoesNotAdvanceOrModify) {
  std::array<std::byte, 7> buffer{};
  buffer.fill(std::byte{0xaa});
  const auto original = buffer;
  ByteWriter writer{buffer};
  const Status status = writer.write_u64_le(0x0123456789abcdefULL);
  EXPECT_FALSE(status.is_ok());
  EXPECT_EQ(status.code(), StatusCode::kOutOfRange);
  EXPECT_EQ(writer.offset(), 0U);
  EXPECT_EQ(buffer, original);
}

TEST(ByteIoTest, FailedBulkWritesDoNotPartiallyModify) {
  std::array<std::byte, 3> destination{};
  destination.fill(std::byte{0x55});
  const auto original = destination;
  const std::array<std::byte, 4> source{
      std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  ByteWriter writer{destination};

  EXPECT_FALSE(writer.write_exact(source).is_ok());
  EXPECT_EQ(destination, original);
  EXPECT_EQ(writer.offset(), 0U);

  EXPECT_FALSE(writer.zero_fill(4).is_ok());
  EXPECT_EQ(destination, original);
  EXPECT_EQ(writer.offset(), 0U);
}

TEST(ByteIoTest, PeekAndSubreaderHaveExplicitAdvanceBehavior) {
  const std::array<std::byte, 5> buffer{
      std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}, std::byte{5}};
  ByteReader reader{buffer};
  const auto peeked = reader.peek_exact(2);
  ASSERT_TRUE(peeked.has_value());
  EXPECT_EQ(reader.offset(), 0U);
  EXPECT_EQ((*peeked)[1], std::byte{2});

  auto subreader = reader.read_subreader(3);
  ASSERT_TRUE(subreader.has_value());
  EXPECT_EQ(reader.offset(), 3U);
  EXPECT_EQ(subreader->remaining(), 3U);
  const auto subvalue = subreader->read_u16_le();
  ASSERT_TRUE(subvalue.has_value());
  EXPECT_EQ(*subvalue, 0x0201U);

  const std::size_t before = reader.offset();
  EXPECT_FALSE(reader.read_subreader(3).has_value());
  EXPECT_EQ(reader.offset(), before);
}

TEST(ByteIoTest, PreservesFloatingPointBitPatterns) {
  constexpr std::array<std::uint32_t, 7> kFloatPatterns{
      0x00000000U, 0x80000000U, 0x7f800000U, 0xff800000U,
      0x7fc12345U, 0x00000001U, 0x7f7fffffU};
  constexpr std::array<std::uint64_t, 7> kDoublePatterns{
      0x0000000000000000ULL, 0x8000000000000000ULL, 0x7ff0000000000000ULL,
      0xfff0000000000000ULL, 0x7ff8123456789abcULL, 0x0000000000000001ULL,
      0x7fefffffffffffffULL};

  std::array<std::byte, (kFloatPatterns.size() * sizeof(float)) +
                            (kDoublePatterns.size() * sizeof(double))>
      buffer{};
  ByteWriter writer{buffer};
  for (const std::uint32_t bits : kFloatPatterns) {
    ASSERT_TRUE(writer.write_float32_le(std::bit_cast<float>(bits)).is_ok());
  }
  for (const std::uint64_t bits : kDoublePatterns) {
    ASSERT_TRUE(writer.write_float64_le(std::bit_cast<double>(bits)).is_ok());
  }

  ByteReader reader{buffer};
  for (const std::uint32_t expected : kFloatPatterns) {
    const auto value = reader.read_float32_le();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(std::bit_cast<std::uint32_t>(*value), expected);
  }
  for (const std::uint64_t expected : kDoublePatterns) {
    const auto value = reader.read_float64_le();
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(std::bit_cast<std::uint64_t>(*value), expected);
  }
}

TEST(ByteIoTest, SupportsUnalignedStartingAddresses) {
  std::array<std::byte, 17> storage{};
  MutableByteView unaligned = MutableByteView{storage}.subspan(1);
  ByteWriter writer{unaligned};
  ASSERT_TRUE(writer.write_u64_le(0x0123456789abcdefULL).is_ok());
  ASSERT_TRUE(writer.write_u64_le(0xfedcba9876543210ULL).is_ok());

  ByteReader reader{ByteView{unaligned}};
  const auto first = reader.read_u64_le();
  const auto second = reader.read_u64_le();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*first, 0x0123456789abcdefULL);
  EXPECT_EQ(*second, 0xfedcba9876543210ULL);
}

TEST(ByteIoTest, RejectsEveryTruncatedBoundary) {
  std::array<std::byte, kMixedEncodedSize> complete{};
  ByteWriter writer{complete};
  ASSERT_TRUE(write_mixed_values(writer).is_ok());

  for (std::size_t size = 0; size < complete.size(); ++size) {
    SCOPED_TRACE(::testing::Message() << "truncated_size=" << size);
    ByteReader reader{ByteView{complete}.first(size)};
    const Status status = read_mixed_values(reader);
    EXPECT_FALSE(status.is_ok());
    EXPECT_LE(reader.offset(), size);
  }
}

TEST(ByteIoTest, DeterministicRandomIntegerRoundTrips) {
  constexpr std::uint64_t kSeed = 0x8d6f3a21c497b5e0ULL;
  std::mt19937_64 random{kSeed};
  for (std::size_t iteration = 0; iteration < 2000; ++iteration) {
    SCOPED_TRACE(::testing::Message() << "seed=" << kSeed << " iteration=" << iteration);
    const std::uint64_t u64 = random();
    const std::uint32_t u32 = static_cast<std::uint32_t>(random());
    const std::uint16_t u16 = static_cast<std::uint16_t>(random());
    const std::uint8_t u8 = static_cast<std::uint8_t>(random());
    std::array<std::byte, 15> buffer{};
    ByteWriter writer{buffer};
    ASSERT_TRUE(writer.write_u64_le(u64).is_ok());
    ASSERT_TRUE(writer.write_u32_le(u32).is_ok());
    ASSERT_TRUE(writer.write_u16_le(u16).is_ok());
    ASSERT_TRUE(writer.write_u8(u8).is_ok());

    ByteReader reader{buffer};
    const auto decoded_u64 = reader.read_u64_le();
    const auto decoded_u32 = reader.read_u32_le();
    const auto decoded_u16 = reader.read_u16_le();
    const auto decoded_u8 = reader.read_u8();
    ASSERT_TRUE(decoded_u64.has_value());
    ASSERT_TRUE(decoded_u32.has_value());
    ASSERT_TRUE(decoded_u16.has_value());
    ASSERT_TRUE(decoded_u8.has_value());
    EXPECT_EQ(*decoded_u64, u64);
    EXPECT_EQ(*decoded_u32, u32);
    EXPECT_EQ(*decoded_u16, u16);
    EXPECT_EQ(*decoded_u8, u8);
    EXPECT_TRUE(reader.empty());
  }
}

TEST(ByteIoTest, RejectsDeclaredRangesThatCannotFitOrOverflow) {
  std::array<std::byte, 8> buffer{};
  ByteReader reader{buffer};
  const auto read = reader.read_exact(std::numeric_limits<std::size_t>::max());
  EXPECT_FALSE(read.has_value());
  EXPECT_EQ(reader.offset(), 0U);

  buffer.fill(std::byte{0x77});
  const auto original = buffer;
  ByteWriter writer{buffer};
  const Status write = writer.zero_fill(std::numeric_limits<std::size_t>::max());
  EXPECT_FALSE(write.is_ok());
  EXPECT_EQ(writer.offset(), 0U);
  EXPECT_EQ(buffer, original);
}

} // namespace
} // namespace chronos::common
