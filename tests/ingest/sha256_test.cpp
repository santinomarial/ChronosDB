#include "chronos/ingest/sha256.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <utility>

namespace chronos::ingest {
namespace {

TEST(Sha256Test, MatchesPublishedEmptyAndAbcVectorsAndStreamsFragments) {
  constexpr Sha256Digest::Bytes empty_expected{
      std::byte{0xe3}, std::byte{0xb0}, std::byte{0xc4}, std::byte{0x42}, std::byte{0x98},
      std::byte{0xfc}, std::byte{0x1c}, std::byte{0x14}, std::byte{0x9a}, std::byte{0xfb},
      std::byte{0xf4}, std::byte{0xc8}, std::byte{0x99}, std::byte{0x6f}, std::byte{0xb9},
      std::byte{0x24}, std::byte{0x27}, std::byte{0xae}, std::byte{0x41}, std::byte{0xe4},
      std::byte{0x64}, std::byte{0x9b}, std::byte{0x93}, std::byte{0x4c}, std::byte{0xa4},
      std::byte{0x95}, std::byte{0x99}, std::byte{0x1b}, std::byte{0x78}, std::byte{0x52},
      std::byte{0xb8}, std::byte{0x55}};
  const auto empty = sha256(common::ByteView{});
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  EXPECT_EQ(empty->bytes(), empty_expected);

  constexpr std::array<std::byte, 3U> abc{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  constexpr Sha256Digest::Bytes abc_expected{
      std::byte{0xba}, std::byte{0x78}, std::byte{0x16}, std::byte{0xbf}, std::byte{0x8f},
      std::byte{0x01}, std::byte{0xcf}, std::byte{0xea}, std::byte{0x41}, std::byte{0x41},
      std::byte{0x40}, std::byte{0xde}, std::byte{0x5d}, std::byte{0xae}, std::byte{0x22},
      std::byte{0x23}, std::byte{0xb0}, std::byte{0x03}, std::byte{0x61}, std::byte{0xa3},
      std::byte{0x96}, std::byte{0x17}, std::byte{0x7a}, std::byte{0x9c}, std::byte{0xb4},
      std::byte{0x10}, std::byte{0xff}, std::byte{0x61}, std::byte{0xf2}, std::byte{0x00},
      std::byte{0x15}, std::byte{0xad}};
  const auto contiguous = sha256(abc);
  ASSERT_TRUE(contiguous.has_value());
  EXPECT_EQ(contiguous->bytes(), abc_expected);

  const std::array<common::ByteView, 4U> fragments{
      common::ByteView{abc}.first(1U), common::ByteView{}, common::ByteView{abc}.subspan(1U, 1U),
      common::ByteView{abc}.last(1U)};
  const auto streamed = sha256(fragments);
  ASSERT_TRUE(streamed.has_value());
  EXPECT_EQ(*streamed, *contiguous);
}

TEST(Sha256Test, IncrementalOwnerMatchesOneShotAndFinalizesExactlyOnce) {
  const std::array<std::byte, 3U> abc{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  auto expected = sha256(abc);
  ASSERT_TRUE(expected.has_value()) << expected.error().to_string();

  auto hasher = Sha256Hasher::create();
  ASSERT_TRUE(hasher.has_value()) << hasher.error().to_string();
  EXPECT_TRUE(hasher->update(common::ByteView{abc}.first(1U)).is_ok());
  EXPECT_TRUE(hasher->update(common::ByteView{}).is_ok());
  EXPECT_TRUE(hasher->update(common::ByteView{abc}.subspan(1U)).is_ok());
  auto actual = hasher->finish();
  ASSERT_TRUE(actual.has_value()) << actual.error().to_string();
  EXPECT_EQ(*actual, *expected);
  EXPECT_EQ(hasher->finish().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(hasher->update(abc).code(), common::StatusCode::kInvalidArgument);

  Sha256Hasher moved = std::move(*hasher);
  EXPECT_EQ(hasher->update(abc).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(moved.finish().error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::ingest
