#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/raft/multiplexed_log.hpp"

#include <cstddef>
#include <gtest/gtest.h>

namespace chronos::raft {
namespace {

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

TEST(MultiplexedLogTest, RoundTripsIndependentGroupPersistentStateAndDetectsCorruption) {
  PersistentState state{};
  state.current_term = 4U;
  state.voted_for = 2U;
  state.log = {LogEntry{1U, 3U, 1U, {std::byte{0x11}}},
               LogEntry{2U, 4U, 2U, {std::byte{0x22}, std::byte{0x23}}}};
  state.commit_index = 2U;
  state.applied_index = 1U;
  state.snapshot.manifest_generation = 9U;
  state.snapshot.part_set_checksum.fill(std::byte{0xa5});
  state.snapshot.configuration_index = 7U;
  state.snapshot.voters = {1U, 2U, 3U};
  const GroupPersistentState persistent{group_id(std::byte{1}), 17U, state};

  auto encoded = encode_multiplexed_log_record_v1(persistent);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto header = inspect_multiplexed_log_record_header_v1(
      common::ByteView{*encoded}.first(kMultiplexedLogHeaderSize));
  ASSERT_TRUE(header.has_value()) << header.error().to_string();
  EXPECT_EQ(header->encoded_size, encoded->size());
  EXPECT_EQ(header->physical_sequence, persistent.physical_sequence);
  EXPECT_EQ(header->group_id, persistent.group_id);
  EXPECT_EQ(header->format_minor, 1U);
  auto decoded = decode_multiplexed_log_record_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->persistent, persistent);
  EXPECT_EQ(decoded->encoded_size, encoded->size());

  (*encoded)[encoded->size() - 5U] ^= std::byte{1U};
  decoded = decode_multiplexed_log_record_v1(*encoded);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);
}

TEST(MultiplexedLogTest, DecodesLegacyMinorZeroWithoutSnapshotMembershipCheckpoint) {
  PersistentState state{};
  state.current_term = 1U;
  state.log = {LogEntry{1U, 1U, 1U, {std::byte{0x33U}}}};
  state.commit_index = 1U;
  auto encoded =
      encode_multiplexed_log_record_v1(GroupPersistentState{group_id(std::byte{2U}), 1U, state});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  constexpr std::size_t checkpoint_offset = kMultiplexedLogHeaderSize + 88U;
  encoded->erase(encoded->begin() + static_cast<std::ptrdiff_t>(checkpoint_offset),
                 encoded->begin() + static_cast<std::ptrdiff_t>(checkpoint_offset + 16U));
  const auto write_u16 = [&](const std::size_t offset, const std::uint16_t value) {
    common::ByteWriter writer{common::MutableByteView{*encoded}.subspan(offset, sizeof(value))};
    ASSERT_TRUE(writer.write_u16_le(value).is_ok());
  };
  const auto write_u32 = [&](const std::size_t offset, const std::uint32_t value) {
    common::ByteWriter writer{common::MutableByteView{*encoded}.subspan(offset, sizeof(value))};
    ASSERT_TRUE(writer.write_u32_le(value).is_ok());
  };
  write_u16(10U, 0U);
  write_u32(16U, static_cast<std::uint32_t>(encoded->size()));
  write_u32(20U, static_cast<std::uint32_t>(encoded->size() - kMultiplexedLogHeaderSize -
                                            kMultiplexedLogTrailerSize));
  write_u32(48U, common::crc32c(common::ByteView{*encoded}.subspan(
                     kMultiplexedLogHeaderSize,
                     encoded->size() - kMultiplexedLogHeaderSize - kMultiplexedLogTrailerSize)));
  write_u32(52U, 0U);
  write_u32(52U, common::crc32c(common::ByteView{*encoded}.first(kMultiplexedLogHeaderSize)));
  write_u32(encoded->size() - kMultiplexedLogTrailerSize,
            common::crc32c(
                common::ByteView{*encoded}.first(encoded->size() - kMultiplexedLogTrailerSize)));

  auto decoded = decode_multiplexed_log_record_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->persistent.state, state);
}

} // namespace
} // namespace chronos::raft
