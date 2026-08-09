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
  const GroupPersistentState persistent{group_id(std::byte{1}), 17U, state};

  auto encoded = encode_multiplexed_log_record_v1(persistent);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_multiplexed_log_record_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->persistent, persistent);
  EXPECT_EQ(decoded->encoded_size, encoded->size());

  (*encoded)[encoded->size() - 5U] ^= std::byte{1U};
  decoded = decode_multiplexed_log_record_v1(*encoded);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
