#include "chronos/common/status.hpp"
#include "chronos/raft/multiplexed_log.hpp"
#include "chronos/raft/node.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-version-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::vector<std::byte> load_hex_fixture(const std::string_view filename) {
  std::ifstream input{std::string{CHRONOS_RAFT_FIXTURE_DIR} + "/" + std::string{filename}};
  std::string hex;
  if (!(input >> hex))
    return {};
  std::string trailing;
  if (input >> trailing || hex.empty() || hex.size() % 2U != 0U)
    return {};

  const auto digit = [](const char value) -> unsigned int {
    if (value >= '0' && value <= '9')
      return static_cast<unsigned int>(value - '0');
    if (value >= 'a' && value <= 'f')
      return static_cast<unsigned int>(value - 'a') + 10U;
    return 16U;
  };
  std::vector<std::byte> bytes;
  bytes.reserve(hex.size() / 2U);
  for (std::size_t offset = 0U; offset < hex.size(); offset += 2U) {
    const unsigned int high = digit(hex[offset]);
    const unsigned int low = digit(hex[offset + 1U]);
    if (high > 15U || low > 15U)
      return {};
    bytes.push_back(static_cast<std::byte>((high << 4U) | low));
  }
  return bytes;
}

[[nodiscard]] PersistentState golden_state(const bool membership_checkpoint) {
  PersistentState state{};
  state.current_term = 4U;
  state.voted_for = 2U;
  state.commit_index = 7U;
  state.applied_index = 6U;
  state.snapshot.last_included_index = 5U;
  state.snapshot.last_included_term = 3U;
  state.snapshot.manifest_generation = 9U;
  state.snapshot.part_set_checksum.fill(std::byte{0xa5U});
  if (membership_checkpoint)
    state.snapshot.voters = {1U, 2U, 3U};
  state.log = {LogEntry{6U, 3U, 1U, {std::byte{0x11U}, std::byte{0x22U}}},
               LogEntry{7U, 4U, 2U, {}}};
  return state;
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

TEST(MultiplexedLogTest, MatchesIndependentMinorOneGoldenAndUpgradesMinorZeroGolden) {
  const GroupId group = group_id(std::byte{0x41U});
  const std::vector<std::byte> minor_zero = load_hex_fixture("multiplexed-state-minor-0.hex");
  const std::vector<std::byte> minor_one = load_hex_fixture("multiplexed-state-minor-1.hex");
  ASSERT_EQ(minor_zero.size(), 230U);
  ASSERT_EQ(minor_one.size(), 270U);

  auto legacy_header = inspect_multiplexed_log_record_header_v1(
      common::ByteView{minor_zero}.first(kMultiplexedLogHeaderSize));
  auto current_header = inspect_multiplexed_log_record_header_v1(
      common::ByteView{minor_one}.first(kMultiplexedLogHeaderSize));
  ASSERT_TRUE(legacy_header.has_value()) << legacy_header.error().to_string();
  ASSERT_TRUE(current_header.has_value()) << current_header.error().to_string();
  EXPECT_EQ(legacy_header->format_minor, 0U);
  EXPECT_EQ(current_header->format_minor, 1U);

  auto legacy = decode_multiplexed_log_record_v1(minor_zero);
  auto current = decode_multiplexed_log_record_v1(minor_one);
  ASSERT_TRUE(legacy.has_value()) << legacy.error().to_string();
  ASSERT_TRUE(current.has_value()) << current.error().to_string();
  EXPECT_EQ(legacy->persistent, (GroupPersistentState{group, 1U, golden_state(false)}));
  EXPECT_EQ(current->persistent, (GroupPersistentState{group, 1U, golden_state(true)}));

  auto recovered = RaftNode::create(2U, {1U, 2U, 3U}, legacy->persistent.state);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->persistent_state(), golden_state(true));
  auto encoded = encode_multiplexed_log_record_v1(
      GroupPersistentState{group, 1U, recovered->persistent_state()});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(*encoded, minor_one);
}

TEST(MultiplexedLogTest, RecoversLegacyDiskHistoryAndReclaimsItAfterCurrentRewrite) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 334U};
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  ASSERT_TRUE(log->close().is_ok());

  const std::vector<std::byte> legacy_fixture = load_hex_fixture("multiplexed-state-minor-0.hex");
  const std::filesystem::path first_segment = directory.path() / "raft-00000000000000000001.rlog";
  std::ofstream legacy_output{first_segment, std::ios::binary | std::ios::app};
  ASSERT_TRUE(legacy_output.is_open());
  for (const std::byte value : legacy_fixture)
    legacy_output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  legacy_output.close();
  ASSERT_TRUE(legacy_output.good());

  const GroupId group = group_id(std::byte{0x41U});
  auto reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
  EXPECT_EQ(reopened->recovery().latest_group_states.front(),
            (GroupPersistentState{group, 1U, golden_state(false)}));

  auto node =
      RaftNode::create(2U, {1U, 2U, 3U}, reopened->recovery().latest_group_states.front().state);
  ASSERT_TRUE(node.has_value()) << node.error().to_string();
  const GroupPersistentState canonical{group, 2U, node->persistent_state()};
  auto rewritten = reopened->append(canonical);
  ASSERT_TRUE(rewritten.has_value()) << rewritten.error().to_string();
  EXPECT_EQ(rewritten->segment_number, 2U);
  ASSERT_TRUE(reopened->synchronize().has_value());
  ASSERT_TRUE(reopened->close().is_ok());

  reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->recovery().record_count, 2U);
  EXPECT_EQ(reopened->recovery().segment_count, 2U);
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
  EXPECT_EQ(reopened->recovery().latest_group_states.front(), canonical);

  GroupPersistentState checkpoint = canonical;
  checkpoint.physical_sequence = 3U;
  auto reclaimed = reopened->checkpoint_and_reclaim({checkpoint});
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->base_segment_number, 3U);
  EXPECT_EQ(reclaimed->reclaimed_segments, 2U);
  EXPECT_EQ(reclaimed->reclaimed_records, 2U);
  ASSERT_TRUE(reopened->close().is_ok());

  reopened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->recovery().base_segment_number, 3U);
  EXPECT_EQ(reopened->recovery().record_count, 1U);
  EXPECT_EQ(reopened->recovery().segment_count, 1U);
  ASSERT_EQ(reopened->recovery().latest_group_states.size(), 1U);
  EXPECT_EQ(reopened->recovery().latest_group_states.front(), checkpoint);
}

TEST(MultiplexedLogTest, EncodesTheExactMaximumAndRejectsTheNextByte) {
  GroupPersistentState persistent{group_id(std::byte{3U}), 1U, {}};
  persistent.state.current_term = 1U;
  persistent.state.log = {LogEntry{1U, 1U, 1U, {}}};
  auto encoded = encode_multiplexed_log_record_v1(persistent);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::size_t maximum_entry_payload = kMaximumMultiplexedLogRecordSize - encoded->size();
  persistent.state.log.front().payload.assign(maximum_entry_payload, std::byte{0xa5U});

  encoded = encode_multiplexed_log_record_v1(persistent);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kMaximumMultiplexedLogRecordSize);

  persistent.physical_sequence = 2U;
  persistent.state.log.front().payload.push_back(std::byte{0x5aU});
  auto too_large = encode_multiplexed_log_record_v1(persistent);
  ASSERT_FALSE(too_large.has_value());
  EXPECT_EQ(too_large.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::raft
