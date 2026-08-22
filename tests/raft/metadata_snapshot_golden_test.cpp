#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

struct MetadataSnapshotGoldenCase {
  std::string_view filename;
  std::uint8_t minor;
  std::size_t encoded_size;
  MetadataApplicationSnapshot snapshot;
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

[[nodiscard]] TabletGroupBindingMetadata golden_binding() {
  common::Uuid::Bytes tablet_bytes{};
  tablet_bytes.front() = std::byte{1U};
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{2U};
  return {schema::TabletId::from_bytes(tablet_bytes).value(), GroupId{group_bytes}};
}

[[nodiscard]] MetadataApplicationSnapshot golden_snapshot(const bool minor_one) {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  SnapshotMetadata metadata{.last_included_index = 8U,
                            .last_included_term = 3U,
                            .manifest_generation = 8U,
                            .part_set_checksum = {},
                            .configuration_index = 6U,
                            .voters = {1U, 2U, 3U}};
  if (minor_one) {
    metadata.part_set_checksum = {
        std::byte{0xc0U}, std::byte{0x05U}, std::byte{0x91U}, std::byte{0x19U}, std::byte{0xe7U},
        std::byte{0x47U}, std::byte{0x38U}, std::byte{0x55U}, std::byte{0xb2U}, std::byte{0x2aU},
        std::byte{0x30U}, std::byte{0x23U}, std::byte{0x65U}, std::byte{0x63U}, std::byte{0x96U},
        std::byte{0xcbU}, std::byte{0x19U}, std::byte{0x8eU}, std::byte{0x3eU}, std::byte{0x6dU},
        std::byte{0x05U}, std::byte{0xe3U}, std::byte{0x14U}, std::byte{0xc2U}, std::byte{0xd8U},
        std::byte{0x37U}, std::byte{0x85U}, std::byte{0xa5U}, std::byte{0x5bU}, std::byte{0x1bU},
        std::byte{0x03U}, std::byte{0xafU}};
  } else {
    metadata.part_set_checksum = {
        std::byte{0x2fU}, std::byte{0x25U}, std::byte{0x2dU}, std::byte{0x5fU}, std::byte{0x98U},
        std::byte{0x12U}, std::byte{0x66U}, std::byte{0x97U}, std::byte{0x42U}, std::byte{0x7cU},
        std::byte{0x23U}, std::byte{0x9fU}, std::byte{0x37U}, std::byte{0x81U}, std::byte{0xb2U},
        std::byte{0xa2U}, std::byte{0xf8U}, std::byte{0xbcU}, std::byte{0x02U}, std::byte{0x4eU},
        std::byte{0x04U}, std::byte{0x80U}, std::byte{0xe2U}, std::byte{0x72U}, std::byte{0xddU},
        std::byte{0x21U}, std::byte{0x2eU}, std::byte{0x38U}, std::byte{0x40U}, std::byte{0xc0U},
        std::byte{0xe2U}, std::byte{0x5bU}};
  }
  auto node_payload = encode_metadata_command_v1(ClusterNodeMetadata{7U, "n"}).value();
  MetadataApplicationSnapshot snapshot{.group_id = GroupId{group_bytes},
                                       .raft_snapshot = std::move(metadata),
                                       .entries = {{.index = 2U,
                                                    .term = 1U,
                                                    .type = kRaftMetadataCommandEntryType,
                                                    .payload = std::move(node_payload)}}};
  if (minor_one) {
    snapshot.entries.push_back(
        {.index = 8U,
         .term = 3U,
         .type = kRaftTabletGroupBindingEntryType,
         .payload = encode_tablet_group_binding_v1(golden_binding()).value()});
  }
  return snapshot;
}

[[nodiscard]] std::vector<MetadataSnapshotGoldenCase> golden_cases() {
  std::vector<MetadataSnapshotGoldenCase> cases;
  cases.push_back({"metadata-application-snapshot-minor-0.hex", 0U, 260U, golden_snapshot(false)});
  cases.push_back({"metadata-application-snapshot-minor-1.hex", 1U, 380U, golden_snapshot(true)});
  return cases;
}

TEST(MetadataSnapshotGoldenTest, BothMinorVersionsHaveCompilerIndependentCanonicalBytes) {
  for (const MetadataSnapshotGoldenCase& test_case : golden_cases()) {
    SCOPED_TRACE(test_case.filename);
    const std::vector<std::byte> fixture = load_hex_fixture(test_case.filename);
    ASSERT_EQ(fixture.size(), test_case.encoded_size);
    ASSERT_GE(fixture.size(), 12U);
    EXPECT_EQ(std::to_integer<std::uint8_t>(fixture[10U]), test_case.minor);
    EXPECT_EQ(fixture[11U], std::byte{0U});

    auto encoded = encode_metadata_application_snapshot_v1(test_case.snapshot);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    EXPECT_EQ(*encoded, fixture);

    auto decoded = decode_metadata_application_snapshot_v1(fixture);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(*decoded, test_case.snapshot);
    auto node = decode_metadata_command_v1(decoded->entries.front().payload);
    ASSERT_TRUE(node.has_value()) << node.error().to_string();
    const MetadataCommand expected_node = ClusterNodeMetadata{7U, "n"};
    EXPECT_EQ(*node, expected_node);
    if (test_case.minor == 1U) {
      auto binding = decode_tablet_group_binding_v1(decoded->entries.back().payload);
      ASSERT_TRUE(binding.has_value()) << binding.error().to_string();
      EXPECT_EQ(*binding, golden_binding());
    }
  }
}

} // namespace
} // namespace chronos::raft
