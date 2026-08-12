#include "chronos/common/crc32c.hpp"
#include "chronos/raft/metadata_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 36U, 0U);
  store_u32(bytes, 36U, common::crc32c(common::ByteView{bytes}.first(kMetadataCommandHeaderSize)));
  store_u32(bytes, bytes.size() - kMetadataCommandTrailerSize, 0U);
  store_u32(
      bytes, bytes.size() - kMetadataCommandTrailerSize,
      common::crc32c(common::ByteView{bytes}.first(bytes.size() - kMetadataCommandTrailerSize)));
}

TEST(MetadataCommandCodecTest, RoundTripsEveryCommandAndCanonicalizesReplicas) {
  const auto table = id<schema::TableId>(1U);
  const auto schema_id = id<schema::SchemaId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  const std::vector<MetadataCommand> commands{
      ClusterNodeMetadata{7U, "node-7.example:9000"},
      SchemaMetadata{table, schema_id, schema::SchemaVersion::initial()},
      TabletPlacementMetadata{table, tablet, 11U, {3U, 1U, 2U}, 2U},
      RetentionMetadata{table, 1'000'000, 4096U},
      TablePolicyMetadata{table, 60'000'000'000LL, 86'400'000'000'000LL, 3'600'000'000'000LL,
                          5'000'000'000LL, 8192U},
  };
  for (const MetadataCommand& command : commands) {
    auto encoded = encode_metadata_command_v1(command);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    EXPECT_GE(encoded->size(), kMetadataCommandHeaderSize + kMetadataCommandTrailerSize);
    auto decoded = decode_metadata_command_v1(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(decoded->index(), command.index());
    if (const auto* placement = std::get_if<TabletPlacementMetadata>(&*decoded)) {
      EXPECT_EQ(placement->replicas, (std::vector<NodeId>{1U, 2U, 3U}));
    } else {
      EXPECT_EQ(*decoded, command);
    }
    EXPECT_EQ(encode_metadata_command_v1(*decoded).value(), *encoded);
  }
}

TEST(MetadataCommandCodecTest, RejectsDamageUnknownVersionAndRuntimeLimits) {
  auto encoded = encode_metadata_command_v1(ClusterNodeMetadata{1U, "node-1"}).value();
  encoded[kMetadataCommandHeaderSize] ^= std::byte{0x01U};
  EXPECT_EQ(decode_metadata_command_v1(encoded).error().code(), common::StatusCode::kCorruption);

  encoded = encode_metadata_command_v1(ClusterNodeMetadata{1U, "node-1"}).value();
  encoded[8U] = std::byte{2U};
  refresh_checksums(encoded);
  EXPECT_EQ(decode_metadata_command_v1(encoded).error().code(), common::StatusCode::kNotSupported);

  encoded = encode_metadata_command_v1(
                TablePolicyMetadata{id<schema::TableId>(1U), 100, 1000, 500, 10, 100U})
                .value();
  encoded[encoded.size() - kMetadataCommandTrailerSize - 1U] = std::byte{1U};
  store_u32(encoded, 32U,
            common::crc32c(common::ByteView{encoded}.subspan(
                kMetadataCommandHeaderSize,
                encoded.size() - kMetadataCommandHeaderSize - kMetadataCommandTrailerSize)));
  refresh_checksums(encoded);
  EXPECT_EQ(decode_metadata_command_v1(encoded).error().code(), common::StatusCode::kCorruption);

  EXPECT_EQ(
      encode_metadata_command_v1(ClusterNodeMetadata{1U, "long"}, {.maximum_endpoint_bytes = 3U})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(decode_metadata_command_v1(
                encode_metadata_command_v1(ClusterNodeMetadata{1U, "node-1"}).value(),
                {.maximum_command_bytes = kMetadataCommandHeaderSize})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
