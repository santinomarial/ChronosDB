#include "chronos/common/crc32c.hpp"
#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

[[nodiscard]] MultiTabletSubscriptionCheckpoint fixture() {
  const schema::TabletId tablet_a = identifier<schema::TabletId>(std::byte{1});
  const schema::TabletId tablet_b = identifier<schema::TabletId>(std::byte{2});
  const wal::WalId wal_a = wal_id(std::byte{3});
  const wal::WalId wal_b = wal_id(std::byte{4});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{5});
  PlanFingerprint plan{};
  plan.fill(std::byte{6});
  return {uuid(std::byte{7}),
          identifier<schema::TableId>(std::byte{8}),
          plan,
          schema_id,
          schema::SchemaVersion::initial(),
          {{{tablet_a, wal_a, 2U}, 0U}, {{tablet_b, wal_b, 1U}, 0U}},
          {{{tablet_a, wal_a, 1U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kUpsert,
            {std::byte{9}},
            {std::byte{10}}},
           {{tablet_b, wal_b, 1U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kDelete,
            {std::byte{11}},
            {}},
           {{tablet_a, wal_a, 2U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kUpsert,
            {std::byte{12}, std::byte{13}},
            {std::byte{14}}}}};
}

[[nodiscard]] std::uint64_t fnv1a(const common::ByteView bytes) noexcept {
  std::uint64_t value = 14695981039346656037ULL;
  for (const std::byte byte : bytes) {
    value ^= std::to_integer<std::uint8_t>(byte);
    value *= 1099511628211ULL;
  }
  return value;
}

void rewrite_crc(std::vector<std::byte>& bytes) {
  const std::uint32_t crc = common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U));
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[bytes.size() - 4U + index] = static_cast<std::byte>((crc >> (index * 8U)) & 0xffU);
}

TEST(MultiTabletSubscriptionCheckpointTest, HasStableBytesAndRoundTripsExactState) {
  const MultiTabletSubscriptionCheckpoint checkpoint = fixture();
  const auto encoded = encode_multi_tablet_subscription_checkpoint_v1(checkpoint);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), 474U);
  EXPECT_EQ(fnv1a(*encoded), 5'820'888'857'849'837'899ULL);
  const auto decoded = decode_multi_tablet_subscription_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, checkpoint);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint(*encoded), decoded);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint_v2(*encoded).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(MultiTabletSubscriptionCheckpointTest, V2RoundTripsMixedWalAndRaftState) {
  MultiTabletSubscriptionCheckpoint checkpoint = fixture();
  const common::Uuid raft_group = uuid(std::byte{4});
  checkpoint.sources[1].latest_position =
      SourcePosition::raft(checkpoint.sources[1].latest_position.tablet_id, raft_group, 1U);
  checkpoint.retained_changes[1].position =
      SourcePosition::raft(checkpoint.retained_changes[1].position.tablet_id, raft_group, 1U);

  const auto encoded = encode_multi_tablet_subscription_checkpoint_v2(checkpoint);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), 514U);
  EXPECT_EQ(fnv1a(*encoded), 3'703'796'810'124'028'601ULL);
  ASSERT_GT(encoded->size(), 347U);
  EXPECT_EQ((*encoded)[7], std::byte{'2'});
  EXPECT_EQ((*encoded)[144], std::byte{1});
  EXPECT_EQ((*encoded)[200], std::byte{2});
  EXPECT_EQ((*encoded)[256], std::byte{1});
  EXPECT_EQ((*encoded)[346], std::byte{2});

  const auto decoded = decode_multi_tablet_subscription_checkpoint_v2(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, checkpoint);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint(*encoded), decoded);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint_v1(*encoded).error().code(),
            common::StatusCode::kNotSupported);

  auto unsupported_kind = *encoded;
  unsupported_kind[200] = std::byte{3};
  rewrite_crc(unsupported_kind);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint(unsupported_kind).error().code(),
            common::StatusCode::kNotSupported);
  auto unsupported_flags = *encoded;
  unsupported_flags[201] = std::byte{1};
  rewrite_crc(unsupported_flags);
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint(unsupported_flags).error().code(),
            common::StatusCode::kNotSupported);
  auto unknown_version = *encoded;
  unknown_version[8] = std::byte{3};
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint(unknown_version).error().code(),
            common::StatusCode::kNotSupported);

  const BoundMultiTabletSubscriptionCheckpoint bound{7U, checkpoint};
  const auto bound_bytes = encode_bound_multi_tablet_subscription_checkpoint_v2(bound);
  ASSERT_TRUE(bound_bytes.has_value()) << bound_bytes.error().to_string();
  EXPECT_EQ((*bound_bytes)[7], std::byte{'2'});
  EXPECT_EQ(decode_bound_multi_tablet_subscription_checkpoint(*bound_bytes).value(), bound);
}

TEST(MultiTabletSubscriptionCheckpointTest, RejectsCorruptionAndDiscontinuousState) {
  MultiTabletSubscriptionCheckpoint checkpoint = fixture();
  auto encoded = encode_multi_tablet_subscription_checkpoint_v1(checkpoint).value();
  encoded[64] ^= std::byte{1};
  EXPECT_EQ(decode_multi_tablet_subscription_checkpoint_v1(encoded).error().code(),
            common::StatusCode::kCorruption);

  checkpoint.retained_changes.erase(checkpoint.retained_changes.begin() + 1);
  EXPECT_FALSE(encode_multi_tablet_subscription_checkpoint_v1(checkpoint).has_value());

  checkpoint = fixture();
  checkpoint.sources.front().latest_position = SourcePosition::raft(
      checkpoint.sources.front().latest_position.tablet_id, uuid(std::byte{3}), 2U);
  const auto raft_rejected = encode_multi_tablet_subscription_checkpoint_v1(checkpoint);
  ASSERT_FALSE(raft_rejected.has_value());
  EXPECT_EQ(raft_rejected.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(MultiTabletSubscriptionCheckpointTest, HonorsExactEncodedSizeLimit) {
  MultiTabletSubscriptionCheckpointCodecLimits limits;
  limits.maximum_checkpoint_bytes = 474U;
  EXPECT_TRUE(encode_multi_tablet_subscription_checkpoint_v1(fixture(), limits).has_value());
  limits.maximum_checkpoint_bytes = 473U;
  const auto rejected = encode_multi_tablet_subscription_checkpoint_v1(fixture(), limits);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);

  MultiTabletSubscriptionCheckpoint terminal = fixture();
  terminal.retained_changes.clear();
  for (auto& source : terminal.sources)
    source.expired_through_sequence = source.latest_position.record_sequence;
  terminal.plan_schema_compatible = false;
  limits.maximum_checkpoint_bytes = 228U;
  EXPECT_TRUE(encode_multi_tablet_subscription_checkpoint_v1(terminal, limits).has_value());
  limits.maximum_checkpoint_bytes = 227U;
  const auto terminal_rejected = encode_multi_tablet_subscription_checkpoint_v1(terminal, limits);
  ASSERT_FALSE(terminal_rejected.has_value());
  EXPECT_EQ(terminal_rejected.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(MultiTabletSubscriptionCheckpointTest, BindsDurableGenerationAroundNestedState) {
  const BoundMultiTabletSubscriptionCheckpoint checkpoint{3U, fixture()};
  auto encoded = encode_bound_multi_tablet_subscription_checkpoint_v1(checkpoint);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_bound_multi_tablet_subscription_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, checkpoint);
  (*encoded)[24] ^= std::byte{1};
  EXPECT_EQ(decode_bound_multi_tablet_subscription_checkpoint_v1(*encoded).error().code(),
            common::StatusCode::kCorruption);
}

TEST(MultiTabletSubscriptionCheckpointTest, MinorOnePersistsTerminalSchemaState) {
  MultiTabletSubscriptionCheckpoint checkpoint = fixture();
  checkpoint.retained_changes.clear();
  for (auto& source : checkpoint.sources)
    source.expired_through_sequence = source.latest_position.record_sequence;
  checkpoint.plan_schema_compatible = false;

  const auto encoded = encode_multi_tablet_subscription_checkpoint_v1(checkpoint);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_GT(encoded->size(), 120U);
  EXPECT_EQ((*encoded)[10], std::byte{1});
  EXPECT_EQ((*encoded)[11], std::byte{0});
  EXPECT_EQ((*encoded)[120], std::byte{1});
  const auto decoded = decode_multi_tablet_subscription_checkpoint_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, checkpoint);
}

} // namespace
} // namespace chronos::live
