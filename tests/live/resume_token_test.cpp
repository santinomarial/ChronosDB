#include "chronos/common/status.hpp"
#include "chronos/live/resume_token.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  auto value = Identifier::from_uuid(uuid(seed));
  EXPECT_TRUE(value.has_value());
  return *value;
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

[[nodiscard]] ResumeTokenMacKey key() {
  ResumeTokenMacKey value{};
  value.fill(std::byte{0xa5});
  return value;
}

[[nodiscard]] std::uint64_t fnv1a(const common::ByteView bytes) noexcept {
  std::uint64_t value = 14695981039346656037ULL;
  for (const std::byte byte : bytes) {
    value ^= std::to_integer<std::uint8_t>(byte);
    value *= 1099511628211ULL;
  }
  return value;
}

TEST(ResumeTokenTest, V1RoundTripsWalIdentityAndPositionState) {
  PlanFingerprint fingerprint{};
  fingerprint.fill(std::byte{0x37});
  const ResumeToken token{
      uuid(std::byte{1}),
      uuid(std::byte{2}),
      identifier<schema::SchemaId>(std::byte{3}),
      schema::SchemaVersion::initial(),
      42U,
      fingerprint,
      {{identifier<schema::TabletId>(std::byte{4}), wal_id(std::byte{5}), 900U}}};

  const auto encoded = encode_resume_token_v1(token, key());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(),
            kResumeTokenHeaderSize + kResumeTokenV1PositionSize + kResumeTokenMacSize);
  const auto decoded = decode_resume_token_v1(*encoded, key());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, token);

  const auto compatible = decode_resume_token(*encoded, key());
  ASSERT_TRUE(compatible.has_value()) << compatible.error().to_string();
  EXPECT_EQ(*compatible, token);
}

TEST(ResumeTokenTest, V2RoundTripsTaggedWalAndRaftSourceIdentities) {
  PlanFingerprint fingerprint{};
  fingerprint.fill(std::byte{0x37});
  const schema::TabletId wal_tablet = identifier<schema::TabletId>(std::byte{4});
  const schema::TabletId raft_tablet = identifier<schema::TabletId>(std::byte{6});
  const wal::WalId wal = wal_id(std::byte{5});
  const common::Uuid group_id = uuid(std::byte{7});
  const ResumeToken token{uuid(std::byte{1}),
                          uuid(std::byte{2}),
                          identifier<schema::SchemaId>(std::byte{3}),
                          schema::SchemaVersion::initial(),
                          42U,
                          fingerprint,
                          {SourcePosition::wal(wal_tablet, wal, 900U),
                           SourcePosition::raft(raft_tablet, group_id, 901U)}};

  const auto encoded = encode_resume_token_v2(token, key());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(),
            kResumeTokenHeaderSize + 2U * kResumeTokenV2PositionSize + kResumeTokenMacSize);
  EXPECT_EQ(fnv1a(*encoded), 6'915'174'662'588'754'402ULL);
  EXPECT_EQ((*encoded)[kResumeTokenHeaderSize + 16U], std::byte{1});
  EXPECT_TRUE(std::ranges::equal(
      std::span{*encoded}.subspan(kResumeTokenHeaderSize + 24U, common::Uuid::kSize), wal.bytes));
  const std::size_t raft_offset = kResumeTokenHeaderSize + kResumeTokenV2PositionSize;
  EXPECT_EQ((*encoded)[raft_offset + 16U], std::byte{2});
  EXPECT_TRUE(std::ranges::equal(
      std::span{*encoded}.subspan(raft_offset + 24U, common::Uuid::kSize), group_id.bytes()));

  const auto decoded = decode_resume_token_v2(*encoded, key());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, token);
  const auto compatible = decode_resume_token(*encoded, key());
  ASSERT_TRUE(compatible.has_value()) << compatible.error().to_string();
  EXPECT_EQ(*compatible, token);

  const auto wrong_version = decode_resume_token_v1(*encoded, key());
  ASSERT_FALSE(wrong_version.has_value());
  EXPECT_EQ(wrong_version.error().code(), common::StatusCode::kNotSupported);
}

TEST(ResumeTokenTest, V1RejectsRaftSourcesWithoutAliasingTheirIdentity) {
  const ResumeToken token{
      uuid(std::byte{1}),
      uuid(std::byte{2}),
      identifier<schema::SchemaId>(std::byte{3}),
      schema::SchemaVersion::initial(),
      0U,
      {},
      {SourcePosition::raft(identifier<schema::TabletId>(std::byte{4}), uuid(std::byte{5}), 9U)}};
  const auto encoded = encode_resume_token_v1(token, key());
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(ResumeTokenTest, RejectsTamperingBeforeUsingSemanticFields) {
  PlanFingerprint fingerprint{};
  const ResumeToken token{uuid(std::byte{1}),
                          uuid(std::byte{2}),
                          identifier<schema::SchemaId>(std::byte{3}),
                          schema::SchemaVersion::initial(),
                          0U,
                          fingerprint,
                          {{identifier<schema::TabletId>(std::byte{4}), wal_id(std::byte{5}), 0U}}};
  auto encoded = encode_resume_token_v1(token, key());
  ASSERT_TRUE(encoded.has_value());
  (*encoded)[80] ^= std::byte{1};

  const auto decoded = decode_resume_token_v1(*encoded, key());
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kUnauthenticated);

  encoded = encode_resume_token_v1(token, key());
  ASSERT_TRUE(encoded.has_value());
  encoded->back() ^= std::byte{1};
  const auto mac_tampered = decode_resume_token_v1(*encoded, key());
  ASSERT_FALSE(mac_tampered.has_value());
  EXPECT_EQ(mac_tampered.error().code(), common::StatusCode::kUnauthenticated);
}

TEST(ResumeTokenTest, RejectsZeroMacKeyAndExcessiveDecoderBound) {
  ResumeTokenMacKey zero{};
  const auto decoded = decode_resume_token_v1({}, zero);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kInvalidArgument);

  const auto excessive = decode_resume_token_v1({}, key(), kMaximumResumeTokenSources + 1U);
  ASSERT_FALSE(excessive.has_value());
  EXPECT_EQ(excessive.error().code(), common::StatusCode::kInvalidArgument);

  const std::vector<std::byte> oversized(kResumeTokenHeaderSize + 2U * kResumeTokenV2PositionSize +
                                         kResumeTokenMacSize);
  const auto bounded = decode_resume_token(oversized, key(), 1U);
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
