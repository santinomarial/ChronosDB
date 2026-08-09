#include "chronos/live/resume_token.hpp"

#include "chronos/common/status.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier>
[[nodiscard]] Identifier identifier(const std::byte seed) {
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

TEST(ResumeTokenTest, RoundTripsAllBoundIdentityAndPositionState) {
  PlanFingerprint fingerprint{};
  fingerprint.fill(std::byte{0x37});
  const ResumeToken token{uuid(std::byte{1}),
                          uuid(std::byte{2}),
                          identifier<schema::SchemaId>(std::byte{3}),
                          schema::SchemaVersion::initial(),
                          42U,
                          fingerprint,
                          {{identifier<schema::TabletId>(std::byte{4}), wal_id(std::byte{5}),
                            900U}}};

  const auto encoded = encode_resume_token_v1(token, key());
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(),
            kResumeTokenHeaderSize + kResumeTokenPositionSize + kResumeTokenMacSize);
  const auto decoded = decode_resume_token_v1(*encoded, key());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, token);
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
}

TEST(ResumeTokenTest, RejectsZeroMacKeyAndExcessiveDecoderBound) {
  ResumeTokenMacKey zero{};
  const auto decoded = decode_resume_token_v1({}, zero);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::live
