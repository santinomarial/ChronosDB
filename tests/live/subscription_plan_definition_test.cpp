#include "chronos/live/subscription_plan_definition.hpp"

#include <cstddef>
#include <gtest/gtest.h>

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

[[nodiscard]] SubscriptionPlanDefinition definition() {
  PlanFingerprint fingerprint{};
  fingerprint.fill(std::byte{5});
  return {uuid(std::byte{1}),
          identifier<schema::TableId>(std::byte{2}),
          identifier<schema::SchemaId>(std::byte{3}),
          schema::SchemaVersion::initial(),
          fingerprint,
          "SUBSCRIBE SELECT count(*) AS total FROM metrics"};
}

TEST(SubscriptionPlanDefinitionTest, RoundTripsExactSqlAndBoundIdentity) {
  const auto expected = definition();
  const auto encoded = encode_subscription_plan_definition_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kSubscriptionPlanDefinitionHeaderSize + expected.sql.size() +
                                 kSubscriptionPlanDefinitionTrailerSize);
  const auto decoded = decode_subscription_plan_definition_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(SubscriptionPlanDefinitionTest, RejectsCorruptionAndFiniteSqlLimit) {
  auto encoded = encode_subscription_plan_definition_v1(definition()).value();
  encoded[100] ^= std::byte{1};
  const auto corrupt = decode_subscription_plan_definition_v1(encoded);
  ASSERT_FALSE(corrupt.has_value());
  EXPECT_EQ(corrupt.error().code(), common::StatusCode::kCorruption);
  SubscriptionPlanDefinitionLimits limits;
  limits.maximum_sql_bytes = 4U;
  EXPECT_EQ(encode_subscription_plan_definition_v1(definition(), limits).error().code(),
            common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::live
