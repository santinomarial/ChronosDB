#include "chronos/network/subscription_messages.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <string_view>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] common::ByteView bytes(const std::string_view value) {
  return std::as_bytes(std::span{value.data(), value.size()});
}

TEST(SubscriptionMessageTest, RoundTripsRequestReadyAndCheckpointLifecycle) {
  const common::Uuid subscription = uuid(std::byte{1});
  const auto request = encode_subscription_request({.mode = SubscriptionStartMode::kNewQuery,
                                                    .subscription_id = subscription,
                                                    .body = bytes("SELECT * FROM trades")});
  ASSERT_TRUE(request.has_value()) << request.error().to_string();
  const auto decoded_request = decode_subscription_request(*request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(decoded_request->mode, SubscriptionStartMode::kNewQuery);
  EXPECT_EQ(decoded_request->subscription_id, subscription);
  EXPECT_TRUE(std::ranges::equal(decoded_request->body, bytes("SELECT * FROM trades")));

  const std::array<std::byte, 3> token{std::byte{7}, std::byte{8}, std::byte{9}};
  const auto resume = encode_subscription_request(
      {.mode = SubscriptionStartMode::kResume, .subscription_id = subscription, .body = token});
  ASSERT_TRUE(resume.has_value());
  EXPECT_EQ(decode_subscription_request(*resume)->mode, SubscriptionStartMode::kResume);

  const auto ready = encode_subscription_ready(token);
  ASSERT_TRUE(ready.has_value());
  EXPECT_TRUE(std::ranges::equal(decode_subscription_ready(*ready)->resume_token, token));

  const auto acknowledgement = encode_subscription_acknowledgement({41U});
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_EQ(decode_subscription_acknowledgement(*acknowledgement)->delivery_sequence, 41U);

  const auto checkpoint = encode_subscription_checkpoint(
      {.acknowledged_delivery_sequence = 41U, .resume_token = token});
  ASSERT_TRUE(checkpoint.has_value());
  const auto decoded_checkpoint = decode_subscription_checkpoint(*checkpoint);
  ASSERT_TRUE(decoded_checkpoint.has_value());
  EXPECT_EQ(decoded_checkpoint->acknowledged_delivery_sequence, 41U);
  EXPECT_TRUE(std::ranges::equal(decoded_checkpoint->resume_token, token));

  const auto end = encode_subscription_end({.reason = SubscriptionEndReason::kCancelled,
                                            .safe_delivery_sequence = 41U,
                                            .resume_token = token});
  ASSERT_TRUE(end.has_value());
  const auto decoded_end = decode_subscription_end(*end);
  ASSERT_TRUE(decoded_end.has_value());
  EXPECT_EQ(decoded_end->reason, SubscriptionEndReason::kCancelled);
  EXPECT_EQ(decoded_end->safe_delivery_sequence, 41U);
}

TEST(SubscriptionMessageTest, RoundTripsCanonicalUpsertAndDeleteChanges) {
  SubscriptionLogId log{};
  log.fill(std::byte{4});
  const std::array<std::byte, 2> key{std::byte{5}, std::byte{6}};
  const std::array<std::byte, 3> body{std::byte{7}, std::byte{8}, std::byte{9}};
  const SubscriptionChangeView upsert{SubscriptionChangeOperation::kUpsert,
                                      12U,
                                      identifier<schema::TabletId>(std::byte{2}),
                                      log,
                                      33U,
                                      identifier<schema::SchemaId>(std::byte{3}),
                                      schema::SchemaVersion::initial(),
                                      key,
                                      body};
  const auto encoded = encode_subscription_change(upsert);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_subscription_change(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->operation, SubscriptionChangeOperation::kUpsert);
  EXPECT_EQ(decoded->delivery_sequence, 12U);
  EXPECT_EQ(decoded->tablet_id, upsert.tablet_id);
  EXPECT_EQ(decoded->log_id, log);
  EXPECT_EQ(decoded->record_sequence, 33U);
  EXPECT_EQ(decoded->schema_id, upsert.schema_id);
  EXPECT_TRUE(std::ranges::equal(decoded->result_key, key));
  EXPECT_TRUE(std::ranges::equal(decoded->payload, body));

  SubscriptionChangeView deletion = upsert;
  deletion.operation = SubscriptionChangeOperation::kDelete;
  deletion.payload = {};
  EXPECT_TRUE(decode_subscription_change(*encode_subscription_change(deletion)).has_value());
  deletion.payload = body;
  EXPECT_FALSE(encode_subscription_change(deletion).has_value());
}

TEST(SubscriptionMessageTest, RejectsHostileLengthsReservedFieldsAndInvalidUtf8) {
  const common::Uuid subscription = uuid(std::byte{1});
  const std::array<std::byte, 1> invalid_utf8{std::byte{0xff}};
  EXPECT_FALSE(encode_subscription_request({.mode = SubscriptionStartMode::kNewQuery,
                                            .subscription_id = subscription,
                                            .body = invalid_utf8})
                   .has_value());
  std::vector<std::byte> request =
      *encode_subscription_request({.mode = SubscriptionStartMode::kResume,
                                    .subscription_id = subscription,
                                    .body = std::array{std::byte{1}}});
  request[3] = std::byte{1};
  EXPECT_FALSE(decode_subscription_request(request).has_value());

  std::vector<std::byte> ready = *encode_subscription_ready(std::array{std::byte{1}});
  ready[4] = std::byte{0xff};
  EXPECT_FALSE(decode_subscription_ready(ready).has_value());
  EXPECT_FALSE(encode_subscription_acknowledgement({0U}).has_value());
  EXPECT_FALSE(
      validate_subscription_message_limits({.protocol = {}, .maximum_resume_token_bytes = 0U})
          .is_ok());
}

} // namespace
} // namespace chronos::network
