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

static_assert(sizeof(SubscriptionEndReason) == sizeof(std::uint8_t));

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

[[nodiscard]] std::uint64_t fnv1a64(const common::ByteView value) noexcept {
  std::uint64_t hash = 1'469'598'103'934'665'603ULL;
  for (const std::byte byte : value) {
    hash ^= std::to_integer<std::uint8_t>(byte);
    hash *= 1'099'511'628'211ULL;
  }
  return hash;
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
  ASSERT_GE(end->size(), 4U);
  EXPECT_EQ((*end)[2U], std::byte{1U});
  EXPECT_EQ((*end)[3U], std::byte{0U});
  const auto decoded_end = decode_subscription_end(*end);
  ASSERT_TRUE(decoded_end.has_value());
  EXPECT_EQ(decoded_end->reason, SubscriptionEndReason::kCancelled);
  EXPECT_EQ(decoded_end->safe_delivery_sequence, 41U);

  std::vector<std::byte> out_of_range_reason = *end;
  out_of_range_reason[3U] = std::byte{1U};
  EXPECT_FALSE(decode_subscription_end(out_of_range_reason).has_value());
}

TEST(SubscriptionMessageTest, RoundTripsCanonicalUpsertAndDeleteChanges) {
  SubscriptionSourceId log{};
  log.fill(std::byte{4});
  const std::array<std::byte, 2> key{std::byte{5}, std::byte{6}};
  const std::array<std::byte, 3> body{std::byte{7}, std::byte{8}, std::byte{9}};
  const SubscriptionChangeView upsert{.operation = SubscriptionChangeOperation::kUpsert,
                                      .delivery_sequence = 12U,
                                      .tablet_id = identifier<schema::TabletId>(std::byte{2}),
                                      .source_kind = SubscriptionSourceKind::kWal,
                                      .source_id = log,
                                      .source_sequence = 33U,
                                      .schema_id = identifier<schema::SchemaId>(std::byte{3}),
                                      .schema_version = schema::SchemaVersion::initial(),
                                      .result_key = key,
                                      .payload = body};
  const auto encoded = encode_subscription_change(upsert);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), 89U);
  EXPECT_EQ(fnv1a64(*encoded), 11'767'502'280'752'276'177ULL);
  EXPECT_EQ((*encoded)[0], std::byte{1U});
  EXPECT_EQ((*encoded)[3], std::byte{0U});
  const auto decoded = decode_subscription_change(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->operation, SubscriptionChangeOperation::kUpsert);
  EXPECT_EQ(decoded->delivery_sequence, 12U);
  EXPECT_EQ(decoded->tablet_id, upsert.tablet_id);
  EXPECT_EQ(decoded->source_kind, SubscriptionSourceKind::kWal);
  EXPECT_EQ(decoded->source_id, log);
  EXPECT_EQ(decoded->source_sequence, 33U);
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

TEST(SubscriptionMessageTest, ProtocolOnePointTwoTagsWalAndRaftWithoutChangingEnvelopeSize) {
  const SubscriptionProtocolContext context{.protocol_minor = 2U};
  SubscriptionSourceId source{};
  source.fill(std::byte{4});
  const std::array<std::byte, 2> key{std::byte{5}, std::byte{6}};
  const std::array<std::byte, 1> body{std::byte{7}};
  SubscriptionChangeView change{.operation = SubscriptionChangeOperation::kUpsert,
                                .delivery_sequence = 12U,
                                .tablet_id = identifier<schema::TabletId>(std::byte{2}),
                                .source_kind = SubscriptionSourceKind::kRaft,
                                .source_id = source,
                                .source_sequence = 33U,
                                .schema_id = identifier<schema::SchemaId>(std::byte{3}),
                                .schema_version = schema::SchemaVersion::initial(),
                                .result_key = key,
                                .payload = body};

  const auto encoded = encode_subscription_change(change, context);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), kSubscriptionChangeEnvelopeSize + key.size() + body.size());
  EXPECT_EQ(fnv1a64(*encoded), 12'794'394'718'722'335'323ULL);
  EXPECT_EQ((*encoded)[0], std::byte{2U});
  EXPECT_EQ((*encoded)[1], std::byte{0U});
  EXPECT_EQ((*encoded)[3], std::byte{2U});
  EXPECT_TRUE(std::ranges::equal(source.begin(), source.end(), encoded->begin() + 28U,
                                 encoded->begin() + 44U));

  const auto decoded = decode_subscription_change(*encoded, context);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_kind, SubscriptionSourceKind::kRaft);
  EXPECT_EQ(decoded->source_id, source);
  EXPECT_EQ(decoded->source_sequence, 33U);

  EXPECT_FALSE(decode_subscription_change(*encoded).has_value());
  change.source_kind = SubscriptionSourceKind::kWal;
  const auto wal = encode_subscription_change(change, context);
  ASSERT_TRUE(wal.has_value());
  EXPECT_EQ((*wal)[3], std::byte{1U});
  EXPECT_FALSE(
      decode_subscription_change(*encode_subscription_change(change), context).has_value());
}

TEST(SubscriptionMessageTest, FrozenProtocolsRejectRaftAndSourceTaggedPayloadsFailClosed) {
  SubscriptionSourceId source{};
  source.fill(std::byte{4});
  const std::array<std::byte, 1> key{std::byte{5}};
  const std::array<std::byte, 1> body{std::byte{6}};
  const SubscriptionChangeView change{.operation = SubscriptionChangeOperation::kUpsert,
                                      .delivery_sequence = 1U,
                                      .tablet_id = identifier<schema::TabletId>(std::byte{2}),
                                      .source_kind = SubscriptionSourceKind::kRaft,
                                      .source_id = source,
                                      .source_sequence = 3U,
                                      .schema_id = identifier<schema::SchemaId>(std::byte{3}),
                                      .schema_version = schema::SchemaVersion::initial(),
                                      .result_key = key,
                                      .payload = body};
  EXPECT_FALSE(encode_subscription_change(change).has_value());
  EXPECT_FALSE(encode_subscription_change(change, {.protocol_major = kProtocolV2Major,
                                                   .protocol_minor = 0U,
                                                   .feature_bits = kProtocolV1SubscriptionFeature})
                   .has_value());
  SubscriptionChangeView protocol_two_wal = change;
  protocol_two_wal.source_kind = SubscriptionSourceKind::kWal;
  const SubscriptionProtocolContext protocol_two{.protocol_major = kProtocolV2Major,
                                                 .protocol_minor = 0U,
                                                 .feature_bits = kProtocolV1SubscriptionFeature};
  const auto frozen = encode_subscription_change(protocol_two_wal, protocol_two);
  ASSERT_TRUE(frozen.has_value());
  EXPECT_EQ((*frozen)[0], std::byte{1U});
  EXPECT_EQ((*frozen)[3], std::byte{0U});
  EXPECT_TRUE(decode_subscription_change(*frozen, protocol_two).has_value());

  auto encoded = encode_subscription_change(change, {.protocol_minor = 2U}).value();
  encoded[3] = std::byte{3U};
  EXPECT_FALSE(decode_subscription_change(encoded, {.protocol_minor = 2U}).has_value());
  EXPECT_FALSE(
      encode_subscription_change(change, {.protocol_minor = 2U, .feature_bits = 0U}).has_value());
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
