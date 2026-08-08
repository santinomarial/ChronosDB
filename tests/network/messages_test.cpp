#include "chronos/network/messages.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace chronos::network {
namespace {

TEST(ProtocolMessageTest, NegotiatesOnlyTheAcceptedVersionAndFiniteLimit) {
  ClientHello input{.maximum_payload_size = 4096U};
  const auto bytes = encode_client_hello(input);
  ASSERT_TRUE(bytes.has_value());
  EXPECT_EQ(bytes->size(), kHelloPayloadSize);
  const auto decoded = decode_client_hello(*bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->minimum_major, kProtocolMajor);
  EXPECT_EQ(decoded->maximum_payload_size, 4096U);
  EXPECT_FALSE(encode_client_hello({.minimum_major = 2U, .maximum_major = 1U}).has_value());
  for (std::size_t size = 0U; size < bytes->size(); ++size)
    EXPECT_FALSE(decode_client_hello(common::ByteView{*bytes}.first(size)).has_value()) << size;

  ServerHello server{.maximum_payload_size = 2048U};
  const auto server_bytes = encode_server_hello(server);
  ASSERT_TRUE(server_bytes.has_value());
  const auto server_decoded = decode_server_hello(*server_bytes);
  ASSERT_TRUE(server_decoded.has_value());
  EXPECT_EQ(server_decoded->maximum_payload_size, 2048U);
}

TEST(ProtocolMessageTest, IngestEnvelopeNamesDurabilityAndBorrowsCanonicalCommand) {
  const std::array<std::byte, 4> command{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  const auto encoded = encode_ingest_request(DurabilityMode::kLocalSync, command);
  ASSERT_TRUE(encoded.has_value());
  const auto decoded = decode_ingest_request(*encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->durability, DurabilityMode::kLocalSync);
  EXPECT_TRUE(std::ranges::equal(decoded->encoded_columnar_append, command));
  EXPECT_FALSE(encode_ingest_request(DurabilityMode::kAsync, command,
                                     {.maximum_payload_size = kIngestEnvelopeSize + 3U})
                   .has_value());
}

TEST(ProtocolMessageTest, AcknowledgementNamesEffectiveDurabilityAndExactWalPosition) {
  IngestAcknowledgement input{.requested_durability = DurabilityMode::kLocalSync,
                              .effective_durability = DurabilityMode::kLocalSync,
                              .outcome = IngestOutcome::kApplied,
                              .record_sequence = 7U,
                              .segment_number = 2U,
                              .byte_offset = 4096U};
  const auto encoded = encode_ingest_acknowledgement(input);
  ASSERT_TRUE(encoded.has_value());
  const auto decoded = decode_ingest_acknowledgement(*encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, input);
  input.outcome = IngestOutcome::kMatchingRetry;
  EXPECT_FALSE(encode_ingest_acknowledgement(input).has_value());
  input.record_sequence = 0U;
  input.segment_number = 0U;
  input.byte_offset = 0U;
  EXPECT_TRUE(encode_ingest_acknowledgement(input).has_value());
}

TEST(ProtocolMessageTest, QueryAndErrorTextRequireExactValidUtf8) {
  const auto query = encode_query_request("SELECT value FROM metrics");
  ASSERT_TRUE(query.has_value());
  EXPECT_TRUE(std::ranges::equal(*decode_query_request(*query),
                                 std::as_bytes(std::span{"SELECT value FROM metrics", 25U})));
  EXPECT_FALSE(encode_query_request("").has_value());
  const std::string invalid_utf8(1U, static_cast<char>(0xff));
  EXPECT_FALSE(encode_query_request(invalid_utf8).has_value());

  const auto error = encode_error_message(ProtocolErrorCode::kOverloaded, "queue full");
  ASSERT_TRUE(error.has_value());
  const auto decoded = decode_error_message(*error);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->code, ProtocolErrorCode::kOverloaded);
  EXPECT_TRUE(std::ranges::equal(decoded->message, std::as_bytes(std::span{"queue full", 10U})));
  std::vector<std::byte> trailing = *error;
  trailing.push_back(std::byte{0});
  EXPECT_FALSE(decode_error_message(trailing).has_value());
}

TEST(ProtocolMessageTest, RejectsCorruptReservedAndLengthFields) {
  std::vector<std::byte> hello = *encode_client_hello({});
  hello.back() = std::byte{1};
  EXPECT_FALSE(decode_client_hello(hello).has_value());
  std::vector<std::byte> query = *encode_query_request("SELECT 1");
  query[4] = std::byte{0xff};
  EXPECT_FALSE(decode_query_request(query).has_value());
  std::vector<std::byte> acknowledgement =
      *encode_ingest_acknowledgement({.outcome = IngestOutcome::kMatchingRetry});
  acknowledgement[5] = std::byte{1};
  EXPECT_FALSE(decode_ingest_acknowledgement(acknowledgement).has_value());
}

TEST(ProtocolMessageTest, QueryResultBatchRoundTripsTypesNullsAndZeroRows) {
  const schema::LogicalType int64 =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const schema::LogicalType string =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::array<QueryResultColumn, 2> columns{
      QueryResultColumn{.name = "count", .type = int64, .nullable = false},
      QueryResultColumn{.name = "label", .type = string, .nullable = true}};
  const std::array<std::byte, 8> one{std::byte{1}};
  const std::array<std::byte, 8> two{std::byte{2}};
  const auto alpha = std::as_bytes(std::span{"alpha", 5U});
  const std::array<QueryResultCell, 4> cells{
      QueryResultCell{.value = one}, QueryResultCell{.value = alpha}, QueryResultCell{.value = two},
      QueryResultCell{.is_null = true}};
  const auto encoded = encode_query_result_batch(2U, columns, cells);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_query_result_batch(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->row_count(), 2U);
  ASSERT_EQ(decoded->columns().size(), 2U);
  EXPECT_EQ(decoded->columns()[1].type.kind(), schema::LogicalTypeKind::kString);
  ASSERT_NE(decoded->cell(1U, 1U), nullptr);
  EXPECT_TRUE(decoded->cell(1U, 1U)->is_null);
  EXPECT_TRUE(std::ranges::equal(decoded->cell(0U, 1U)->value, alpha));
  EXPECT_EQ(decoded->cell(2U, 0U), nullptr);

  const auto empty = encode_query_result_batch(0U, columns, {});
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(decode_query_result_batch(*empty)->row_count(), 0U);
}

TEST(ProtocolMessageTest, QueryResultRejectsHostileShapesAndNoncanonicalCells) {
  const schema::LogicalType boolean =
      schema::LogicalType::create(schema::LogicalTypeKind::kBool).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "ok", .type = boolean, .nullable = false}};
  const std::array<std::byte, 1> invalid_bool{std::byte{2}};
  EXPECT_FALSE(encode_query_result_batch(1U, columns, {}).has_value());
  EXPECT_FALSE(
      encode_query_result_batch(1U, columns, std::array{QueryResultCell{.value = invalid_bool}})
          .has_value());
  const std::array<std::byte, 1> valid_bool{std::byte{1}};
  std::vector<std::byte> encoded =
      *encode_query_result_batch(1U, columns, std::array{QueryResultCell{.value = valid_bool}});
  encoded.push_back(std::byte{0});
  EXPECT_FALSE(decode_query_result_batch(encoded).has_value());
  encoded.pop_back();
  encoded[12] = std::byte{0xff};
  EXPECT_FALSE(decode_query_result_batch(encoded).has_value());

  const schema::LogicalType decimal = schema::LogicalType::decimal(1U, 0U).value();
  const std::array<QueryResultColumn, 1> decimal_column{
      QueryResultColumn{.name = "d", .type = decimal, .nullable = false}};
  std::array<std::byte, 16> ten{};
  ten.front() = std::byte{10};
  EXPECT_FALSE(
      encode_query_result_batch(1U, decimal_column, std::array{QueryResultCell{.value = ten}})
          .has_value());
}

} // namespace
} // namespace chronos::network
