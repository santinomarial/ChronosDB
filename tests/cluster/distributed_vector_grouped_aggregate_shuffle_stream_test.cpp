#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleEdge edge() {
  return {.tablet_id = tablet(),
          .partition_id = 0U,
          .source_node_id = 2U,
          .target_node_id = 3U,
          .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1};
}

[[nodiscard]] query::DistributedVectorGroupedAggregateExchangeMessage
message(const std::size_t ordinal, const std::size_t count,
        const std::optional<std::uint64_t> sequence = std::nullopt) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  for (std::size_t value = 0U; value <= ordinal; ++value)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), ordinal == 0U ? "east-larger-than-SSO"
                                                                         : "west-larger-than-SSO")
                       .value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {{.query_id = uuid(1U),
           .tablet_id = tablet(),
           .sequence = sequence.value_or(ordinal + 1U),
           .group_ordinal = static_cast<std::uint32_t>(ordinal),
           .group_count = static_cast<std::uint32_t>(count),
           .terminal = ordinal + 1U == count,
           .empty = false},
          std::move(values),
          std::move(states)};
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
encoded_messages(const std::optional<std::uint64_t> first_sequence = std::nullopt) {
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    auto value = message(ordinal, 2U, ordinal == 0U ? first_sequence : std::nullopt);
    result.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         value, keys(), aggregates())
                         .value());
  }
  return result;
}

[[nodiscard]] query::EncodedDistributedVectorGroupedAggregateExchangeMessage empty_message() {
  query::DistributedVectorGroupedAggregateExchangeMessage value{{.query_id = uuid(1U),
                                                                 .tablet_id = tablet(),
                                                                 .sequence = 1U,
                                                                 .group_ordinal = 0U,
                                                                 .group_count = 0U,
                                                                 .terminal = true,
                                                                 .empty = true},
                                                                {},
                                                                {}};
  return query::encode_distributed_vector_grouped_aggregate_exchange_message(value, keys(),
                                                                             aggregates())
      .value();
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    ++calls;
    return common::Result<bool>{principal_id == 91U && claimed_node_id == 2U};
  }

  mutable std::size_t calls{};
};

TEST(DistributedVectorGroupedAggregateShuffleStreamTest,
     AuthenticatesAndPublishesOnlyOneCompleteFragmentedStream) {
  auto expected = authority();
  auto encoded = encoded_messages();
  query::QueryResourceContext sender_resources =
      query::QueryResourceContext::create(4U << 20U).value();
  auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(
                    expected, edge(), encoded, sender_resources)
                    .value();
  EXPECT_EQ(sender.frame_count(), 2U);
  EXPECT_GT(sender.encoded_bytes(), 0U);

  Authorizer authorizer;
  query::QueryResourceContext receiver_resources =
      query::QueryResourceContext::create(4U << 20U).value();
  auto receiver =
      DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
          expected, 3U, authorizer, {.authorized = true, .principal_id = 91U}, receiver_resources)
          .value();
  EXPECT_EQ(receiver.take_complete_stream().error().code(), common::StatusCode::kInvalidArgument);
  while (!sender.complete()) {
    const common::ByteView pending = sender.pending_write();
    const std::size_t count = std::min<std::size_t>(7U, pending.size());
    auto read = receiver.consume(pending.first(count));
    ASSERT_TRUE(read.has_value()) << read.error().to_string();
    EXPECT_EQ(read->consumed_bytes, count);
    ASSERT_TRUE(sender.consume_written(count).is_ok());
  }
  EXPECT_TRUE(receiver.complete());
  EXPECT_TRUE(receiver.finish_input().is_ok());
  EXPECT_EQ(authorizer.calls, 1U);
  EXPECT_EQ(receiver.accepted_frames(), 2U);
  EXPECT_EQ(receiver.accepted_bytes(), sender.encoded_bytes());
  EXPECT_GT(receiver_resources.reserved_memory_bytes(), 0U);

  auto complete = receiver.take_complete_stream();
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_EQ(complete->edge.source_node_id, 2U);
  EXPECT_EQ(complete->edge.target_node_id, 3U);
  ASSERT_EQ(complete->messages.size(), 2U);
  EXPECT_EQ(std::get<std::string>(complete->messages[0].keys()[0].storage()),
            "east-larger-than-SSO");
  EXPECT_EQ(complete->messages[1].position().sequence, 2U);
  EXPECT_EQ(receiver.take_complete_stream().error().code(), common::StatusCode::kInvalidArgument);
  complete = common::make_unexpected(common::Status{common::StatusCode::kInternal, "release"});
  EXPECT_EQ(receiver_resources.reserved_memory_bytes(), 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleStreamTest,
     RejectsUnauthorizedWrongSequenceIncompleteAndTerminalSuffixWithoutPrefix) {
  auto expected = authority();
  auto encoded = encoded_messages();
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  auto sender_result = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      expected, edge(), encoded, resources);
  ASSERT_TRUE(sender_result.has_value()) << sender_result.error().to_string();
  auto sender = std::move(*sender_result);
  Authorizer authorizer;
  auto unauthenticated_create = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = false, .principal_id = 0U}, resources);
  ASSERT_FALSE(unauthenticated_create.has_value());
  EXPECT_EQ(unauthenticated_create.error().code(), common::StatusCode::kUnauthenticated);
  auto unauthorized_create = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = true, .principal_id = 90U}, resources);
  ASSERT_TRUE(unauthorized_create.has_value()) << unauthorized_create.error().to_string();
  auto unauthorized = std::move(*unauthorized_create);
  auto unauthorized_result = unauthorized.consume(sender.pending_write());
  ASSERT_FALSE(unauthorized_result.has_value());
  EXPECT_EQ(unauthorized_result.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_TRUE(unauthorized.failed());
  EXPECT_EQ(unauthorized.accepted_frames(), 0U);

  auto first = message(0U, 2U);
  DistributedVectorGroupedAggregateShuffleFrameV1 first_frame{.query_id = expected.query_id(),
                                                              .edge = edge(),
                                                              .partition_count =
                                                                  expected.partition_count(),
                                                              .payload = std::move(first)};
  auto first_encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(first_frame, expected);
  ASSERT_TRUE(first_encoded.has_value()) << first_encoded.error().to_string();
  auto first_bytes = std::move(*first_encoded);
  auto incomplete_nested_message = message(0U, 2U);
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> incomplete_nested;
  incomplete_nested.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                                  incomplete_nested_message, keys(), aggregates())
                                  .value());
  auto incomplete_sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      expected, edge(), incomplete_nested, resources);
  ASSERT_FALSE(incomplete_sender.has_value());
  EXPECT_EQ(incomplete_sender.error().code(), common::StatusCode::kInvalidArgument);

  auto incomplete_create = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources);
  ASSERT_TRUE(incomplete_create.has_value()) << incomplete_create.error().to_string();
  auto incomplete = std::move(*incomplete_create);
  ASSERT_TRUE(incomplete.consume(first_bytes).has_value());
  EXPECT_EQ(incomplete.finish_input().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(incomplete.accepted_frames(), 0U);

  auto duplicate_create = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources);
  ASSERT_TRUE(duplicate_create.has_value()) << duplicate_create.error().to_string();
  auto duplicate = std::move(*duplicate_create);
  ASSERT_TRUE(duplicate.consume(first_bytes).has_value());
  auto duplicate_result = duplicate.consume(first_bytes);
  ASSERT_FALSE(duplicate_result.has_value());
  EXPECT_EQ(duplicate_result.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(duplicate.accepted_frames(), 0U);

  auto complete_receiver_create = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources);
  ASSERT_TRUE(complete_receiver_create.has_value()) << complete_receiver_create.error().to_string();
  auto complete_receiver = std::move(*complete_receiver_create);
  auto full_sender_create = DistributedVectorGroupedAggregateShuffleStreamSender::create(
      expected, edge(), encoded, resources);
  ASSERT_TRUE(full_sender_create.has_value()) << full_sender_create.error().to_string();
  auto full_sender = std::move(*full_sender_create);
  std::vector<std::byte> coalesced;
  while (!full_sender.complete()) {
    const common::ByteView pending = full_sender.pending_write();
    coalesced.insert(coalesced.end(), pending.begin(), pending.end());
    ASSERT_TRUE(full_sender.consume_written(pending.size()).is_ok());
  }
  coalesced.push_back(std::byte{0x5aU});
  auto coalesced_result = complete_receiver.consume(coalesced);
  ASSERT_FALSE(coalesced_result.has_value());
  EXPECT_EQ(coalesced_result.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(complete_receiver.failed());
  EXPECT_EQ(complete_receiver.accepted_frames(), 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleStreamTest,
     AcceptsOneCanonicalEmptyTerminalAsACompleteStream) {
  auto expected = authority();
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(empty_message());
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge(),
                                                                             encoded, resources);
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  Authorizer authorizer;
  auto receiver = DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
      expected, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources);
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  auto read = receiver->consume(sender->pending_write());
  ASSERT_TRUE(read.has_value()) << read.error().to_string();
  ASSERT_TRUE(sender->consume_written(read->consumed_bytes).is_ok());
  ASSERT_TRUE(read->complete);
  auto stream = receiver->take_complete_stream();
  ASSERT_TRUE(stream.has_value()) << stream.error().to_string();
  ASSERT_EQ(stream->messages.size(), 1U);
  EXPECT_TRUE(stream->messages.front().position().empty);
  EXPECT_TRUE(stream->messages.front().position().terminal);
}

TEST(DistributedVectorGroupedAggregateShuffleStreamTest,
     EnforcesWholeStreamBytesAndRejectsLocalNetworkEdge) {
  auto expected = authority();
  auto encoded = encoded_messages();
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  auto valid = DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge(),
                                                                            encoded, resources);
  ASSERT_TRUE(valid.has_value());
  DistributedVectorGroupedAggregateShuffleStreamLimits lower;
  lower.maximum_encoded_bytes = valid->encoded_bytes() - 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleStreamSender::create(expected, edge(), encoded,
                                                                         resources, lower)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto local_authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
                             {{.partition_id = 0U, .node_id = 2U}}, keys(), aggregates())
                             .value();
  auto local_edge = edge();
  local_edge.target_node_id = 2U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleStreamSender::create(
                local_authority, local_edge, encoded, resources)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
