#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_tcp_client.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
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

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(4U),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(9U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 8U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && node == 1U) ||
                                (principal == 92U && node == 2U)};
  }
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    ++bind_calls;
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++execute_calls;
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = {.keys = keys(), .aggregates = aggregates()},
        .input_rows = 3U,
        .group_count = 2U};
    for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
      auto state =
          query::MergeableVectorAggregateState::create(result.authority.aggregates.front()).value();
      for (std::size_t count = 0U; count <= ordinal; ++count)
        EXPECT_TRUE(state.accumulate_count_star().has_value());
      std::vector<query::ScalarValue> key_values;
      key_values.push_back(
          query::ScalarValue::text(string_type(), ordinal == 0U ? "east" : "west").value());
      std::vector<query::MergeableVectorAggregateState> states;
      states.push_back(std::move(state));
      query::DistributedVectorGroupedAggregateExchangeMessage message{
          query::DistributedVectorGroupedAggregateExchangePosition{
              .query_id = received.query_id,
              .tablet_id = received.tablet_id,
              .sequence = ordinal + 1U,
              .group_ordinal = static_cast<std::uint32_t>(ordinal),
              .group_count = 2U,
              .terminal = ordinal == 1U,
              .empty = false},
          std::move(key_values), std::move(states)};
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          message, result.authority.keys, result.authority.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      result.encoded_bytes += encoded->bytes().size();
      result.messages.push_back(std::move(*encoded));
    }
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
};

[[nodiscard]] DistributedMutableVectorGroupedAggregateQueryTlsLimits limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .maximum_response_frames = 2U,
          .maximum_response_bytes = std::size_t{1024U} * 1024U};
}

// Optional values below are asserted present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedMutableVectorGroupedAggregateQueryTcpClientTest,
     OwnsConnectAuthorityResourcesAndCompleteMutualTlsStream) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  auto listener = network::TcpListener::bind();
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(receiver.has_value());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(server_context.has_value());
  ASSERT_TRUE(client_context.has_value());

  auto request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()});
  auto expected_keys = keys();
  auto expected_aggregates = aggregates();
  auto resources = query::QueryResourceContext::create(1U << 20U);
  ASSERT_TRUE(request.has_value());
  ASSERT_TRUE(resources.has_value());
  Authenticator server_authenticator{92U};
  const auto start =
      DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::clock::now();
  auto client = DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
      {1U, 2U, std::move(*request)}, std::move(expected_keys), std::move(expected_aggregates),
      std::move(*resources),
      {.remote_endpoint = listener->bound_endpoint(),
       .tls_context = &*client_context,
       .carrier = {.authenticator = &server_authenticator,
                   .node_authorizer = &authorizer,
                   .peer_ipv4_address = {127U, 0U, 0U, 1U},
                   .limits = limits()},
       .connect_timeout = std::chrono::milliseconds{1000}},
      start);
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  EXPECT_FALSE(client->responses().has_value());

  Authenticator client_authenticator{91U};
  std::optional<network::TcpSocket> accepted_socket;
  std::optional<DistributedMutableVectorGroupedAggregateQueryTlsServer> server;
  for (std::size_t iteration = 0U; iteration < 4096U; ++iteration) {
    if (!accepted_socket.has_value()) {
      auto accepted = listener->accept_one();
      ASSERT_TRUE(accepted.has_value());
      if (accepted->has_value()) {
        accepted_socket.emplace(std::move(accepted->value()));
        auto tls = network::TlsSocket::accept(*server_context, accepted_socket->descriptor());
        ASSERT_TRUE(tls.has_value());
        auto carrier = DistributedMutableVectorGroupedAggregateQueryTlsServer::create(
            std::move(*tls),
            {.authenticator = &client_authenticator,
             .receiver = &*receiver,
             .peer_ipv4_address = accepted_socket->peer_endpoint().value().address,
             .limits = limits()},
            DistributedMutableVectorGroupedAggregateQueryTlsServer::TimePoint::clock::now());
        ASSERT_TRUE(carrier.has_value());
        server.emplace(std::move(*carrier));
      }
    }

    std::array<pollfd, 2U> descriptors{};
    std::size_t count{};
    const auto client_interest = client->interest();
    descriptors[count++] = {.fd = client->descriptor(),
                            .events =
                                static_cast<short>((client_interest.want_read ? POLLIN : 0) |
                                                   (client_interest.want_write ? POLLOUT : 0)),
                            .revents = 0};
    if (server.has_value()) {
      const auto server_interest = server->interest();
      descriptors[count++] = {.fd = accepted_socket->descriptor(),
                              .events =
                                  static_cast<short>((server_interest.want_read ? POLLIN : 0) |
                                                     (server_interest.want_write ? POLLOUT : 0)),
                              .revents = 0};
    }
    ASSERT_GE(::poll(descriptors.data(), static_cast<nfds_t>(count), 1), 0);
    const auto now =
        DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint::clock::now();
    ASSERT_TRUE(client
                    ->on_ready((descriptors[0].revents & POLLIN) != 0,
                               (descriptors[0].revents & POLLOUT) != 0, now)
                    .is_ok())
        << client->failure().to_string();
    if (server.has_value()) {
      ASSERT_TRUE(server
                      ->on_ready((descriptors[1].revents & POLLIN) != 0,
                                 (descriptors[1].revents & POLLOUT) != 0, now)
                      .is_ok())
          << server->failure().to_string();
    }
    if (client->state() == DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete &&
        server.has_value() &&
        server->state() == DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete) {
      break;
    }
  }

  ASSERT_EQ(client->state(),
            DistributedMutableVectorGroupedAggregateQueryTcpClientState::kComplete);
  ASSERT_TRUE(server.has_value());
  EXPECT_EQ(server->state(), DistributedMutableVectorGroupedAggregateQueryTlsState::kComplete);
  EXPECT_TRUE(client_authenticator.saw_fingerprint);
  EXPECT_TRUE(server_authenticator.saw_fingerprint);
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  const auto responses = client->responses();
  ASSERT_TRUE(responses.has_value());
  ASSERT_EQ(responses->size(), 2U);
  for (std::size_t ordinal = 0U; ordinal < responses->size(); ++ordinal) {
    ASSERT_TRUE((*responses)[ordinal].payload.has_value());
    const auto& position = (*responses)[ordinal].payload->position();
    EXPECT_EQ(position.group_ordinal, ordinal);
    EXPECT_EQ(position.sequence, ordinal + 1U);
    EXPECT_EQ(position.terminal, ordinal + 1U == responses->size());
    EXPECT_EQ((*responses)[ordinal].payload->states().front().definition(), aggregates().front());
  }
}

TEST(DistributedMutableVectorGroupedAggregateQueryTcpClientTest,
     ValidatesAuthorityBeforeConnectAndExpiresExactly) {
  Authorizer authorizer;
  Authenticator authenticator{92U};
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value());
  ASSERT_TRUE(tls_context.has_value());
  auto request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()});
  auto expected_keys = keys();
  auto expected_aggregates = aggregates();
  auto resources = query::QueryResourceContext::create(1U << 20U);
  ASSERT_TRUE(request.has_value());
  ASSERT_TRUE(resources.has_value());
  const auto start = DistributedMutableVectorGroupedAggregateQueryTcpClient::TimePoint{};
  auto config = DistributedMutableVectorGroupedAggregateQueryTcpClientConfig{
      .remote_endpoint = listener->bound_endpoint(),
      .tls_context = &*tls_context,
      .carrier = {.authenticator = &authenticator,
                  .node_authorizer = &authorizer,
                  .peer_ipv4_address = {127U, 0U, 0U, 1U},
                  .limits = limits()},
      .connect_timeout = std::chrono::milliseconds{5}};
  auto client = DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
      {1U, 2U, std::move(*request)}, std::move(expected_keys), std::move(expected_aggregates),
      *resources, config, start);
  ASSERT_TRUE(client.has_value());
  EXPECT_TRUE(client->on_ready(false, false, start + std::chrono::milliseconds{4}).is_ok());
  const auto expired = client->on_ready(false, false, start + std::chrono::milliseconds{5});
  EXPECT_EQ(expired.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(client->state(), DistributedMutableVectorGroupedAggregateQueryTcpClientState::kFailed);
  EXPECT_EQ(client->descriptor(), -1);
  EXPECT_EQ(client->on_ready(true, true, start + std::chrono::milliseconds{6}), expired);
  EXPECT_FALSE(client->responses().has_value());

  auto invalid_request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()});
  auto invalid_keys = keys();
  auto invalid_aggregates = aggregates();
  ASSERT_TRUE(invalid_request.has_value());
  config.carrier.limits.maximum_response_frames = 0U;
  auto invalid = DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
      {1U, 2U, std::move(*invalid_request)}, std::move(invalid_keys), std::move(invalid_aggregates),
      *resources, config, start);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);

  auto mismatched_request = encode_distributed_mutable_vector_query_request({1U, 2U, fragment()});
  auto mismatched_keys = keys();
  auto mismatched_aggregates = aggregates();
  ASSERT_TRUE(mismatched_request.has_value());
  mismatched_keys.front().nullable = true;
  config.carrier.limits.maximum_response_frames = 2U;
  auto mismatched = DistributedMutableVectorGroupedAggregateQueryTcpClient::begin(
      {1U, 2U, std::move(*mismatched_request)}, std::move(mismatched_keys),
      std::move(mismatched_aggregates), *resources, config, start);
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);
}
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
