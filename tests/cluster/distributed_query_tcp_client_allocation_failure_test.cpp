#include "chronos/cluster/distributed_grouped_query_tcp_client.hpp"
#include "chronos/cluster/distributed_query_tcp_client.hpp"
#include "chronos/cluster/distributed_vector_query_tcp_client_v2.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
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

[[nodiscard]] query::DistributedAggregateFragmentDispatch aggregate_dispatch() {
  return {.raft_group_id = uuid(9U),
          .fragment = {.query_id = uuid(1U),
                       .database_id = id<manifest::DatabaseId>(2U),
                       .table_id = id<schema::TableId>(3U),
                       .tablet_id = id<schema::TabletId>(4U),
                       .destination_schema_id = id<schema::SchemaId>(5U),
                       .snapshot_generation = 6U,
                       .serving_node = 2U,
                       .applied_position = 10U,
                       .observed_leader_commit_position = 10U,
                       .placement_epoch = 8U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLeaderLinearizable},
                       .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                       .destination_column_ordinals = {1U},
                       .aggregate_input_index = 0U}};
}

[[nodiscard]] query::DistributedGroupedFloat64FragmentDispatch grouped_dispatch() {
  return {.raft_group_id = uuid(9U),
          .fragment = {.aggregate = aggregate_dispatch().fragment, .group_key_input_index = 0U}};
}

[[nodiscard]] query::DistributedVectorResultSchema vector_result_schema() {
  return {
      .columns = {
          {"value", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), true}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 vector_dispatch() {
  const query::DistributedAggregateFragmentDispatch aggregate = aggregate_dispatch();
  return {.dispatch = {.query_id = aggregate.fragment.query_id,
                       .database_id = aggregate.fragment.database_id,
                       .table_id = aggregate.fragment.table_id,
                       .tablet_id = aggregate.fragment.tablet_id,
                       .destination_schema_id = aggregate.fragment.destination_schema_id,
                       .raft_group_id = aggregate.raft_group_id,
                       .snapshot_generation = aggregate.fragment.snapshot_generation,
                       .serving_node = aggregate.fragment.serving_node,
                       .applied_position = aggregate.fragment.applied_position,
                       .observed_leader_commit_position =
                           aggregate.fragment.observed_leader_commit_position,
                       .placement_epoch = aggregate.fragment.placement_epoch,
                       .read_policy = aggregate.fragment.read_policy,
                       .linearizable_barrier = aggregate.fragment.linearizable_barrier,
                       .destination_column_ordinals = {1U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kRows,
                                .row_output_indices = {0U}}},
          .result_schema = vector_result_schema()};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 92U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  test::ScopedAllocationFailure failure{fail_after};
  try {
    result.emplace(operation());
  } catch (...) {
    failure.disable();
    throw;
  }
  failure.disable();
  return std::move(*result);
}

template <typename Preparation, typename Operation>
void expect_sweep(Preparation&& preparation, Operation&& operation) {
  bool saw_failure = false;
  bool saw_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    auto prepared = preparation();
    auto result = run_failure(fail_after, [&] { return operation(std::move(prepared)); });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

TEST(DistributedQueryTcpClientAllocationFailureTest,
     ClassifiesScalarAggregateClientOwnerAllocation) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authenticator authenticator;
  Authorizer authorizer;
  expect_sweep(
      [] { return encode_distributed_query_request_v1({1U, 2U, aggregate_dispatch()}).value(); },
      [&](std::vector<std::byte> request) {
        return DistributedQueryTcpClient::begin(
            {1U, 2U, std::move(request)},
            {.remote_endpoint = listener->bound_endpoint(),
             .tls_context = &*tls_context,
             .carrier = {.authenticator = &authenticator,
                         .node_authorizer = &authorizer,
                         .peer_ipv4_address = {127U, 0U, 0U, 1U},
                         .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                    .exchange_timeout = std::chrono::milliseconds{1000}}},
             .connect_timeout = std::chrono::milliseconds{1000}},
            DistributedQueryTcpClient::TimePoint::clock::now());
      });
}

TEST(DistributedQueryTcpClientAllocationFailureTest, ClassifiesGroupedClientOwnerAllocation) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authenticator authenticator;
  Authorizer authorizer;
  expect_sweep(
      [] {
        return encode_distributed_grouped_query_request_v1({1U, 2U, grouped_dispatch()}).value();
      },
      [&](std::vector<std::byte> request) {
        return DistributedGroupedQueryTcpClient::begin(
            {1U, 2U, std::move(request)},
            {.remote_endpoint = listener->bound_endpoint(),
             .tls_context = &*tls_context,
             .carrier = {.authenticator = &authenticator,
                         .node_authorizer = &authorizer,
                         .peer_ipv4_address = {127U, 0U, 0U, 1U},
                         .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                    .exchange_timeout = std::chrono::milliseconds{1000},
                                    .maximum_response_frames = 1U}},
             .connect_timeout = std::chrono::milliseconds{1000}},
            DistributedGroupedQueryTcpClient::TimePoint::clock::now());
      });
}

TEST(DistributedQueryTcpClientAllocationFailureTest, ClassifiesVectorRowClientOwnerAllocation) {
  auto listener = network::TcpListener::bind();
  auto tls_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  ASSERT_TRUE(tls_context.has_value()) << tls_context.error().to_string();
  Authenticator authenticator;
  Authorizer authorizer;
  expect_sweep(
      [] {
        return encode_distributed_vector_query_request_v2({1U, 2U, vector_dispatch()}).value();
      },
      [&](std::vector<std::byte> request) {
        return DistributedVectorQueryTcpClientV2::begin(
            {1U, 2U, std::move(request)},
            {.remote_endpoint = listener->bound_endpoint(),
             .tls_context = &*tls_context,
             .carrier = {.authenticator = &authenticator,
                         .node_authorizer = &authorizer,
                         .peer_ipv4_address = {127U, 0U, 0U, 1U},
                         .limits = {.handshake_timeout = std::chrono::milliseconds{1000},
                                    .exchange_timeout = std::chrono::milliseconds{1000},
                                    .maximum_response_frames = 1U,
                                    .maximum_response_bytes = std::size_t{1024U} * 1024U}},
             .connect_timeout = std::chrono::milliseconds{1000}},
            DistributedVectorQueryTcpClientV2::TimePoint::clock::now());
      });
}

} // namespace
} // namespace chronos::cluster
