#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_server.hpp"
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

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 91U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return common::Result<bool>{true};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultTcpServerAllocationFailureTest,
     ClassifiesTlsListenerAndPreallocatedCompletionStorage) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, {})
                       .value();
  query::DistributedVectorResultSchema schema{.columns = {{"region", type, false}}};
  Authenticator authenticator;
  Authorizer authorizer;
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(testing::Message{} << "fail_after=" << fail_after);
    DistributedVectorGroupedAggregateShuffleResultTcpServerConfig config{
        .listener = {},
        .tls = server_tls(),
        .authenticator = &authenticator,
        .node_authorizer = &authorizer,
        .authority = &authority,
        .result_schema = &schema,
        .coordinator_node_id = 9U,
        .carrier_limits = {.handshake_timeout = std::chrono::milliseconds{100},
                           .exchange_timeout = std::chrono::milliseconds{100}},
        .maximum_retained_streams = 8U,
        .maximum_accepts_per_poll = 2U};
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultTcpServer::start(std::move(config));
    });
    if (!result.has_value()) {
      saw_failure = true;
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
          << result.error().to_string();
      continue;
    }
    saw_success = true;
    EXPECT_TRUE(result->shutdown().is_ok());
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

} // namespace
} // namespace chronos::cluster
