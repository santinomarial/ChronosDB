#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_coordinator_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tcp_execution.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::integration {
namespace {

constexpr raft::NodeId kCoordinatorNode = 9U;

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_TEST_TLS_FIXTURE_DIR} / name;
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
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType i64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", i64_type(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 1U},
          .result_schema = result_schema()};
}

struct Proofs {
  Proofs() = default;

  std::vector<query::DistributedMutableVectorFragment> fragments{fragment(2U, 3U),
                                                                 fragment(3U, 4U)};
  cluster::DistributedVectorGroupedAggregateShuffleAuthority authority{
      *cluster::DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
          fragments, std::array{query::VectorGroupKeyDefinition{0U, string_type(), false}},
          std::array{query::VectorAggregateDefinition{query::VectorAggregateOperation::kCountStar,
                                                      std::nullopt}})};
  cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2 finalization{
      *cluster::DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(authority,
                                                                                        fragments)};
};

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)> encoded_u64(std::uint64_t value) {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint64_t{0xffU});
  return bytes;
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value, const std::uint64_t count) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false},
                           network::QueryResultColumn{"count", i64_type(), false}};
  const auto encoded_count = encoded_u64(count);
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})},
                         network::QueryResultCell{.value = encoded_count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleResultTlsLimits carrier_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{1000},
          .exchange_timeout = std::chrono::milliseconds{1000},
          .stream = {.maximum_frames = 1U, .maximum_encoded_bytes = 1U << 20U}};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

private:
  std::uint64_t principal_{};
};

class Authorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return common::Result<bool>{(principal == 91U && (node == 3U || node == 4U)) ||
                                (principal == 92U && node == kCoordinatorNode)};
  }
};

template <typename Integer>
[[nodiscard]] std::optional<Integer> parse_integer(std::string_view text) {
  Integer value{};
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
    return std::nullopt;
  return value;
}

[[nodiscard]] int run_coordinator(const std::uint16_t port,
                                  const std::chrono::milliseconds timeout) {
  Proofs proofs;
  Authenticator authenticator{91U};
  Authorizer authorizer;
  auto execution =
      cluster::DistributedVectorGroupedAggregateShuffleResultCoordinatorExecution::create(
          proofs.authority, proofs.finalization,
          {.listener = {.bind_endpoint = {{127U, 0U, 0U, 1U}, port}},
           .tls = server_tls(),
           .authenticator = &authenticator,
           .node_authorizer = &authorizer,
           .coordinator_node_id = kCoordinatorNode,
           .carrier_limits = carrier_limits(),
           .maximum_retained_server_streams = 2U,
           .maximum_accepts_per_poll = 2U,
           .execution_deadline = std::chrono::steady_clock::now() + timeout});
  if (!execution.has_value()) {
    std::cerr << execution.error().to_string() << '\n';
    return 2;
  }
  std::cout << "READY " << execution->bound_endpoint().port << '\n' << std::flush;
  while (
      execution->state() ==
      cluster::DistributedVectorGroupedAggregateShuffleResultCoordinatorExecutionState::kRunning) {
    const common::Status status = execution->poll_once(std::chrono::milliseconds{10});
    if (!status.is_ok()) {
      if (status.code() == common::StatusCode::kCancelled) {
        std::cout << "CANCELLED\n" << std::flush;
        return 3;
      }
      std::cerr << status.to_string() << '\n';
      return 4;
    }
  }
  auto result = execution->take_result();
  if (!result.has_value() || result->encoded_batches.size() != 1U) {
    std::cerr << "final grouped result is unavailable\n";
    return 5;
  }
  auto decoded = network::decode_query_result_batch(result->encoded_batches.front());
  if (!decoded.has_value() || decoded->row_count() != 1U) {
    std::cerr << "final grouped result is invalid\n";
    return 6;
  }
  const auto* region = decoded->cell(0U, 0U);
  const auto* count = decoded->cell(0U, 1U);
  const std::string wanted{"west"};
  const auto wanted_bytes = std::as_bytes(std::span{wanted});
  if (region == nullptr || count == nullptr ||
      !std::equal(region->value.begin(), region->value.end(), wanted_bytes.begin(),
                  wanted_bytes.end()) ||
      count->value.size() != sizeof(std::uint64_t) || count->value.front() != std::byte{2U}) {
    std::cerr << "final grouped result has unexpected values\n";
    return 7;
  }
  std::cout << "RESULT west 2\n" << std::flush;
  return 0;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] int run_reducer(const std::uint16_t coordinator_port,
                              const std::uint16_t refused_port, const std::uint32_t partition_id,
                              const std::string& value, const std::uint64_t count) {
  Proofs proofs;
  Authenticator authenticator{92U};
  Authorizer authorizer;
  auto context = network::TlsClientContext::create(client_tls());
  if (!context.has_value()) {
    std::cerr << context.error().to_string() << '\n';
    return 2;
  }
  const auto source = proofs.authority.destination_node(partition_id);
  if (!source.has_value())
    return 3;
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(batch(value, count));
  auto retry = cluster::DistributedVectorGroupedAggregateShuffleResultRetry::create(
      proofs.authority, proofs.finalization.result_schema(),
      {.partition_id = partition_id,
       .source_node_id = *source,
       .coordinator_node_id = kCoordinatorNode},
      std::move(batches),
      {.retry = {.maximum_attempts = 8U,
                 .initial_backoff = std::chrono::milliseconds{20},
                 .maximum_backoff = std::chrono::milliseconds{50}},
       .stream = carrier_limits().stream});
  if (!retry.has_value()) {
    std::cerr << retry.error().to_string() << '\n';
    return 4;
  }
  std::vector<network::Ipv4Endpoint> endpoints;
  if (refused_port != 0U)
    endpoints.push_back({{127U, 0U, 0U, 1U}, refused_port});
  endpoints.push_back({{127U, 0U, 0U, 1U}, coordinator_port});
  std::vector<cluster::DistributedVectorGroupedAggregateShuffleResultRetry> retries;
  retries.push_back(std::move(*retry));
  auto execution = cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
      proofs.authority, proofs.finalization.result_schema(), std::move(retries),
      {.authenticator = &authenticator,
       .node_authorizer = &authorizer,
       .routes = {{.node_id = kCoordinatorNode,
                   .endpoints = std::move(endpoints),
                   .tls_context = &*context}},
       .carrier_limits = carrier_limits(),
       .connect_timeout = std::chrono::milliseconds{500},
       .execution_deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5}});
  if (!execution.has_value()) {
    std::cerr << execution.error().to_string() << '\n';
    return 5;
  }
  while (execution->state() ==
         cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kRunning) {
    const common::Status status = execution->poll_once(std::chrono::milliseconds{10});
    if (!status.is_ok()) {
      std::cerr << status.to_string() << '\n';
      return 6;
    }
  }
  if (execution->state() !=
      cluster::DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete) {
    std::cerr << execution->failure().to_string() << '\n';
    return 7;
  }
  const auto metrics = execution->metrics();
  std::cout << "SENT " << partition_id << " attempts=" << metrics.attempts_started
            << " retries=" << metrics.retries_started << '\n'
            << std::flush;
  return 0;
}

[[nodiscard]] int run_stalled_reducer(const std::uint32_t partition_id) {
  Proofs proofs;
  if (!proofs.authority.destination_node(partition_id).has_value())
    return 2;
  std::cout << "STALLING " << partition_id << '\n' << std::flush;
  for (;;)
    std::this_thread::sleep_for(std::chrono::seconds{1});
}

} // namespace
} // namespace chronos::integration

int main(const int argc, char** argv) {
  using namespace chronos::integration;
  if (argc == 3 && std::string_view{argv[1]} == "stall-reducer") {
    const auto partition = parse_integer<std::uint32_t>(argv[2]);
    if (partition.has_value())
      return run_stalled_reducer(*partition);
  }
  if (argc == 4 && std::string_view{argv[1]} == "coordinator") {
    const auto port = parse_integer<std::uint16_t>(argv[2]);
    const auto timeout = parse_integer<std::uint64_t>(argv[3]);
    if (port.has_value() && timeout.has_value())
      return run_coordinator(*port, std::chrono::milliseconds{*timeout});
  }
  if (argc == 7 && std::string_view{argv[1]} == "reducer") {
    const auto coordinator_port = parse_integer<std::uint16_t>(argv[2]);
    const auto refused_port = parse_integer<std::uint16_t>(argv[3]);
    const auto partition = parse_integer<std::uint32_t>(argv[4]);
    const auto count = parse_integer<std::uint64_t>(argv[6]);
    if (coordinator_port.has_value() && refused_port.has_value() && partition.has_value() &&
        count.has_value()) {
      return run_reducer(*coordinator_port, *refused_port, *partition, argv[5], *count);
    }
  }
  std::cerr << "usage: grouped_shuffle_result_process_child "
               "coordinator PORT TIMEOUT_MS | reducer PORT REFUSED_PORT PARTITION VALUE COUNT | "
               "stall-reducer PARTITION\n";
  return 64;
}
