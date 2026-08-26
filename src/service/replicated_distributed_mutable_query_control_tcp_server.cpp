#include "chronos/service/replicated_distributed_mutable_query_control_tcp_server.hpp"

#include "chronos/service/replicated_raft_read_authority_service.hpp"

#include <memory>
#include <new>
#include <optional>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status empty_server() {
  return {common::StatusCode::kInvalidArgument,
          "replicated distributed mutable query-control TCP server is empty"};
}

} // namespace

class ReplicatedDistributedMutableQueryControlTcpServer::Impl {
public:
  Impl(ReplicatedDistributedMutableVectorQueryWorker owned_worker,
       ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker owned_grouped_worker,
       ReplicatedRaftReadAuthorityService owned_authority_service) noexcept
      : worker(std::move(owned_worker)), grouped_worker(std::move(owned_grouped_worker)),
        authority_service(std::move(owned_authority_service)) {}

  ReplicatedDistributedMutableVectorQueryWorker worker;
  ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker grouped_worker;
  ReplicatedRaftReadAuthorityService authority_service;
  std::optional<cluster::DistributedMutableVectorQueryReceiver> mutable_receiver;
  std::optional<cluster::DistributedMutableVectorGroupedAggregateQueryReceiver>
      mutable_grouped_receiver;
  std::optional<cluster::RaftReadAuthorityReceiver> authority_receiver;
  std::optional<cluster::DistributedVectorGroupedAggregateShuffleJobService>
      grouped_shuffle_job_service;
  std::optional<cluster::DistributedMutableQueryControlTcpServer> server;

  [[nodiscard]] cluster::DistributedMutableQueryControlTcpServer* active_server() noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }

  [[nodiscard]] const cluster::DistributedMutableQueryControlTcpServer*
  active_server() const noexcept {
    if (!server.has_value())
      return nullptr;
    // Guarded by the presence check above.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    return std::addressof(*server);
  }
};

ReplicatedDistributedMutableQueryControlTcpServer::
    ReplicatedDistributedMutableQueryControlTcpServer(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedDistributedMutableQueryControlTcpServer::
    ~ReplicatedDistributedMutableQueryControlTcpServer() = default;
ReplicatedDistributedMutableQueryControlTcpServer::
    ReplicatedDistributedMutableQueryControlTcpServer(
        ReplicatedDistributedMutableQueryControlTcpServer&&) noexcept = default;
ReplicatedDistributedMutableQueryControlTcpServer&
ReplicatedDistributedMutableQueryControlTcpServer::operator=(
    ReplicatedDistributedMutableQueryControlTcpServer&&) noexcept = default;

common::Result<ReplicatedDistributedMutableQueryControlTcpServer>
ReplicatedDistributedMutableQueryControlTcpServer::start(
    ReplicatedDistributedMutableQueryControlTcpServerConfig config) {
  if (config.authenticator == nullptr || config.node_authorizer == nullptr ||
      config.read_barrier == nullptr) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated mutable query-control TCP server authority configuration is invalid"});
  }
  if (config.grouped_shuffle_jobs.has_value() &&
      (config.grouped_shuffle_jobs->local_node_id != config.worker.local_node_id ||
       config.grouped_shuffle_jobs->shuffle_authenticator != config.authenticator ||
       config.grouped_shuffle_jobs->result_authenticator != config.authenticator ||
       config.grouped_shuffle_jobs->node_authorizer != config.node_authorizer)) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument,
        "replicated mutable query-control grouped-shuffle authority configuration is invalid"});
  }
  auto worker = ReplicatedDistributedMutableVectorQueryWorker::create(config.worker);
  if (!worker.has_value())
    return common::make_unexpected(worker.error());
  auto grouped_worker = ReplicatedDistributedMutableVectorGroupedAggregateQueryWorker::create(
      {.local_node_id = config.worker.local_node_id,
       .context_provider = config.worker.context_provider,
       .limits = config.grouped_worker_limits});
  if (!grouped_worker.has_value())
    return common::make_unexpected(grouped_worker.error());
  auto authority_service = ReplicatedRaftReadAuthorityService::create(config.read_barrier);
  if (!authority_service.has_value())
    return common::make_unexpected(authority_service.error());
  try {
    auto implementation = std::make_unique<Impl>(std::move(*worker), std::move(*grouped_worker),
                                                 std::move(*authority_service));
    auto mutable_receiver = cluster::DistributedMutableVectorQueryReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .worker = std::addressof(implementation->worker),
         .leader_hint_provider = config.leader_hint_provider,
         .maximum_response_frames = config.carrier_limits.maximum_mutable_response_frames,
         .maximum_response_bytes = config.carrier_limits.maximum_mutable_response_bytes});
    if (!mutable_receiver.has_value())
      return common::make_unexpected(mutable_receiver.error());
    auto mutable_grouped_receiver =
        cluster::DistributedMutableVectorGroupedAggregateQueryReceiver::create(
            {.local_node_id = config.worker.local_node_id,
             .authorizer = config.node_authorizer,
             .worker = std::addressof(implementation->grouped_worker),
             .leader_hint_provider = config.leader_hint_provider,
             .maximum_response_frames =
                 config.carrier_limits.maximum_mutable_grouped_response_frames,
             .maximum_response_bytes = config.carrier_limits.maximum_mutable_grouped_response_bytes,
             .maximum_decode_memory_bytes =
                 config.carrier_limits.maximum_mutable_grouped_decode_memory_bytes,
             .payload = config.carrier_limits.mutable_grouped_payload});
    if (!mutable_grouped_receiver.has_value())
      return common::make_unexpected(mutable_grouped_receiver.error());
    auto authority_receiver = cluster::RaftReadAuthorityReceiver::create(
        {.local_node_id = config.worker.local_node_id,
         .authorizer = config.node_authorizer,
         .service = std::addressof(implementation->authority_service),
         .limits = config.carrier_limits.read_authority_transport});
    if (!authority_receiver.has_value())
      return common::make_unexpected(authority_receiver.error());
    auto& installed_mutable_receiver = implementation->mutable_receiver.emplace(*mutable_receiver);
    auto& installed_mutable_grouped_receiver =
        implementation->mutable_grouped_receiver.emplace(*mutable_grouped_receiver);
    auto& installed_authority_receiver =
        implementation->authority_receiver.emplace(*authority_receiver);
    cluster::DistributedVectorGroupedAggregateShuffleJobService* grouped_shuffle_job_service{};
    if (config.grouped_shuffle_jobs.has_value()) {
      auto service = cluster::DistributedVectorGroupedAggregateShuffleJobService::create(
          std::move(*config.grouped_shuffle_jobs));
      if (!service.has_value())
        return common::make_unexpected(service.error());
      grouped_shuffle_job_service =
          std::addressof(implementation->grouped_shuffle_job_service.emplace(std::move(*service)));
    }
    auto server = cluster::DistributedMutableQueryControlTcpServer::start(
        {.listener = config.listener,
         .tls = std::move(config.tls),
         .authenticator = config.authenticator,
         .mutable_receiver = std::addressof(installed_mutable_receiver),
         .mutable_grouped_receiver = std::addressof(installed_mutable_grouped_receiver),
         .read_authority_receiver = std::addressof(installed_authority_receiver),
         .grouped_shuffle_job_service = grouped_shuffle_job_service,
         .carrier_limits = config.carrier_limits,
         .maximum_connections = config.maximum_connections,
         .maximum_accepts_per_poll = config.maximum_accepts_per_poll});
    if (!server.has_value())
      return common::make_unexpected(server.error());
    implementation->server.emplace(std::move(*server));
    return ReplicatedDistributedMutableQueryControlTcpServer{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted,
        "replicated distributed mutable query-control TCP server allocation failed"});
  }
}

common::Status ReplicatedDistributedMutableQueryControlTcpServer::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  cluster::DistributedMutableQueryControlTcpServer* const server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? empty_server() : server->poll_once(maximum_wait);
}

common::Status ReplicatedDistributedMutableQueryControlTcpServer::shutdown() {
  cluster::DistributedMutableQueryControlTcpServer* const server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? empty_server() : server->shutdown();
}

network::Ipv4Endpoint
ReplicatedDistributedMutableQueryControlTcpServer::bound_endpoint() const noexcept {
  const cluster::DistributedMutableQueryControlTcpServer* const server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? network::Ipv4Endpoint{} : server->bound_endpoint();
}

cluster::DistributedMutableQueryControlTcpServerMetrics
ReplicatedDistributedMutableQueryControlTcpServer::metrics() const noexcept {
  const cluster::DistributedMutableQueryControlTcpServer* const server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server == nullptr ? cluster::DistributedMutableQueryControlTcpServerMetrics{}
                           : server->metrics();
}

bool ReplicatedDistributedMutableQueryControlTcpServer::is_running() const noexcept {
  const cluster::DistributedMutableQueryControlTcpServer* const server =
      implementation_ ? implementation_->active_server() : nullptr;
  return server != nullptr && server->is_running();
}

} // namespace chronos::service
