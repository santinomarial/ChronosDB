#include "chronos/service/replicated_raft_transport_runtime.hpp"

#include "chronos/cluster/raft_transport_peer_manager.hpp"
#include "chronos/cluster/raft_transport_receiver.hpp"
#include "chronos/cluster/raft_transport_tcp_server.hpp"
#include "chronos/common/uuid_generator.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/raft/runtime_timer_driver.hpp"
#include "chronos/service/replicated_peer_authority.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const char* message) {
  return {code, message};
}

class SystemElectionDeadlines final : public raft::RaftElectionDeadlineSource {
public:
  SystemElectionDeadlines(const std::chrono::milliseconds minimum,
                          const std::chrono::milliseconds maximum) noexcept
      : minimum_(minimum), maximum_(maximum) {}

  [[nodiscard]] common::Result<raft::RaftTimerRuntime::TimePoint>
  next_election_deadline(const raft::GroupId&, raft::Term,
                         const raft::RaftTimerRuntime::TimePoint now) override {
    auto entropy = entropy_.generate();
    if (!entropy.has_value())
      return common::make_unexpected(entropy.error());
    std::uint64_t random{};
    for (std::size_t index = 0U; index < sizeof(random); ++index)
      random = (random << 8U) | std::to_integer<std::uint8_t>(entropy->bytes()[index]);
    const auto range = static_cast<std::uint64_t>((maximum_ - minimum_).count());
    const auto delay = minimum_ + std::chrono::milliseconds{random % (range + 1U)};
    const auto converted =
        std::chrono::duration_cast<raft::RaftTimerRuntime::TimePoint::duration>(delay);
    if (now > raft::RaftTimerRuntime::TimePoint::max() - converted)
      return raft::RaftTimerRuntime::TimePoint::max();
    return now + converted;
  }

private:
  std::chrono::milliseconds minimum_;
  std::chrono::milliseconds maximum_;
  common::SystemUuidGenerator entropy_;
};

[[nodiscard]] bool valid_timeouts(const ReplicatedRaftTransportLimits& limits) noexcept {
  constexpr std::chrono::milliseconds maximum_election{60'000};
  return limits.minimum_election_timeout.count() > 0 &&
         limits.minimum_election_timeout > limits.timers.timers.heartbeat_interval &&
         limits.maximum_election_timeout >= limits.minimum_election_timeout &&
         limits.maximum_election_timeout <= maximum_election && limits.connect_timeout.count() > 0;
}

} // namespace

class ReplicatedRaftTransportRuntime::Impl {
public:
  Impl(ReplicatedPeerAuthority configured_authority,
       raft::AsyncDurableMultiRaftRuntime* configured_durable,
       const std::chrono::milliseconds minimum_election,
       const std::chrono::milliseconds maximum_election) noexcept
      : authority(std::move(configured_authority)), deadlines(minimum_election, maximum_election),
        durable(configured_durable) {}

  [[nodiscard]] common::Status initialize(ReplicatedRaftTransportRuntimeConfig config) {
    auto receiver_value =
        cluster::RaftTransportReceiver::create({.local_node_id = config.local_node_id,
                                                .authorizer = &authority,
                                                .runtime = durable,
                                                .codec_limits = config.limits.peer_pool.codec});
    if (!receiver_value.has_value())
      return receiver_value.error();
    receiver.emplace(std::move(*receiver_value));

    try {
      const std::size_t remote_count = authority.peers().size() - 1U;
      client_contexts.reserve(remote_count);
      std::vector<cluster::RaftTransportPeerReconnectConfig> reconnects;
      reconnects.reserve(remote_count);
      cluster::RaftTransportTlsClientLimits outbound_limits = config.limits.outbound_carrier;
      outbound_limits.codec = config.limits.peer_pool.codec;
      for (const ReplicatedPeer& peer : authority.peers()) {
        if (peer.node_id == config.local_node_id)
          continue;
        auto context = network::TlsClientContext::create(
            {.certificate_chain_file = config.tls.certificate_chain_file,
             .private_key_file = config.tls.private_key_file,
             .trust_store_file = config.tls.trust_store_file,
             .expected_server_identity = peer.tls_server_identity});
        if (!context.has_value())
          return context.error();
        client_contexts.push_back(std::move(*context));
        reconnects.push_back({.connector = {.remote_endpoint = peer.endpoint,
                                            .tls_context = &client_contexts.back(),
                                            .carrier = {.local_node_id = config.local_node_id,
                                                        .peer_node_id = peer.node_id,
                                                        .authenticator = &authority,
                                                        .node_authorizer = &authority,
                                                        .peer_ipv4_address = peer.endpoint.address,
                                                        .limits = outbound_limits},
                                            .connect_timeout = config.limits.connect_timeout},
                              .limits = config.limits.reconnect});
      }

      auto inbound = cluster::RaftTransportTcpServer::start(
          {.listener = {.bind_endpoint = authority.local_peer().endpoint},
           .tls = {.certificate_chain_file = config.tls.certificate_chain_file,
                   .private_key_file = config.tls.private_key_file,
                   .trust_store_file = config.tls.trust_store_file},
           .authenticator = &authority,
           .receiver = &*receiver,
           .carrier_limits = config.limits.inbound_carrier,
           .codec_limits = config.limits.peer_pool.codec,
           .maximum_connections = config.limits.maximum_inbound_connections,
           .maximum_accepts_per_poll = config.limits.maximum_accepts_per_poll});
      if (!inbound.has_value())
        return inbound.error();
      auto outbound =
          cluster::RaftTransportPeerManager::create({.local_node_id = config.local_node_id,
                                                     .peers = std::move(reconnects),
                                                     .pool = config.limits.peer_pool});
      if (!outbound.has_value())
        return outbound.error();
      auto timer = raft::RaftTimerDriver::create(
          {.runtime = durable, .election_deadlines = &deadlines, .limits = config.limits.timers});
      if (!timer.has_value())
        return timer.error();
      auto transport_value =
          cluster::RaftTransportRuntime::create(durable, std::move(*timer), std::move(*inbound),
                                                std::move(*outbound), config.limits.runtime);
      if (!transport_value.has_value())
        return transport_value.error();
      transport.emplace(std::move(*transport_value));

      const auto now = std::chrono::steady_clock::now();
      for (const raft::GroupId& group_id : config.resident_groups) {
        auto completion = durable->try_observe_group(group_id);
        if (!completion.has_value())
          return completion.error();
        auto results = completion->wait();
        if (!results.has_value())
          return results.error();
        if (results->size() != 1U || !results->front().status.is_ok() ||
            results->front().transition.has_value() || !results->front().observation.has_value() ||
            results->front().observation->group_id != group_id ||
            results->front().observation->node_id != config.local_node_id)
          return status(common::StatusCode::kCorruption,
                        "replicated transport initial group observation is invalid");
        const common::Status added = transport->add_group(*results->front().observation, now);
        if (!added.is_ok())
          return added;
      }
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return status(common::StatusCode::kResourceExhausted,
                    "replicated Raft transport allocation failed");
    } catch (const std::length_error&) {
      return status(common::StatusCode::kResourceExhausted,
                    "replicated Raft transport configuration exceeds limits");
    }
  }

  ReplicatedPeerAuthority authority;
  SystemElectionDeadlines deadlines;
  raft::AsyncDurableMultiRaftRuntime* durable{};
  std::optional<cluster::RaftTransportReceiver> receiver;
  std::vector<network::TlsClientContext> client_contexts;
  std::optional<cluster::RaftTransportRuntime> transport;
};

ReplicatedRaftTransportRuntime::ReplicatedRaftTransportRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedRaftTransportRuntime::~ReplicatedRaftTransportRuntime() = default;
ReplicatedRaftTransportRuntime::ReplicatedRaftTransportRuntime(
    ReplicatedRaftTransportRuntime&&) noexcept = default;
ReplicatedRaftTransportRuntime&
ReplicatedRaftTransportRuntime::operator=(ReplicatedRaftTransportRuntime&&) noexcept = default;

common::Result<ReplicatedRaftTransportRuntime>
ReplicatedRaftTransportRuntime::create(ReplicatedRaftTransportRuntimeConfig config) {
  if (config.local_node_id == 0U || config.durable_runtime == nullptr || config.peers.size() < 2U ||
      config.resident_groups.empty() || config.tls.certificate_chain_file.empty() ||
      config.tls.private_key_file.empty() || config.tls.trust_store_file.empty() ||
      !valid_timeouts(config.limits))
    return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                          "replicated Raft transport configuration is invalid"));
  if (std::ranges::any_of(config.resident_groups, &raft::GroupId::is_nil))
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "replicated Raft transport group is nil"));
  std::ranges::sort(config.resident_groups);
  if (std::ranges::adjacent_find(config.resident_groups) != config.resident_groups.end())
    return common::make_unexpected(status(common::StatusCode::kAlreadyExists,
                                          "replicated Raft transport group is duplicated"));
  auto authority = ReplicatedPeerAuthority::create(config.local_node_id, std::move(config.peers));
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  try {
    auto impl = std::make_unique<Impl>(std::move(*authority), config.durable_runtime,
                                       config.limits.minimum_election_timeout,
                                       config.limits.maximum_election_timeout);
    const common::Status initialized = impl->initialize(std::move(config));
    if (!initialized.is_ok())
      return common::make_unexpected(initialized);
    return ReplicatedRaftTransportRuntime{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                          "replicated Raft transport owner allocation failed"));
  }
}

common::Status
ReplicatedRaftTransportRuntime::poll_once(const std::chrono::milliseconds maximum_wait) {
  if (!is_running())
    return status(common::StatusCode::kUnavailable, "replicated Raft transport is not running");
  return impl_->transport->poll_once(maximum_wait);
}

common::Result<std::uint64_t>
ReplicatedRaftTransportRuntime::try_submit_application(raft::DurableRaftRequest request) {
  if (!is_running())
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "replicated Raft transport is not running"));
  return impl_->transport->try_submit_application(std::move(request));
}

common::Result<cluster::RaftTransportRuntimeResult>
ReplicatedRaftTransportRuntime::take_completed() {
  if (!is_running())
    return common::make_unexpected(
        status(common::StatusCode::kUnavailable, "replicated Raft transport is not running"));
  return impl_->transport->take_completed();
}

network::Ipv4Endpoint ReplicatedRaftTransportRuntime::bound_endpoint() const noexcept {
  return is_running() ? impl_->transport->bound_endpoint() : network::Ipv4Endpoint{};
}

cluster::RaftTransportRuntimeMetrics ReplicatedRaftTransportRuntime::metrics() const noexcept {
  return is_running() ? impl_->transport->metrics() : cluster::RaftTransportRuntimeMetrics{};
}

bool ReplicatedRaftTransportRuntime::is_running() const noexcept {
  return impl_ != nullptr && impl_->transport.has_value();
}

common::Status ReplicatedRaftTransportRuntime::shutdown() {
  if (!impl_ || !impl_->transport.has_value())
    return common::Status::ok();
  const common::Status result =
      impl_->transport->failed() ? impl_->transport->failure() : common::Status::ok();
  impl_->transport.reset();
  return result;
}

} // namespace chronos::service
