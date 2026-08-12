#include "chronos/service/replicated_ingest_coordinator.hpp"

#include "chronos/network/messages.hpp"
#include "chronos/service/replicated_ingest_operation.hpp"

#include <algorithm>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

void increment(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

[[nodiscard]] network::ProtocolErrorCode protocol_error(const common::StatusCode code) noexcept {
  switch (code) {
  case common::StatusCode::kCancelled:
    return network::ProtocolErrorCode::kCancelled;
  case common::StatusCode::kResourceExhausted:
    return network::ProtocolErrorCode::kOverloaded;
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kOutOfRange:
  case common::StatusCode::kNotFound:
  case common::StatusCode::kAlreadyExists:
    return network::ProtocolErrorCode::kInvalidRequest;
  case common::StatusCode::kUnauthenticated:
    return network::ProtocolErrorCode::kUnauthorized;
  case common::StatusCode::kInternal:
  case common::StatusCode::kCorruption:
    return network::ProtocolErrorCode::kInternal;
  default:
    return network::ProtocolErrorCode::kExecutionFailure;
  }
}

} // namespace

class ReplicatedIngestCoordinator::Impl {
public:
  struct Routing {
    raft::GroupId group_id;
    schema::TableId table_id;
    schema::TabletId tablet_id;
    schema::SchemaId schema_id;
    schema::SchemaVersion schema_version;
    std::vector<std::byte> command;
    raft::AsyncDurableRaftCompletion observation;
  };

  struct Pending {
    std::uint64_t connection_id{};
    std::uint64_t principal_id{};
    std::uint64_t request_id{};
    network::NetworkTaskProtocolContext protocol;
    std::chrono::steady_clock::time_point deadline;
    std::variant<Routing, ReplicatedIngestOperation> work;
  };

  Impl(raft::AsyncDurableMultiRaftRuntime& configured_runtime,
       ingest::AsyncRaftTabletApplication& configured_application,
       raft::AsyncRaftMetadataApplication& configured_metadata,
       const ReplicatedIngestCoordinatorLimits configured_limits) noexcept
      : runtime(&configured_runtime), application(&configured_application),
        metadata(&configured_metadata), limits(configured_limits) {}

  [[nodiscard]] common::Result<const schema::TableSchema*>
  active_schema(const raft::MetadataCatalogSnapshot& catalog, const schema::TableId& table_id,
                const schema::SchemaId& schema_id,
                const schema::SchemaVersion schema_version) const {
    const auto active = std::ranges::lower_bound(catalog.active_schemas, table_id, {},
                                                 &raft::ActiveSchemaMetadata::table_id);
    if (active == catalog.active_schemas.end() || active->table_id != table_id)
      return common::make_unexpected(
          unavailable("replicated ingest table has no committed active schema"));
    if (active->schema_id != schema_id)
      return common::make_unexpected(
          invalid("replicated ingest requires the committed active schema"));
    const schema::TableSchema* definition = nullptr;
    for (const raft::CatalogTableDefinition& candidate : catalog.schema_definitions) {
      if (candidate.schema == nullptr)
        return common::make_unexpected(
            corruption("replicated ingest catalog contains an empty schema definition"));
      if (candidate.schema->schema_id() == schema_id) {
        definition = candidate.schema.get();
        break;
      }
    }
    if (definition == nullptr || definition->table_id() != table_id ||
        definition->version() != schema_version)
      return common::make_unexpected(
          corruption("replicated ingest active schema definition is inconsistent"));
    return definition;
  }

  [[nodiscard]] common::Status admit(network::NetworkTask request,
                                     const std::chrono::steady_clock::time_point now) {
    const auto reject = [&](common::Status status) {
      increment(stats.rejected_requests);
      return status;
    };
    if (request.connection_id == 0U || request.principal_id == 0U ||
        request.frame.header.message_type != network::MessageType::kIngestRequest ||
        request.frame.header.request_id == 0U ||
        request.frame.header.protocol_major != request.protocol.protocol_major ||
        request.frame.header.protocol_minor != request.protocol.protocol_minor ||
        request.frame.header.payload_size != request.frame.payload.size() ||
        request.protocol.maximum_payload_size == 0U ||
        request.frame.payload.size() > request.protocol.maximum_payload_size ||
        request.protocol.protocol_major != network::kProtocolV2Major ||
        (request.protocol.feature_bits & network::kProtocolV2QuorumSyncFeature) == 0U)
      return reject(invalid("replicated ingest request lacks negotiated protocol authority"));
    if (pending.size() >= limits.maximum_pending_requests)
      return reject(exhausted("replicated ingest coordinator capacity is full"));
    if (std::ranges::any_of(pending, [&](const Pending& item) {
          return item.connection_id == request.connection_id &&
                 item.request_id == request.frame.header.request_id;
        }))
      return reject(invalid("replicated ingest request identity is already pending"));
    const network::IngestProtocolContext context{.protocol_major = request.protocol.protocol_major,
                                                 .protocol_minor = request.protocol.protocol_minor,
                                                 .feature_bits = request.protocol.feature_bits};
    network::ProtocolLimits protocol_limits = limits.protocol;
    protocol_limits.maximum_payload_size =
        std::min(protocol_limits.maximum_payload_size, request.protocol.maximum_payload_size);
    auto envelope = network::decode_ingest_request(request.frame.payload, context, protocol_limits);
    if (!envelope.has_value() || envelope->durability != network::DurabilityMode::kQuorumSync)
      return reject(invalid("replicated ingest requires negotiated QUORUM_SYNC bytes"));
    auto decoded = ingest::decode_columnar_append_v1_exact(envelope->encoded_columnar_append,
                                                           limits.columnar_append);
    if (!decoded.has_value())
      return reject(decoded.error().status());
    auto catalog = metadata->catalog_snapshot();
    if (!catalog.has_value())
      return reject(catalog.error());
    auto schema = active_schema(**catalog, decoded->table_id(), decoded->schema_id(),
                                decoded->schema_version());
    if (!schema.has_value())
      return reject(schema.error());
    const common::Status schema_status =
        ingest::validate_columnar_append_schema(*decoded, **schema);
    if (!schema_status.is_ok())
      return reject(schema_status);
    const auto placement =
        std::ranges::lower_bound((*catalog)->tablet_placements, decoded->tablet_id(), {},
                                 &raft::TabletPlacementMetadata::tablet_id);
    if (placement == (*catalog)->tablet_placements.end() ||
        placement->tablet_id != decoded->tablet_id())
      return reject(unavailable("replicated ingest tablet has no committed placement"));
    if (placement->table_id != decoded->table_id())
      return reject(invalid("replicated ingest tablet belongs to another table"));
    const auto binding =
        std::ranges::lower_bound((*catalog)->tablet_group_bindings, decoded->tablet_id(), {},
                                 &raft::TabletGroupBindingMetadata::tablet_id);
    if (binding == (*catalog)->tablet_group_bindings.end() ||
        binding->tablet_id != decoded->tablet_id())
      return reject(unavailable("replicated ingest tablet has no committed Raft group binding"));
    if (binding->group_id == metadata->group_id())
      return reject(corruption("replicated ingest tablet aliases the metadata Raft group"));
    auto observation = runtime->try_observe_group(binding->group_id);
    if (!observation.has_value())
      return reject(observation.error());
    try {
      std::vector<std::byte> command{envelope->encoded_columnar_append.begin(),
                                     envelope->encoded_columnar_append.end()};
      pending.push_back(
          {.connection_id = request.connection_id,
           .principal_id = request.principal_id,
           .request_id = request.frame.header.request_id,
           .protocol = request.protocol,
           .deadline = now + limits.request_timeout,
           .work = Routing{binding->group_id, decoded->table_id(), decoded->tablet_id(),
                           decoded->schema_id(), decoded->schema_version(), std::move(command),
                           std::move(*observation)}});
      stats.pending_requests = pending.size();
      stats.high_water_pending_requests =
          std::max(stats.high_water_pending_requests, stats.pending_requests);
      increment(stats.admitted_requests);
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return reject(exhausted("replicated ingest coordinator allocation failed"));
    } catch (const std::length_error&) {
      return reject(exhausted("replicated ingest coordinator exceeds container limits"));
    }
  }

  [[nodiscard]] common::Status validate_route(const Routing& route,
                                              const raft::RaftGroupObservation& observed) const {
    if (observed.group_id != route.group_id || observed.node_id == 0U ||
        observed.current_term == 0U || observed.last_log_index < observed.commit_index ||
        observed.commit_index < observed.applied_index)
      return corruption("replicated ingest received an invalid Raft group observation");
    auto catalog = metadata->catalog_snapshot();
    if (!catalog.has_value())
      return catalog.error();
    auto schema = active_schema(**catalog, route.table_id, route.schema_id, route.schema_version);
    if (!schema.has_value())
      return schema.error();
    const auto placement = std::ranges::lower_bound((*catalog)->tablet_placements, route.tablet_id,
                                                    {}, &raft::TabletPlacementMetadata::tablet_id);
    if (placement == (*catalog)->tablet_placements.end() || placement->tablet_id != route.tablet_id)
      return unavailable("replicated ingest tablet placement is no longer available");
    if (placement->table_id != route.table_id)
      return corruption("replicated ingest tablet placement changed table identity");
    const auto binding =
        std::ranges::lower_bound((*catalog)->tablet_group_bindings, route.tablet_id, {},
                                 &raft::TabletGroupBindingMetadata::tablet_id);
    if (binding == (*catalog)->tablet_group_bindings.end() ||
        binding->tablet_id != route.tablet_id || binding->group_id != route.group_id)
      return corruption("replicated ingest tablet Raft group binding changed");
    if (observed.role != raft::Role::kLeader || observed.leader_id != observed.node_id)
      return unavailable("replicated ingest tablet is not led by this node");
    if (!std::ranges::binary_search(placement->replicas, observed.node_id))
      return unavailable("replicated ingest leader is outside committed placement");
    if (observed.joint_membership_active || observed.joint_membership_can_finalize ||
        observed.final_membership_pending || observed.voters != placement->replicas ||
        observed.committed_voters != placement->replicas || !observed.joint_old_voters.empty() ||
        !observed.joint_new_voters.empty())
      return unavailable(
          "replicated ingest tablet membership is reconfiguring or differs from placement");
    return common::Status::ok();
  }

  [[nodiscard]] common::Result<std::optional<network::NetworkTask>>
  poll(const std::chrono::steady_clock::time_point now) {
    if (pending.empty())
      return std::optional<network::NetworkTask>{};
    for (std::size_t checked = 0U; checked < pending.size(); ++checked) {
      cursor %= pending.size();
      Pending& item = pending[cursor];
      common::Status failure;
      std::optional<ReplicatedIngestResult> completed;
      if (now >= item.deadline) {
        failure = {common::StatusCode::kCancelled, "replicated ingest request timed out"};
        increment(stats.timed_out_requests);
      } else {
        if (auto* route = std::get_if<Routing>(&item.work); route != nullptr) {
          if (!route->observation.is_ready()) {
            ++cursor;
            continue;
          }
          auto result = route->observation.wait();
          if (!result.has_value())
            failure = result.error();
          else if (result->size() != 1U)
            failure = corruption("replicated ingest route observation is not singular");
          else {
            const raft::DurableRaftResult& observed = result->front();
            if (observed.transition.has_value() ||
                (!observed.status.is_ok() && observed.observation.has_value()))
              failure = corruption("replicated ingest route observation is malformed");
            else if (!observed.status.is_ok())
              failure = observed.status;
            else if (!observed.observation.has_value())
              failure = corruption("replicated ingest route observation is missing");
            else
              failure = validate_route(*route, *observed.observation);
          }
          if (failure.is_ok()) {
            auto operation = ReplicatedIngestOperation::submit(
                route->group_id, result->front().observation->current_term,
                std::move(route->command), *runtime, *application, limits.columnar_append);
            if (!operation.has_value())
              failure = operation.error();
            else {
              item.work = std::move(*operation);
              ++cursor;
              continue;
            }
          }
        } else {
          auto result = std::get<ReplicatedIngestOperation>(item.work).poll();
          if (!result.has_value())
            failure = result.error();
          else if (result->has_value())
            completed = std::move(**result);
          else {
            ++cursor;
            continue;
          }
        }
      }
      common::Result<std::vector<std::byte>> payload =
          completed.has_value() ? encode_replicated_ingest_acknowledgement(*completed)
                                : network::encode_error_message(
                                      protocol_error(failure.code()), failure.message(),
                                      {.maximum_payload_size = item.protocol.maximum_payload_size});
      if (!payload.has_value()) {
        pending.erase(pending.begin() + static_cast<std::ptrdiff_t>(cursor));
        if (cursor == pending.size())
          cursor = 0U;
        stats.pending_requests = pending.size();
        return common::make_unexpected(payload.error());
      }
      const network::MessageType type = completed.has_value()
                                            ? network::MessageType::kQuorumSyncIngestAcknowledgement
                                            : network::MessageType::kError;
      network::NetworkTask response{
          .connection_id = item.connection_id,
          .principal_id = item.principal_id,
          .protocol = item.protocol,
          .frame = {.header = {.protocol_major = item.protocol.protocol_major,
                               .protocol_minor = item.protocol.protocol_minor,
                               .message_type = type,
                               .request_id = item.request_id,
                               .payload_size = static_cast<std::uint32_t>(payload->size())},
                    .payload = std::move(*payload)}};
      pending.erase(pending.begin() + static_cast<std::ptrdiff_t>(cursor));
      if (cursor == pending.size())
        cursor = 0U;
      stats.pending_requests = pending.size();
      increment(stats.completed_requests);
      return std::optional<network::NetworkTask>{std::move(response)};
    }
    return std::optional<network::NetworkTask>{};
  }

  raft::AsyncDurableMultiRaftRuntime* runtime;
  ingest::AsyncRaftTabletApplication* application;
  raft::AsyncRaftMetadataApplication* metadata;
  ReplicatedIngestCoordinatorLimits limits;
  std::vector<Pending> pending;
  std::size_t cursor{};
  ReplicatedIngestCoordinatorMetrics stats;
};

ReplicatedIngestCoordinator::ReplicatedIngestCoordinator(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedIngestCoordinator::~ReplicatedIngestCoordinator() = default;
ReplicatedIngestCoordinator::ReplicatedIngestCoordinator(ReplicatedIngestCoordinator&&) noexcept =
    default;
ReplicatedIngestCoordinator&
ReplicatedIngestCoordinator::operator=(ReplicatedIngestCoordinator&&) noexcept = default;

common::Result<ReplicatedIngestCoordinator> ReplicatedIngestCoordinator::create(
    raft::AsyncDurableMultiRaftRuntime& runtime, ingest::AsyncRaftTabletApplication& application,
    raft::AsyncRaftMetadataApplication& metadata, const ReplicatedIngestCoordinatorLimits limits) {
  if (limits.maximum_pending_requests == 0U || limits.maximum_pending_requests > 65'536U ||
      limits.request_timeout <= std::chrono::milliseconds::zero() ||
      !runtime.owns_worker_extension(application) || !runtime.owns_worker_extension(metadata))
    return common::make_unexpected(invalid("replicated ingest coordinator limits are invalid"));
  try {
    auto impl = std::make_unique<Impl>(runtime, application, metadata, limits);
    impl->pending.reserve(limits.maximum_pending_requests);
    return ReplicatedIngestCoordinator{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest coordinator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated ingest coordinator exceeds limits"));
  }
}

common::Status ReplicatedIngestCoordinator::admit(network::NetworkTask request,
                                                  const std::chrono::steady_clock::time_point now) {
  return impl_->admit(std::move(request), now);
}

bool ReplicatedIngestCoordinator::cancel(const std::uint64_t connection_id,
                                         const std::uint64_t request_id) noexcept {
  const auto found = std::ranges::find_if(impl_->pending, [&](const Impl::Pending& item) {
    return item.connection_id == connection_id && item.request_id == request_id;
  });
  if (found == impl_->pending.end())
    return false;
  const std::size_t offset = static_cast<std::size_t>(found - impl_->pending.begin());
  impl_->pending.erase(found);
  if (impl_->cursor > offset)
    --impl_->cursor;
  if (impl_->cursor == impl_->pending.size())
    impl_->cursor = 0U;
  impl_->stats.pending_requests = impl_->pending.size();
  increment(impl_->stats.cancelled_requests);
  return true;
}

common::Result<std::optional<network::NetworkTask>>
ReplicatedIngestCoordinator::poll(const std::chrono::steady_clock::time_point now) {
  return impl_->poll(now);
}

ReplicatedIngestCoordinatorMetrics ReplicatedIngestCoordinator::metrics() const noexcept {
  return impl_->stats;
}

} // namespace chronos::service
