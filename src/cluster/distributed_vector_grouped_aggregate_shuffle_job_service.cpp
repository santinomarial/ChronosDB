#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

void increment_saturated(std::uint64_t& value) noexcept {
  if (value != std::numeric_limits<std::uint64_t>::max())
    ++value;
}

[[nodiscard]] bool same_keys(const std::span<const query::VectorGroupKeyDefinition> left,
                             const std::span<const query::VectorGroupKeyDefinition> right) {
  if (left.size() != right.size())
    return false;
  for (std::size_t index = 0U; index < left.size(); ++index) {
    if (left[index].column_ordinal != right[index].column_ordinal ||
        left[index].type != right[index].type || left[index].nullable != right[index].nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool same_prepare(const DistributedVectorGroupedAggregateShuffleJobPrepare& left,
                                const DistributedVectorGroupedAggregateShuffleJobPrepare& right) {
  return left.coordinator_node_id == right.coordinator_node_id &&
         left.target_node_id == right.target_node_id &&
         left.coordinator_result_endpoint == right.coordinator_result_endpoint &&
         left.execution_timeout == right.execution_timeout &&
         left.authority.query_id() == right.authority.query_id() &&
         left.authority.hash_version() == right.authority.hash_version() &&
         std::ranges::equal(left.authority.sources(), right.authority.sources()) &&
         std::ranges::equal(left.authority.destinations(), right.authority.destinations()) &&
         same_keys(left.authority.key_definitions(), right.authority.key_definitions()) &&
         std::ranges::equal(left.authority.aggregate_definitions(),
                            right.authority.aggregate_definitions()) &&
         left.result_schema == right.result_schema;
}

[[nodiscard]] std::chrono::steady_clock::time_point
saturating_deadline(const std::chrono::steady_clock::time_point now,
                    const std::chrono::milliseconds timeout) noexcept {
  const auto converted = std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
  if (now > std::chrono::steady_clock::time_point::max() - converted)
    return std::chrono::steady_clock::time_point::max();
  return now + converted;
}

[[nodiscard]] bool valid_timeout(const std::chrono::milliseconds timeout) noexcept {
  const auto maximum = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::duration::max());
  return timeout.count() > 0 && timeout <= maximum;
}

[[nodiscard]] bool valid_service_limits(
    const DistributedVectorGroupedAggregateShuffleJobServiceConfig& config) noexcept {
  const auto& retry = config.result_retry_limits.retry;
  const auto& source_retry = config.source_plan_limits.retry.retry;
  const auto& result_batch = config.result_batch_limits;
  return valid_timeout(config.shuffle_carrier_limits.handshake_timeout) &&
         valid_timeout(config.shuffle_carrier_limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(
             config.shuffle_carrier_limits.stream) &&
         config.source_plan_limits.maximum_total_outer_encoded_bytes > 0U &&
         config.source_plan_limits.maximum_total_outer_encoded_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes &&
         source_retry.maximum_attempts > 0U && source_retry.maximum_attempts <= 1024U &&
         valid_timeout(source_retry.initial_backoff) &&
         source_retry.maximum_backoff >= source_retry.initial_backoff &&
         valid_timeout(source_retry.maximum_backoff) &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(
             config.source_plan_limits.retry.stream) &&
         retry.maximum_attempts > 0U && retry.maximum_attempts <= 1024U &&
         valid_timeout(retry.initial_backoff) && retry.maximum_backoff >= retry.initial_backoff &&
         valid_timeout(retry.maximum_backoff) &&
         valid_timeout(config.result_carrier_limits.handshake_timeout) &&
         valid_timeout(config.result_carrier_limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(
             config.result_retry_limits.stream) &&
         validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(
             config.result_carrier_limits.stream) &&
         network::validate_protocol_limits(result_batch.protocol).is_ok() &&
         result_batch.maximum_rows > 0U && result_batch.maximum_columns > 0U &&
         result_batch.maximum_columns <= 4096U && result_batch.maximum_column_name_bytes > 0U &&
         result_batch.maximum_column_name_bytes <= 65'536U;
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_chunk(const query::VectorChunk& chunk,
             const query::DistributedVectorResultSchema& result_schema,
             const network::QueryResultLimits& limits) {
  if (chunk.column_count() != result_schema.columns.size() || chunk.selected_row_count() == 0U ||
      chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(invalid("grouped shuffle reducer-job output shape is invalid"));
  }
  try {
    std::vector<network::QueryResultColumn> columns;
    columns.reserve(result_schema.columns.size());
    for (const auto& column : result_schema.columns)
      columns.push_back({column.name, column.type, column.nullable});
    const auto cell_count =
        common::checked_multiply(chunk.selected_row_count(), result_schema.columns.size());
    if (!cell_count.has_value())
      return common::make_unexpected(
          exhausted("grouped shuffle reducer-job cell count overflowed"));
    std::vector<network::QueryResultCell> cells;
    cells.reserve(*cell_count);
    std::vector<std::byte> booleans(*cell_count);
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      for (std::size_t column = 0U; column < result_schema.columns.size(); ++column) {
        auto cell = chunk.cell({.column_ordinal = column, .selected_row = row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (cell->is_null()) {
          cells.push_back({.is_null = true});
        } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
          auto value = cell->boolean();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          const std::size_t ordinal = cells.size();
          booleans[ordinal] = *value ? std::byte{1U} : std::byte{};
          cells.push_back({.value = {&booleans[ordinal], 1U}});
        } else {
          auto value = cell->bytes();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          cells.push_back({.value = *value});
        }
      }
    }
    return network::encode_query_result_batch(
        static_cast<std::uint32_t>(chunk.selected_row_count()), columns, cells, limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job output allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle reducer-job output exceeds limits"));
  }
}

enum class JobState : std::uint8_t {
  kReceiving = 1,
  kTransmitting = 2,
  kComplete = 3,
  kFailed = 4,
  kCancelled = 5,
};

} // namespace

class DistributedVectorGroupedAggregateShuffleJobService::Impl {
public:
  struct CancelTombstone {
    common::Uuid query_id;
    raft::NodeId coordinator_node_id{};
    std::chrono::steady_clock::time_point deadline;
  };

  struct Job {
    struct SourceSubmission {
      schema::TabletId tablet_id;
      query::QueryMemoryReservation retained_bytes;
      std::vector<std::vector<std::byte>> messages;
    };

    explicit Job(DistributedVectorGroupedAggregateShuffleJobPrepare owned_prepare,
                 const network::TlsClientContext* result_context,
                 query::QueryResourceContext owned_resources,
                 const std::chrono::steady_clock::time_point owned_deadline) noexcept
        : prepare(std::move(owned_prepare)), result_tls_context(result_context),
          resources(std::move(owned_resources)), deadline(owned_deadline) {}

    DistributedVectorGroupedAggregateShuffleJobPrepare prepare;
    const network::TlsClientContext* result_tls_context{};
    query::QueryResourceContext resources;
    DistributedVectorGroupedAggregateShuffleDestinationExecution destination;
    std::optional<std::vector<DistributedQueryNodeRoute>> source_routes;
    std::vector<SourceSubmission> source_submissions;
    std::vector<DistributedVectorGroupedAggregateShuffleRetry> source_retries;
    std::optional<DistributedVectorGroupedAggregateShuffleTcpExecution> source_transport;
    std::optional<DistributedVectorGroupedAggregateShuffleResultTcpExecution> result_sender;
    std::chrono::steady_clock::time_point deadline;
    std::optional<std::chrono::milliseconds> lease_duration;
    std::optional<std::chrono::steady_clock::time_point> lease_deadline;
    JobState state{JobState::kReceiving};
    common::Status failure{common::StatusCode::kInternal,
                           "grouped shuffle reducer job has not failed"};
    bool source_publication_started{};
    bool source_transport_complete{};
  };

  Impl(DistributedVectorGroupedAggregateShuffleJobServiceConfig configured,
       std::vector<std::unique_ptr<Job>> owned_jobs,
       std::vector<CancelTombstone> owned_cancel_tombstones) noexcept
      : config(std::move(configured)), jobs(std::move(owned_jobs)),
        cancel_tombstones(std::move(owned_cancel_tombstones)) {}

  [[nodiscard]] Job* find(const common::Uuid& query_id) noexcept {
    const auto found = std::ranges::find_if(
        jobs, [&](const auto& job) { return job->prepare.authority.query_id() == query_id; });
    return found == jobs.end() ? nullptr : found->get();
  }

  [[nodiscard]] CancelTombstone* find_cancel_tombstone(const common::Uuid& query_id) noexcept {
    const auto found = std::ranges::find(cancel_tombstones, query_id, &CancelTombstone::query_id);
    return found == cancel_tombstones.end() ? nullptr : std::addressof(*found);
  }

  void prune_cancel_tombstones(const std::chrono::steady_clock::time_point now) noexcept {
    std::erase_if(cancel_tombstones,
                  [now](const auto& tombstone) { return now >= tombstone.deadline; });
    service_metrics.cancel_tombstones = cancel_tombstones.size();
  }

  [[nodiscard]] static DistributedVectorGroupedAggregateShuffleJobControlResponse
  response(const DistributedVectorGroupedAggregateShuffleJobControlAction action,
           const common::StatusCode status_code, const common::Uuid& query_id,
           const raft::NodeId coordinator_node_id, const raft::NodeId target_node_id,
           std::optional<network::Ipv4Endpoint> endpoint = std::nullopt) {
    return {.action = action,
            .status_code = status_code,
            .query_id = query_id,
            .coordinator_node_id = coordinator_node_id,
            .target_node_id = target_node_id,
            .reducer_shuffle_endpoint = endpoint};
  }

  void fail(Job& job, common::Status failure) noexcept {
    if (job.state == JobState::kFailed || job.state == JobState::kCancelled ||
        job.state == JobState::kComplete) {
      return;
    }
    static_cast<void>(job.destination.cancel());
    if (job.source_transport.has_value())
      static_cast<void>(job.source_transport->cancel());
    if (job.result_sender.has_value())
      static_cast<void>(job.result_sender->cancel());
    job.failure = std::move(failure);
    job.state = JobState::kFailed;
    increment_saturated(service_metrics.failed_jobs);
  }

  void cancel_job(Job& job) noexcept {
    if (job.state == JobState::kCancelled) {
      increment_saturated(service_metrics.duplicate_cancels);
      return;
    }
    if (job.state == JobState::kComplete || job.state == JobState::kFailed)
      return;
    static_cast<void>(job.destination.cancel());
    if (job.source_transport.has_value())
      static_cast<void>(job.source_transport->cancel());
    if (job.result_sender.has_value())
      static_cast<void>(job.result_sender->cancel());
    if (job.state == JobState::kTransmitting && service_metrics.transmitting_jobs != 0U)
      --service_metrics.transmitting_jobs;
    job.failure = {common::StatusCode::kCancelled, "grouped shuffle reducer job was cancelled"};
    job.state = JobState::kCancelled;
    increment_saturated(service_metrics.cancelled_jobs);
  }

  void expire_lease(Job& job) noexcept {
    if (job.state == JobState::kComplete || job.state == JobState::kFailed ||
        job.state == JobState::kCancelled) {
      return;
    }
    static_cast<void>(job.destination.cancel());
    if (job.source_transport.has_value())
      static_cast<void>(job.source_transport->cancel());
    if (job.result_sender.has_value())
      static_cast<void>(job.result_sender->cancel());
    if (job.state == JobState::kTransmitting && service_metrics.transmitting_jobs != 0U)
      --service_metrics.transmitting_jobs;
    job.failure = {common::StatusCode::kCancelled,
                   "grouped shuffle reducer-job coordinator lease expired"};
    job.state = JobState::kCancelled;
    increment_saturated(service_metrics.cancelled_jobs);
    increment_saturated(service_metrics.lease_expirations);
  }

  void expire_execution(Job& job) noexcept {
    if (job.state == JobState::kComplete || job.state == JobState::kFailed ||
        job.state == JobState::kCancelled) {
      return;
    }
    static_cast<void>(job.destination.cancel());
    if (job.source_transport.has_value())
      static_cast<void>(job.source_transport->cancel());
    if (job.result_sender.has_value())
      static_cast<void>(job.result_sender->cancel());
    if (job.state == JobState::kTransmitting && service_metrics.transmitting_jobs != 0U)
      --service_metrics.transmitting_jobs;
    job.failure = {common::StatusCode::kCancelled,
                   "grouped shuffle reducer-job execution deadline expired"};
    job.state = JobState::kCancelled;
    increment_saturated(service_metrics.cancelled_jobs);
  }

  [[nodiscard]] common::Result<bool>
  authorize(const network::PeerAuthenticationResult& peer,
            const raft::NodeId coordinator_node_id) const noexcept {
    if (!peer.authorized)
      return false;
    try {
      return config.node_authorizer->authorize_node(peer.principal_id, coordinator_node_id);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("grouped shuffle reducer-job authorization allocation failed"));
    } catch (...) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInternal, "grouped shuffle reducer-job authorizer threw"});
    }
  }

  [[nodiscard]] common::Status prepare_result_sender(Job& job) {
    if (job.source_publication_started && !job.source_transport_complete) {
      return {common::StatusCode::kUnavailable,
              "grouped shuffle reducer-job source delivery is incomplete"};
    }
    common::Status sealed = job.destination.seal_transport();
    if (!sealed.is_ok())
      return sealed;
    try {
      std::vector<DistributedVectorGroupedAggregateShuffleResultRetry> retries;
      const auto local_partitions =
          std::ranges::count_if(job.prepare.authority.destinations(), [&](const auto& destination) {
            return destination.node_id == config.local_node_id;
          });
      retries.reserve(static_cast<std::size_t>(local_partitions));
      for (const auto& destination : job.prepare.authority.destinations()) {
        if (destination.node_id != config.local_node_id)
          continue;
        std::vector<std::vector<std::byte>> batches;
        for (;;) {
          auto next = job.destination.next(destination.partition_id);
          if (!next.has_value())
            return next.error();
          if (next->kind() == query::PhysicalOperatorStepKind::kEnd)
            break;
          auto chunk = std::move(*next).take_chunk();
          if (!chunk.has_value())
            return chunk.error();
          auto encoded =
              encode_chunk(chunk->chunk(), job.prepare.result_schema, config.result_batch_limits);
          if (!encoded.has_value())
            return encoded.error();
          batches.push_back(std::move(*encoded));
        }
        auto retry = DistributedVectorGroupedAggregateShuffleResultRetry::create(
            job.prepare.authority, job.prepare.result_schema,
            {.partition_id = destination.partition_id,
             .source_node_id = config.local_node_id,
             .coordinator_node_id = job.prepare.coordinator_node_id},
            std::move(batches), config.result_retry_limits);
        if (!retry.has_value())
          return retry.error();
        retries.push_back(std::move(*retry));
      }
      auto sender = DistributedVectorGroupedAggregateShuffleResultTcpExecution::create(
          job.prepare.authority, job.prepare.result_schema, std::move(retries),
          {.authenticator = config.result_authenticator,
           .node_authorizer = config.node_authorizer,
           .routes = {{.node_id = job.prepare.coordinator_node_id,
                       .endpoints = {job.prepare.coordinator_result_endpoint},
                       .tls_context = job.result_tls_context}},
           .carrier_limits = config.result_carrier_limits,
           .connect_timeout = config.result_connect_timeout,
           .execution_deadline = job.deadline});
      if (!sender.has_value())
        return sender.error();
      job.result_sender.emplace(std::move(*sender));
      job.state = JobState::kTransmitting;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return exhausted("grouped shuffle reducer-job result preparation allocation failed");
    } catch (const std::length_error&) {
      return exhausted("grouped shuffle reducer-job result preparation exceeds limits");
    }
  }

  [[nodiscard]] std::size_t expected_local_sources(const Job& job) const noexcept {
    return static_cast<std::size_t>(
        std::ranges::count(job.prepare.authority.sources(), config.local_node_id,
                           &DistributedVectorGroupedAggregateShuffleSource::node_id));
  }

  [[nodiscard]] common::Status start_source_transport(Job& job) {
    if (!job.source_publication_started || job.source_transport_complete ||
        job.source_transport.has_value() ||
        job.source_submissions.size() != expected_local_sources(job) ||
        !job.source_routes.has_value()) {
      return common::Status::ok();
    }
    if (job.source_retries.empty()) {
      job.source_transport_complete = true;
      increment_saturated(service_metrics.completed_source_transports);
      return common::Status::ok();
    }
    auto transport = DistributedVectorGroupedAggregateShuffleTcpExecution::create(
        job.prepare.authority, std::move(job.source_retries),
        {.authenticator = config.shuffle_authenticator,
         .node_authorizer = config.node_authorizer,
         .routes = *job.source_routes,
         .carrier_limits = config.shuffle_carrier_limits,
         .connect_timeout = config.shuffle_connect_timeout,
         .execution_deadline = job.deadline});
    if (!transport.has_value())
      return transport.error();
    job.source_transport.emplace(std::move(*transport));
    return common::Status::ok();
  }

  DistributedVectorGroupedAggregateShuffleJobServiceConfig config;
  std::vector<std::unique_ptr<Job>> jobs;
  std::vector<CancelTombstone> cancel_tombstones;
  DistributedVectorGroupedAggregateShuffleJobServiceMetrics service_metrics;
};

DistributedVectorGroupedAggregateShuffleJobService::
    DistributedVectorGroupedAggregateShuffleJobService() noexcept = default;
DistributedVectorGroupedAggregateShuffleJobService::
    ~DistributedVectorGroupedAggregateShuffleJobService() = default;
DistributedVectorGroupedAggregateShuffleJobService::
    DistributedVectorGroupedAggregateShuffleJobService(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleJobService::
    DistributedVectorGroupedAggregateShuffleJobService(
        DistributedVectorGroupedAggregateShuffleJobService&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleJobService&
DistributedVectorGroupedAggregateShuffleJobService::operator=(
    DistributedVectorGroupedAggregateShuffleJobService&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleJobService>
DistributedVectorGroupedAggregateShuffleJobService::create(
    DistributedVectorGroupedAggregateShuffleJobServiceConfig config) {
  if (config.local_node_id == 0U || config.shuffle_listener.bind_endpoint.port != 0U ||
      config.shuffle_authenticator == nullptr || config.result_authenticator == nullptr ||
      config.node_authorizer == nullptr || config.result_tls_contexts.empty() ||
      !std::ranges::is_sorted(config.result_tls_contexts, {},
                              &DistributedQueryNodeTlsContext::node_id) ||
      std::ranges::adjacent_find(config.result_tls_contexts, {},
                                 &DistributedQueryNodeTlsContext::node_id) !=
          config.result_tls_contexts.end() ||
      std::ranges::any_of(config.result_tls_contexts,
                          [](const auto& context) {
                            return context.node_id == 0U || context.tls_context == nullptr;
                          }) ||
      config.maximum_jobs == 0U || config.maximum_jobs > 4096U ||
      config.maximum_job_query_memory_bytes == 0U ||
      config.maximum_job_query_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleJobQueryMemoryBytes ||
      config.maximum_retained_streams_per_job == 0U || config.maximum_accepts_per_job_poll == 0U ||
      config.maximum_reducer_admissions_per_job_poll == 0U ||
      config.maximum_cancel_tombstones == 0U || config.maximum_cancel_tombstones > 65'536U ||
      config.cancel_tombstone_retention.count() <= 0 ||
      config.cancel_tombstone_retention >
          distributed_vector_grouped_aggregate_shuffle_job_control_format::
              kMaximumExecutionTimeout ||
      config.result_connect_timeout.count() <= 0 ||
      config.result_connect_timeout.count() > INT_MAX ||
      config.shuffle_connect_timeout.count() <= 0 ||
      config.shuffle_connect_timeout.count() > INT_MAX || !valid_service_limits(config)) {
    return common::make_unexpected(
        invalid("grouped shuffle reducer-job service config is invalid"));
  }
  try {
    std::vector<std::unique_ptr<Impl::Job>> jobs;
    jobs.reserve(config.maximum_jobs);
    std::vector<Impl::CancelTombstone> cancel_tombstones;
    cancel_tombstones.reserve(config.maximum_cancel_tombstones);
    return DistributedVectorGroupedAggregateShuffleJobService{
        std::make_unique<Impl>(std::move(config), std::move(jobs), std::move(cancel_tombstones))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job service allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("grouped shuffle reducer-job service limits are too large"));
  }
}

common::Result<DistributedVectorGroupedAggregateShuffleJobControlResponse>
DistributedVectorGroupedAggregateShuffleJobService::receive(
    DistributedVectorGroupedAggregateShuffleJobControlRequest request,
    const network::PeerAuthenticationResult& authenticated_peer,
    const std::chrono::steady_clock::time_point now) {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle reducer-job service is empty"));
  Impl& impl = *implementation_;
  impl.prune_cancel_tombstones(now);
  if (auto* prepare = std::get_if<DistributedVectorGroupedAggregateShuffleJobPrepare>(&request)) {
    increment_saturated(impl.service_metrics.prepare_requests);
    const common::Uuid query_id = prepare->authority.query_id();
    auto authorized = impl.authorize(authenticated_peer, prepare->coordinator_node_id);
    if (!authorized.has_value())
      return common::make_unexpected(authorized.error());
    if (!*authorized)
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kUnauthenticated, query_id,
                            prepare->coordinator_node_id, prepare->target_node_id);
    if (prepare->target_node_id != impl.config.local_node_id)
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kInvalidArgument, query_id,
                            prepare->coordinator_node_id, prepare->target_node_id);
    if (const Impl::CancelTombstone* tombstone = impl.find_cancel_tombstone(query_id);
        tombstone != nullptr) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            tombstone->coordinator_node_id == prepare->coordinator_node_id
                                ? common::StatusCode::kCancelled
                                : common::StatusCode::kAlreadyExists,
                            query_id, prepare->coordinator_node_id, prepare->target_node_id);
    }
    if (Impl::Job* existing = impl.find(query_id); existing != nullptr) {
      if (!same_prepare(existing->prepare, *prepare)) {
        increment_saturated(impl.service_metrics.conflicting_prepares);
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                              common::StatusCode::kAlreadyExists, query_id,
                              prepare->coordinator_node_id, prepare->target_node_id);
      }
      increment_saturated(impl.service_metrics.duplicate_prepares);
      const auto endpoint = existing->destination.bound_endpoint();
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
          existing->state == JobState::kFailed || existing->state == JobState::kCancelled
              ? existing->failure.code()
              : common::StatusCode::kOk,
          query_id, prepare->coordinator_node_id, prepare->target_node_id,
          endpoint.port == 0U ? std::nullopt : std::optional<network::Ipv4Endpoint>{endpoint});
    }
    if (impl.jobs.size() == impl.config.maximum_jobs)
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kResourceExhausted, query_id,
                            prepare->coordinator_node_id, prepare->target_node_id);
    const auto result_context =
        std::ranges::lower_bound(impl.config.result_tls_contexts, prepare->coordinator_node_id, {},
                                 &DistributedQueryNodeTlsContext::node_id);
    if (result_context == impl.config.result_tls_contexts.end() ||
        result_context->node_id != prepare->coordinator_node_id) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kNotFound, query_id, prepare->coordinator_node_id,
                            prepare->target_node_id);
    }
    const auto execution_timeout = prepare->execution_timeout;
    const raft::NodeId coordinator_node_id = prepare->coordinator_node_id;
    const raft::NodeId target_node_id = prepare->target_node_id;
    try {
      auto resources =
          query::QueryResourceContext::create(impl.config.maximum_job_query_memory_bytes);
      if (!resources.has_value())
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                              resources.error().code(), query_id, coordinator_node_id,
                              target_node_id);
      auto job =
          std::make_unique<Impl::Job>(std::move(*prepare), result_context->tls_context, *resources,
                                      saturating_deadline(now, execution_timeout));
      auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
          job->prepare.authority,
          {.local_node_id = impl.config.local_node_id,
           .listener = impl.config.shuffle_listener,
           .tls = impl.config.shuffle_tls,
           .authenticator = impl.config.shuffle_authenticator,
           .node_authorizer = impl.config.node_authorizer,
           .resources = job->resources,
           .carrier_limits = impl.config.shuffle_carrier_limits,
           .reducer_limits = impl.config.reducer_limits,
           .maximum_retained_streams = impl.config.maximum_retained_streams_per_job,
           .maximum_accepts_per_poll = impl.config.maximum_accepts_per_job_poll,
           .maximum_reducer_admissions_per_poll =
               impl.config.maximum_reducer_admissions_per_job_poll});
      if (!destination.has_value())
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                              destination.error().code(), query_id,
                              job->prepare.coordinator_node_id, job->prepare.target_node_id);
      job->destination = std::move(*destination);
      const auto endpoint = job->destination.bound_endpoint();
      const raft::NodeId coordinator = job->prepare.coordinator_node_id;
      const raft::NodeId target = job->prepare.target_node_id;
      impl.jobs.push_back(std::move(job));
      impl.service_metrics.active_jobs = impl.jobs.size();
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kOk, query_id, coordinator, target,
                            endpoint.port == 0U ? std::nullopt
                                                : std::optional<network::Ipv4Endpoint>{endpoint});
    } catch (const std::bad_alloc&) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kResourceExhausted, query_id, coordinator_node_id,
                            target_node_id);
    } catch (const std::length_error&) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                            common::StatusCode::kResourceExhausted, query_id, coordinator_node_id,
                            target_node_id);
    }
  }

  if (const auto* routes =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobInstallRoutes>(&request)) {
    increment_saturated(impl.service_metrics.route_install_requests);
    auto authorized = impl.authorize(authenticated_peer, routes->coordinator_node_id);
    if (!authorized.has_value())
      return common::make_unexpected(authorized.error());
    if (!*authorized) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          common::StatusCode::kUnauthenticated, routes->query_id, routes->coordinator_node_id,
          routes->target_node_id);
    }
    Impl::Job* job = impl.find(routes->query_id);
    if (job == nullptr || routes->target_node_id != impl.config.local_node_id ||
        (job != nullptr && routes->coordinator_node_id != job->prepare.coordinator_node_id)) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          job == nullptr ? common::StatusCode::kNotFound : common::StatusCode::kInvalidArgument,
          routes->query_id, routes->coordinator_node_id, routes->target_node_id);
    }
    if (job->state == JobState::kFailed || job->state == JobState::kCancelled) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          job->failure.code(), routes->query_id, routes->coordinator_node_id,
          routes->target_node_id);
    }
    try {
      std::vector<raft::NodeId> expected_nodes;
      for (const auto& destination : job->prepare.authority.destinations()) {
        const bool needs_remote =
            std::ranges::any_of(job->prepare.authority.sources(), [&](const auto& source) {
              return source.node_id != destination.node_id;
            });
        if (needs_remote)
          expected_nodes.push_back(destination.node_id);
      }
      std::ranges::sort(expected_nodes);
      expected_nodes.erase(std::ranges::unique(expected_nodes).begin(), expected_nodes.end());
      if (!std::ranges::equal(expected_nodes, routes->routes, {}, std::identity{},
                              &DistributedVectorGroupedAggregateShuffleJobRoute::node_id)) {
        return Impl::response(
            DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
            common::StatusCode::kInvalidArgument, routes->query_id, routes->coordinator_node_id,
            routes->target_node_id);
      }
      const auto local_route =
          std::ranges::find(routes->routes, impl.config.local_node_id,
                            &DistributedVectorGroupedAggregateShuffleJobRoute::node_id);
      const auto local_endpoint = job->destination.bound_endpoint();
      if ((local_route == routes->routes.end()) != (local_endpoint.port == 0U) ||
          (local_route != routes->routes.end() && local_route->endpoint != local_endpoint)) {
        return Impl::response(
            DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
            common::StatusCode::kInvalidArgument, routes->query_id, routes->coordinator_node_id,
            routes->target_node_id);
      }

      std::vector<DistributedQueryNodeRoute> installed;
      installed.reserve(routes->routes.size());
      for (const auto& route : routes->routes) {
        const auto context =
            std::ranges::lower_bound(impl.config.result_tls_contexts, route.node_id, {},
                                     &DistributedQueryNodeTlsContext::node_id);
        if (context == impl.config.result_tls_contexts.end() || context->node_id != route.node_id) {
          return Impl::response(
              DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
              common::StatusCode::kNotFound, routes->query_id, routes->coordinator_node_id,
              routes->target_node_id);
        }
        installed.push_back({.node_id = route.node_id,
                             .endpoints = {route.endpoint},
                             .tls_context = context->tls_context});
      }
      if (job->source_routes.has_value()) {
        const bool same =
            job->source_routes->size() == installed.size() &&
            std::ranges::equal(
                *job->source_routes, installed, {},
                [](const auto& route) { return std::pair{route.node_id, route.endpoints.front()}; },
                [](const auto& route) {
                  return std::pair{route.node_id, route.endpoints.front()};
                });
        increment_saturated(impl.service_metrics.duplicate_route_installs);
        return Impl::response(
            DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
            same ? common::StatusCode::kOk : common::StatusCode::kAlreadyExists, routes->query_id,
            routes->coordinator_node_id, routes->target_node_id);
      }
      job->source_routes.emplace(std::move(installed));
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          common::StatusCode::kOk, routes->query_id, routes->coordinator_node_id,
          routes->target_node_id);
    } catch (const std::bad_alloc&) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          common::StatusCode::kResourceExhausted, routes->query_id, routes->coordinator_node_id,
          routes->target_node_id);
    } catch (const std::length_error&) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kInstallRoutes,
          common::StatusCode::kResourceExhausted, routes->query_id, routes->coordinator_node_id,
          routes->target_node_id);
    }
  }

  if (const auto* cancel =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobCancel>(&request)) {
    increment_saturated(impl.service_metrics.cancel_requests);
    auto authorized = impl.authorize(authenticated_peer, cancel->coordinator_node_id);
    if (!authorized.has_value())
      return common::make_unexpected(authorized.error());
    if (!*authorized) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kUnauthenticated, cancel->query_id,
                            cancel->coordinator_node_id, cancel->target_node_id);
    }
    if (cancel->target_node_id != impl.config.local_node_id) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kInvalidArgument, cancel->query_id,
                            cancel->coordinator_node_id, cancel->target_node_id);
    }
    if (Impl::Job* job = impl.find(cancel->query_id); job != nullptr) {
      if (job->prepare.coordinator_node_id != cancel->coordinator_node_id) {
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                              common::StatusCode::kInvalidArgument, cancel->query_id,
                              cancel->coordinator_node_id, cancel->target_node_id);
      }
      impl.cancel_job(*job);
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kOk, cancel->query_id, cancel->coordinator_node_id,
                            cancel->target_node_id);
    }
    if (Impl::CancelTombstone* tombstone = impl.find_cancel_tombstone(cancel->query_id);
        tombstone != nullptr) {
      if (tombstone->coordinator_node_id != cancel->coordinator_node_id) {
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                              common::StatusCode::kAlreadyExists, cancel->query_id,
                              cancel->coordinator_node_id, cancel->target_node_id);
      }
      increment_saturated(impl.service_metrics.duplicate_cancels);
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kOk, cancel->query_id, cancel->coordinator_node_id,
                            cancel->target_node_id);
    }
    if (impl.cancel_tombstones.size() == impl.config.maximum_cancel_tombstones) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kResourceExhausted, cancel->query_id,
                            cancel->coordinator_node_id, cancel->target_node_id);
    }
    try {
      impl.cancel_tombstones.push_back(
          {.query_id = cancel->query_id,
           .coordinator_node_id = cancel->coordinator_node_id,
           .deadline = saturating_deadline(now, impl.config.cancel_tombstone_retention)});
      impl.service_metrics.cancel_tombstones = impl.cancel_tombstones.size();
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kOk, cancel->query_id, cancel->coordinator_node_id,
                            cancel->target_node_id);
    } catch (const std::bad_alloc&) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
                            common::StatusCode::kResourceExhausted, cancel->query_id,
                            cancel->coordinator_node_id, cancel->target_node_id);
    }
  }

  if (const auto* renewal =
          std::get_if<DistributedVectorGroupedAggregateShuffleJobRenewLease>(&request)) {
    increment_saturated(impl.service_metrics.lease_renew_requests);
    auto authorized = impl.authorize(authenticated_peer, renewal->coordinator_node_id);
    if (!authorized.has_value())
      return common::make_unexpected(authorized.error());
    if (!*authorized) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kUnauthenticated, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    Impl::Job* job = impl.find(renewal->query_id);
    if (job == nullptr || renewal->target_node_id != impl.config.local_node_id ||
        (job != nullptr && renewal->coordinator_node_id != job->prepare.coordinator_node_id)) {
      return Impl::response(
          DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
          job == nullptr ? common::StatusCode::kNotFound : common::StatusCode::kInvalidArgument,
          renewal->query_id, renewal->coordinator_node_id, renewal->target_node_id);
    }
    if (job->state == JobState::kFailed || job->state == JobState::kCancelled) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            job->failure.code(), renewal->query_id, renewal->coordinator_node_id,
                            renewal->target_node_id);
    }
    if (job->state == JobState::kComplete) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kOk, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    const bool execution_expired = now >= job->deadline;
    const bool lease_expired = job->lease_deadline.has_value() && now >= *job->lease_deadline &&
                               (!execution_expired || *job->lease_deadline <= job->deadline);
    if (lease_expired) {
      impl.expire_lease(*job);
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kCancelled, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    if (execution_expired) {
      impl.expire_execution(*job);
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kCancelled, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    if (!job->source_routes.has_value()) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kUnavailable, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    if (job->lease_duration.has_value() && *job->lease_duration != renewal->lease_duration) {
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                            common::StatusCode::kAlreadyExists, renewal->query_id,
                            renewal->coordinator_node_id, renewal->target_node_id);
    }
    const bool activation = !job->lease_duration.has_value();
    job->lease_duration = renewal->lease_duration;
    job->lease_deadline = saturating_deadline(now, renewal->lease_duration);
    if (activation)
      increment_saturated(impl.service_metrics.lease_activations);
    else
      increment_saturated(impl.service_metrics.lease_renewals);
    return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kRenewLease,
                          common::StatusCode::kOk, renewal->query_id, renewal->coordinator_node_id,
                          renewal->target_node_id);
  }

  const auto& seal = std::get<DistributedVectorGroupedAggregateShuffleJobSeal>(request);
  increment_saturated(impl.service_metrics.seal_requests);
  auto authorized = impl.authorize(authenticated_peer, seal.coordinator_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized)
    return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
                          common::StatusCode::kUnauthenticated, seal.query_id,
                          seal.coordinator_node_id, seal.target_node_id);
  Impl::Job* job = impl.find(seal.query_id);
  if (job == nullptr || seal.target_node_id != impl.config.local_node_id ||
      (job != nullptr && seal.coordinator_node_id != job->prepare.coordinator_node_id)) {
    return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
                          job == nullptr ? common::StatusCode::kNotFound
                                         : common::StatusCode::kInvalidArgument,
                          seal.query_id, seal.coordinator_node_id, seal.target_node_id);
  }
  if (job->state == JobState::kFailed || job->state == JobState::kCancelled)
    return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
                          job->failure.code(), seal.query_id, seal.coordinator_node_id,
                          seal.target_node_id);
  if (job->state == JobState::kReceiving) {
    const common::Status started = impl.prepare_result_sender(*job);
    if (!started.is_ok()) {
      if (started.code() != common::StatusCode::kUnavailable)
        impl.fail(*job, started);
      return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
                            started.code(), seal.query_id, seal.coordinator_node_id,
                            seal.target_node_id);
    }
    ++impl.service_metrics.transmitting_jobs;
  }
  return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kSeal,
                        common::StatusCode::kOk, seal.query_id, seal.coordinator_node_id,
                        seal.target_node_id);
}

common::Status DistributedVectorGroupedAggregateShuffleJobService::accept_local_stream(
    const common::Uuid& query_id,
    const DistributedVectorGroupedAggregateShuffleCompleteStream& stream) {
  if (!implementation_)
    return invalid("grouped shuffle reducer-job service is empty");
  Impl::Job* job = implementation_->find(query_id);
  if (job == nullptr)
    return invalid("grouped shuffle reducer job was not found");
  if (job->state != JobState::kReceiving)
    return invalid("grouped shuffle reducer job is not receiving");
  return job->destination.accept_local_stream(stream);
}

common::Result<bool> DistributedVectorGroupedAggregateShuffleJobService::publish_local_source(
    const common::Uuid& query_id, const schema::TabletId& tablet_id,
    const std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
        messages) {
  if (!implementation_)
    return common::make_unexpected(invalid("grouped shuffle reducer-job service is empty"));
  Impl& impl = *implementation_;
  Impl::Job* job = impl.find(query_id);
  if (job == nullptr)
    return false;
  if (job->state != JobState::kReceiving)
    return common::make_unexpected(job->state == JobState::kFailed ||
                                           job->state == JobState::kCancelled
                                       ? job->failure
                                       : invalid("grouped shuffle reducer job is not receiving"));
  if (!job->source_routes.has_value()) {
    return common::make_unexpected(
        invalid("grouped shuffle reducer job has no installed source routes"));
  }
  auto source_node = job->prepare.authority.source_node(tablet_id);
  if (!source_node.has_value() || *source_node != impl.config.local_node_id || messages.empty()) {
    return common::make_unexpected(
        invalid("grouped shuffle source publication is not local authority"));
  }
  const auto existing = std::ranges::find(job->source_submissions, tablet_id,
                                          &Impl::Job::SourceSubmission::tablet_id);
  if (existing != job->source_submissions.end()) {
    bool same = existing->messages.size() == messages.size();
    for (std::size_t index = 0U; same && index < messages.size(); ++index)
      same = std::ranges::equal(existing->messages[index], messages[index].bytes());
    increment_saturated(impl.service_metrics.duplicate_source_submissions);
    if (!same) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "grouped shuffle source retry conflicts with retained publication"});
    }
    return true;
  }

  try {
    std::size_t retained_size{};
    for (const auto& message : messages) {
      const auto next = common::checked_add(retained_size, message.bytes().size());
      if (!next.has_value())
        return common::make_unexpected(exhausted("grouped shuffle source bytes overflowed"));
      retained_size = *next;
    }
    auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(
        job->prepare.authority, tablet_id, messages, job->resources,
        impl.config.source_plan_limits);
    if (!plan.has_value())
      return common::make_unexpected(plan.error());
    auto reservation = job->resources.reserve(retained_size);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());
    std::vector<std::vector<std::byte>> retained;
    retained.reserve(messages.size());
    for (const auto& message : messages)
      retained.emplace_back(message.bytes().begin(), message.bytes().end());
    auto local_streams = plan->take_local_streams();
    auto remote_retries = plan->take_remote_retries();
    job->source_submissions.reserve(job->source_submissions.size() + 1U);
    job->source_retries.reserve(job->source_retries.size() + remote_retries.size());
    for (const auto& stream : local_streams) {
      const common::Status accepted = job->destination.accept_local_stream(stream);
      if (!accepted.is_ok())
        return common::make_unexpected(accepted);
    }
    job->source_retries.insert(job->source_retries.end(),
                               std::make_move_iterator(remote_retries.begin()),
                               std::make_move_iterator(remote_retries.end()));
    job->source_submissions.push_back({.tablet_id = tablet_id,
                                       .retained_bytes = std::move(*reservation),
                                       .messages = std::move(retained)});
    job->source_publication_started = true;
    increment_saturated(impl.service_metrics.submitted_source_tablets);
    const common::Status started = impl.start_source_transport(*job);
    if (!started.is_ok()) {
      impl.fail(*job, started);
      return common::make_unexpected(started);
    }
    return true;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("grouped shuffle source publication allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("grouped shuffle source publication exceeds limits"));
  }
}

common::Status DistributedVectorGroupedAggregateShuffleJobService::poll_once(
    const std::chrono::milliseconds maximum_wait, const std::chrono::steady_clock::time_point now) {
  if (!implementation_)
    return invalid("grouped shuffle reducer-job service is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle reducer-job service poll timeout is invalid");
  Impl& impl = *implementation_;
  impl.prune_cancel_tombstones(now);
  bool waited{};
  for (auto& owner : impl.jobs) {
    Impl::Job& job = *owner;
    const bool active = job.state == JobState::kReceiving || job.state == JobState::kTransmitting;
    const bool execution_expired = active && now >= job.deadline;
    const bool lease_expired = active && job.lease_deadline.has_value() &&
                               now >= *job.lease_deadline &&
                               (!execution_expired || *job.lease_deadline <= job.deadline);
    if (lease_expired) {
      impl.expire_lease(job);
      continue;
    }
    if (execution_expired) {
      impl.expire_execution(job);
      continue;
    }
    if (job.state == JobState::kReceiving) {
      const auto wait = waited ? std::chrono::milliseconds{0} : maximum_wait;
      waited = waited || wait.count() != 0;
      const common::Status progress = job.destination.poll_once(wait);
      if (!progress.is_ok() && progress.code() != common::StatusCode::kResourceExhausted)
        impl.fail(job, progress);
      if (job.state == JobState::kReceiving) {
        const common::Status started = impl.start_source_transport(job);
        if (!started.is_ok())
          impl.fail(job, started);
      }
      if (job.state == JobState::kReceiving && job.source_transport.has_value()) {
        const common::Status source_progress =
            job.source_transport->poll_once(std::chrono::milliseconds{0});
        const auto source_state = job.source_transport->state();
        if (!source_progress.is_ok() ||
            source_state == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kFailed ||
            source_state == DistributedVectorGroupedAggregateShuffleTcpExecutionState::kCancelled) {
          impl.fail(job,
                    source_progress.is_ok() ? job.source_transport->failure() : source_progress);
        } else if (source_state ==
                   DistributedVectorGroupedAggregateShuffleTcpExecutionState::kComplete) {
          job.source_transport_complete = true;
          job.source_transport.reset();
          increment_saturated(impl.service_metrics.completed_source_transports);
        }
      }
    } else if (job.state == JobState::kTransmitting) {
      const auto wait = waited ? std::chrono::milliseconds{0} : maximum_wait;
      waited = waited || wait.count() != 0;
      const common::Status progress = job.result_sender->poll_once(wait);
      const auto state = job.result_sender->state();
      if (!progress.is_ok() ||
          state == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kFailed ||
          state == DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kCancelled) {
        const common::Status failure = progress.is_ok() ? job.result_sender->failure() : progress;
        impl.fail(job, failure);
        if (impl.service_metrics.transmitting_jobs != 0U)
          --impl.service_metrics.transmitting_jobs;
      } else if (state ==
                 DistributedVectorGroupedAggregateShuffleResultTcpExecutionState::kComplete) {
        job.state = JobState::kComplete;
        ++impl.service_metrics.completed_jobs;
        if (impl.service_metrics.transmitting_jobs != 0U)
          --impl.service_metrics.transmitting_jobs;
      }
    }
  }
  std::erase_if(impl.jobs, [&](const auto& job) {
    const bool terminal = job->state == JobState::kComplete || job->state == JobState::kFailed ||
                          job->state == JobState::kCancelled;
    return terminal && now >= job->deadline;
  });
  impl.service_metrics.active_jobs = impl.jobs.size();
  return common::Status::ok();
}

common::Status
DistributedVectorGroupedAggregateShuffleJobService::cancel(const common::Uuid& query_id) {
  if (!implementation_)
    return invalid("grouped shuffle reducer-job service is empty");
  Impl::Job* job = implementation_->find(query_id);
  if (job == nullptr)
    return invalid("grouped shuffle reducer job was not found");
  if (job->state == JobState::kComplete || job->state == JobState::kFailed)
    return common::Status::ok();
  implementation_->cancel_job(*job);
  return job->failure;
}

DistributedVectorGroupedAggregateShuffleJobServiceMetrics
DistributedVectorGroupedAggregateShuffleJobService::metrics() const noexcept {
  return implementation_ ? implementation_->service_metrics
                         : DistributedVectorGroupedAggregateShuffleJobServiceMetrics{};
}

} // namespace chronos::cluster
