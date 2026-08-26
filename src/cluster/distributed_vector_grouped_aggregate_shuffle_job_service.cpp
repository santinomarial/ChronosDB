#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_service.hpp"

#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
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
  const auto& result_batch = config.result_batch_limits;
  return valid_timeout(config.shuffle_carrier_limits.handshake_timeout) &&
         valid_timeout(config.shuffle_carrier_limits.exchange_timeout) &&
         validate_distributed_vector_grouped_aggregate_shuffle_stream_limits(
             config.shuffle_carrier_limits.stream) &&
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
  struct Job {
    explicit Job(DistributedVectorGroupedAggregateShuffleJobPrepare owned_prepare,
                 const std::chrono::steady_clock::time_point owned_deadline) noexcept
        : prepare(std::move(owned_prepare)), deadline(owned_deadline) {}

    DistributedVectorGroupedAggregateShuffleJobPrepare prepare;
    DistributedVectorGroupedAggregateShuffleDestinationExecution destination;
    std::optional<DistributedVectorGroupedAggregateShuffleResultTcpExecution> result_sender;
    std::chrono::steady_clock::time_point deadline;
    JobState state{JobState::kReceiving};
    common::Status failure{common::StatusCode::kInternal,
                           "grouped shuffle reducer job has not failed"};
  };

  Impl(DistributedVectorGroupedAggregateShuffleJobServiceConfig configured,
       std::vector<std::unique_ptr<Job>> owned_jobs) noexcept
      : config(std::move(configured)), jobs(std::move(owned_jobs)) {}

  [[nodiscard]] Job* find(const common::Uuid& query_id) noexcept {
    const auto found = std::ranges::find_if(
        jobs, [&](const auto& job) { return job->prepare.authority.query_id() == query_id; });
    return found == jobs.end() ? nullptr : found->get();
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
    if (job.result_sender.has_value())
      static_cast<void>(job.result_sender->cancel());
    job.failure = std::move(failure);
    job.state = JobState::kFailed;
    increment_saturated(service_metrics.failed_jobs);
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
                       .tls_context = config.result_tls_context}},
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

  DistributedVectorGroupedAggregateShuffleJobServiceConfig config;
  std::vector<std::unique_ptr<Job>> jobs;
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
      config.node_authorizer == nullptr || config.result_tls_context == nullptr ||
      config.maximum_jobs == 0U || config.maximum_jobs > 4096U ||
      config.maximum_job_query_memory_bytes == 0U ||
      config.maximum_job_query_memory_bytes >
          kMaximumDistributedVectorGroupedAggregateShuffleJobQueryMemoryBytes ||
      config.maximum_retained_streams_per_job == 0U || config.maximum_accepts_per_job_poll == 0U ||
      config.maximum_reducer_admissions_per_job_poll == 0U ||
      config.result_connect_timeout.count() <= 0 ||
      config.result_connect_timeout.count() > INT_MAX || !valid_service_limits(config)) {
    return common::make_unexpected(
        invalid("grouped shuffle reducer-job service config is invalid"));
  }
  try {
    std::vector<std::unique_ptr<Impl::Job>> jobs;
    jobs.reserve(config.maximum_jobs);
    return DistributedVectorGroupedAggregateShuffleJobService{
        std::make_unique<Impl>(std::move(config), std::move(jobs))};
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
    const auto execution_timeout = prepare->execution_timeout;
    const raft::NodeId coordinator_node_id = prepare->coordinator_node_id;
    const raft::NodeId target_node_id = prepare->target_node_id;
    try {
      auto job = std::make_unique<Impl::Job>(std::move(*prepare),
                                             saturating_deadline(now, execution_timeout));
      auto resources =
          query::QueryResourceContext::create(impl.config.maximum_job_query_memory_bytes);
      if (!resources.has_value())
        return Impl::response(DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
                              resources.error().code(), query_id, job->prepare.coordinator_node_id,
                              job->prepare.target_node_id);
      auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
          job->prepare.authority,
          {.local_node_id = impl.config.local_node_id,
           .listener = impl.config.shuffle_listener,
           .tls = impl.config.shuffle_tls,
           .authenticator = impl.config.shuffle_authenticator,
           .node_authorizer = impl.config.node_authorizer,
           .resources = *resources,
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

common::Status DistributedVectorGroupedAggregateShuffleJobService::poll_once(
    const std::chrono::milliseconds maximum_wait, const std::chrono::steady_clock::time_point now) {
  if (!implementation_)
    return invalid("grouped shuffle reducer-job service is empty");
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return invalid("grouped shuffle reducer-job service poll timeout is invalid");
  Impl& impl = *implementation_;
  bool waited{};
  for (auto& owner : impl.jobs) {
    Impl::Job& job = *owner;
    if ((job.state == JobState::kReceiving || job.state == JobState::kTransmitting) &&
        now >= job.deadline) {
      static_cast<void>(job.destination.cancel());
      if (job.result_sender.has_value())
        static_cast<void>(job.result_sender->cancel());
      job.failure = {common::StatusCode::kCancelled,
                     "grouped shuffle reducer-job execution deadline expired"};
      job.state = JobState::kCancelled;
      increment_saturated(impl.service_metrics.cancelled_jobs);
      if (impl.service_metrics.transmitting_jobs != 0U && job.result_sender.has_value())
        --impl.service_metrics.transmitting_jobs;
      continue;
    }
    if (job.state == JobState::kReceiving) {
      const auto wait = waited ? std::chrono::milliseconds{0} : maximum_wait;
      waited = waited || wait.count() != 0;
      const common::Status progress = job.destination.poll_once(wait);
      if (!progress.is_ok() && progress.code() != common::StatusCode::kResourceExhausted)
        impl.fail(job, progress);
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
  if (job->state == JobState::kCancelled)
    return job->failure;
  if (job->state == JobState::kComplete || job->state == JobState::kFailed)
    return common::Status::ok();
  static_cast<void>(job->destination.cancel());
  if (job->result_sender.has_value())
    static_cast<void>(job->result_sender->cancel());
  if (job->state == JobState::kTransmitting &&
      implementation_->service_metrics.transmitting_jobs != 0U)
    --implementation_->service_metrics.transmitting_jobs;
  job->failure = {common::StatusCode::kCancelled, "grouped shuffle reducer job was cancelled"};
  job->state = JobState::kCancelled;
  increment_saturated(implementation_->service_metrics.cancelled_jobs);
  return job->failure;
}

DistributedVectorGroupedAggregateShuffleJobServiceMetrics
DistributedVectorGroupedAggregateShuffleJobService::metrics() const noexcept {
  return implementation_ ? implementation_->service_metrics
                         : DistributedVectorGroupedAggregateShuffleJobServiceMetrics{};
}

} // namespace chronos::cluster
