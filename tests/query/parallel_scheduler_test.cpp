#include "chronos/common/status.hpp"
#include "chronos/query/parallel_scheduler.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] columnar::OwnedPhysicalColumn
int64_column(const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   const std::span<const std::int64_t> values) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::all(static_cast<std::uint32_t>(values.size())).value())
          .value();
  QueryMemoryReservation reservation = resources.reserve(chunk.retained_buffer_bytes()).value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

[[nodiscard]] std::int64_t selected_int64(const VectorChunk& chunk,
                                          const std::size_t selected_row) {
  const std::span<const std::byte> bytes =
      chunk.cell({.column_ordinal = 0U, .selected_row = selected_row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte) {
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  }
  return std::bit_cast<std::int64_t>(bits);
}

class EndSource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

class CountingEndSource final : public PhysicalOperator {
public:
  explicit CountingEndSource(std::shared_ptr<std::atomic<std::size_t>> executions)
      : executions_(std::move(executions)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    executions_->fetch_add(1U, std::memory_order_relaxed);
    return PhysicalOperatorStep::end();
  }

private:
  std::shared_ptr<std::atomic<std::size_t>> executions_;
};

struct StartGate {
  explicit StartGate(const std::size_t expected_count) : expected(expected_count) {}

  void arrive_and_wait() {
    std::unique_lock lock{mutex};
    ++arrived;
    condition.notify_all();
    condition.wait(lock, [&] { return arrived == expected; });
  }

  std::mutex mutex;
  std::condition_variable condition;
  std::size_t expected;
  std::size_t arrived{};
};

class ChunkTask final : public PhysicalOperator {
public:
  ChunkTask(std::vector<AccountedVectorChunk> chunks, std::shared_ptr<StartGate> gate,
            std::shared_ptr<std::atomic<bool>> affinity_broken)
      : chunks_(std::move(chunks)), gate_(std::move(gate)),
        affinity_broken_(std::move(affinity_broken)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    if (!thread_.has_value()) {
      thread_ = std::this_thread::get_id();
      if (gate_ != nullptr)
        gate_->arrive_and_wait();
    } else if (*thread_ != std::this_thread::get_id()) {
      affinity_broken_->store(true, std::memory_order_relaxed);
    }
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::shared_ptr<StartGate> gate_;
  std::shared_ptr<std::atomic<bool>> affinity_broken_;
  std::optional<std::thread::id> thread_;
  std::size_t next_{};
};

class CoordinatedFailureSource final : public PhysicalOperator {
public:
  CoordinatedFailureSource(std::shared_ptr<StartGate> gate, common::Status error)
      : gate_(std::move(gate)), error_(std::move(error)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    gate_->arrive_and_wait();
    return common::make_unexpected(error_);
  }

private:
  std::shared_ptr<StartGate> gate_;
  common::Status error_;
};

class ThrowingSource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    throw std::bad_alloc{};
  }
};

[[nodiscard]] std::vector<std::unique_ptr<PhysicalOperator>> end_tasks(const std::size_t count) {
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  tasks.reserve(count);
  for (std::size_t task = 0U; task < count; ++task)
    tasks.push_back(std::make_unique<EndSource>());
  return tasks;
}

TEST(ParallelSchedulerTest, RunsIndependentTasksConcurrentlyWithBoundedUnorderedMerge) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto gate = std::make_shared<StartGate>(2U);
  auto affinity_broken = std::make_shared<std::atomic<bool>>(false);
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  for (std::int64_t task = 0; task < 2; ++task) {
    std::vector<AccountedVectorChunk> chunks;
    chunks.push_back(
        accounted_chunk(resources, std::vector<std::int64_t>{task * 10, task * 10 + 1}));
    chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{task * 10 + 2}));
    tasks.push_back(std::make_unique<ChunkTask>(std::move(chunks), gate, affinity_broken));
  }

  auto scheduler =
      ParallelMergeOperator::create(resources, std::move(tasks),
                                    {.maximum_tasks = 2U,
                                     .maximum_workers = 2U,
                                     .maximum_ready_chunks = 1U,
                                     .maximum_retained_configuration_bytes = 1U << 20U})
          .value();
  std::vector<std::int64_t> actual;
  for (;;) {
    common::Result<PhysicalOperatorStep> step = scheduler->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().message();
    if (step->kind() == PhysicalOperatorStepKind::kEnd)
      break;
    AccountedVectorChunk chunk = std::move(*step).take_chunk().value();
    for (std::size_t row = 0U; row < chunk.chunk().selected_row_count(); ++row)
      actual.push_back(selected_int64(chunk.chunk(), row));
  }
  std::ranges::sort(actual);
  EXPECT_EQ(actual, (std::vector<std::int64_t>{0, 1, 2, 10, 11, 12}));
  EXPECT_FALSE(affinity_broken->load(std::memory_order_relaxed));
  EXPECT_EQ(scheduler->worker_count(), 2U);
  const ParallelSchedulerMetrics metrics = scheduler->metrics();
  EXPECT_EQ(metrics.tasks_started, 2U);
  EXPECT_EQ(metrics.tasks_completed, 2U);
  EXPECT_EQ(metrics.chunks_published, 4U);
  EXPECT_EQ(metrics.peak_ready_chunks, 1U);
  EXPECT_GT(scheduler->retained_configuration_bytes(), 0U);
  scheduler.reset();
  EXPECT_FALSE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ParallelSchedulerTest, SelectsDeterministicLowestOrdinalNonCancellationFailure) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto gate = std::make_shared<StartGate>(2U);
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  tasks.push_back(std::make_unique<CoordinatedFailureSource>(
      gate, common::Status{common::StatusCode::kInvalidArgument, "task zero"}));
  tasks.push_back(std::make_unique<CoordinatedFailureSource>(
      gate, common::Status{common::StatusCode::kInternal, "task one"}));
  auto scheduler =
      ParallelMergeOperator::create(resources, std::move(tasks),
                                    {.maximum_tasks = 2U,
                                     .maximum_workers = 2U,
                                     .maximum_ready_chunks = 1U,
                                     .maximum_retained_configuration_bytes = 1U << 20U})
          .value();
  const common::Result<PhysicalOperatorStep> step = scheduler->next(resources);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(step.error().message(), "task zero");
  scheduler.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ParallelSchedulerTest, ClassifiesUnexpectedWorkerAllocationFailureAndReleasesCredit) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  tasks.push_back(std::make_unique<ThrowingSource>());
  auto scheduler = ParallelMergeOperator::create(resources, std::move(tasks)).value();
  const common::Result<PhysicalOperatorStep> step = scheduler->next(resources);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(resources.is_cancelled());
  scheduler.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ParallelSchedulerTest, RejectsHostileShapesAndForeignQueryPulls) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  EXPECT_EQ(ParallelMergeOperator::create(resources, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  ParallelSchedulerLimits zero_workers;
  zero_workers.maximum_workers = 0U;
  EXPECT_EQ(ParallelMergeOperator::create(resources, end_tasks(1U), zero_workers).error().code(),
            common::StatusCode::kInvalidArgument);
  ParallelSchedulerLimits too_few_tasks;
  too_few_tasks.maximum_tasks = 1U;
  EXPECT_EQ(ParallelMergeOperator::create(resources, end_tasks(2U), too_few_tasks).error().code(),
            common::StatusCode::kResourceExhausted);
  auto null_tasks = end_tasks(1U);
  null_tasks.push_back(nullptr);
  EXPECT_EQ(ParallelMergeOperator::create(resources, std::move(null_tasks)).error().code(),
            common::StatusCode::kInvalidArgument);
  ParallelSchedulerLimits tiny_configuration;
  tiny_configuration.maximum_retained_configuration_bytes = 1U;
  EXPECT_EQ(
      ParallelMergeOperator::create(resources, end_tasks(1U), tiny_configuration).error().code(),
      common::StatusCode::kResourceExhausted);
  ParallelSchedulerLimits overflowing_queue;
  overflowing_queue.maximum_ready_chunks = std::numeric_limits<std::size_t>::max();
  overflowing_queue.maximum_retained_configuration_bytes = std::numeric_limits<std::size_t>::max();
  EXPECT_EQ(
      ParallelMergeOperator::create(resources, end_tasks(1U), overflowing_queue).error().code(),
      common::StatusCode::kResourceExhausted);

  auto scheduler = ParallelMergeOperator::create(resources, end_tasks(1U)).value();
  QueryResourceContext foreign = QueryResourceContext::create(1U << 20U).value();
  const common::Result<PhysicalOperatorStep> step = scheduler->next(foreign);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kInvalidArgument);
  scheduler.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(ParallelSchedulerTest, AppliesExactWorkerPlacementsBeforeReturningFromCreate) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array<runtime::ThreadPlacement, 2U> two_placements{};
  EXPECT_EQ(
      ParallelMergeOperator::create(resources, end_tasks(1U), {}, two_placements).error().code(),
      common::StatusCode::kInvalidArgument);

  runtime::ThreadPlacement unsupported;
  unsupported.numa_node = 0U;
  const std::array<runtime::ThreadPlacement, 2U> unsupported_placement{runtime::ThreadPlacement{},
                                                                       unsupported};
  auto executions = std::make_shared<std::atomic<std::size_t>>(0U);
  std::vector<std::unique_ptr<PhysicalOperator>> blocked_tasks;
  blocked_tasks.push_back(std::make_unique<CountingEndSource>(executions));
  blocked_tasks.push_back(std::make_unique<CountingEndSource>(executions));
  auto failed = ParallelMergeOperator::create(resources, std::move(blocked_tasks),
                                              {.maximum_tasks = 2U,
                                               .maximum_workers = 2U,
                                               .maximum_ready_chunks = 1U,
                                               .maximum_retained_configuration_bytes = 1U << 20U},
                                              unsupported_placement);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kNotSupported);
  EXPECT_EQ(executions->load(std::memory_order_relaxed), 0U);
  EXPECT_FALSE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto scheduler =
      ParallelMergeOperator::create(resources, end_tasks(2U),
                                    {.maximum_tasks = 2U,
                                     .maximum_workers = 2U,
                                     .maximum_ready_chunks = 1U,
                                     .maximum_retained_configuration_bytes = 1U << 20U},
                                    two_placements);
  ASSERT_TRUE(scheduler.has_value()) << scheduler.error().to_string();
  const auto end = (*scheduler)->next(resources);
  ASSERT_TRUE(end.has_value()) << end.error().to_string();
  EXPECT_EQ(end->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(ParallelSchedulerTest, EarlyDestructionCancelsWorkersAndReleasesQueuedCredit) {
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto affinity_broken = std::make_shared<std::atomic<bool>>(false);
  std::vector<AccountedVectorChunk> chunks;
  for (std::int64_t value = 0; value < 8; ++value)
    chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{value}));
  std::vector<std::unique_ptr<PhysicalOperator>> tasks;
  tasks.push_back(std::make_unique<ChunkTask>(std::move(chunks), nullptr, affinity_broken));
  auto scheduler =
      ParallelMergeOperator::create(resources, std::move(tasks),
                                    {.maximum_tasks = 1U,
                                     .maximum_workers = 1U,
                                     .maximum_ready_chunks = 1U,
                                     .maximum_retained_configuration_bytes = 1U << 20U})
          .value();
  scheduler.reset();
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
