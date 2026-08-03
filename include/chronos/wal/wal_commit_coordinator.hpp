#ifndef CHRONOS_WAL_WAL_COMMIT_COORDINATOR_HPP_
#define CHRONOS_WAL_WAL_COMMIT_COORDINATOR_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/wal_append_result.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::wal {

// QUORUM_SYNC is deliberately absent until replication exists. The requested and effective modes
// are both returned so a future implementation cannot silently weaken a request.
enum class WalDurabilityMode : std::uint8_t {
  kAsync,
  kLocalSync,
};

struct WalCommitCoordinatorConfig {
  // These limits cover every accepted request that has not completed, including a request being
  // appended by the worker and LOCAL_SYNC requests waiting for a synchronization boundary.
  std::size_t maximum_pending_requests{1024U};
  std::size_t maximum_pending_encoded_bytes{64U * 1024U * 1024U};

  // A sync window begins when the worker completes the first LOCAL_SYNC append. ASYNC records
  // physically appended while that window is open count toward its request and byte limits even
  // though their completions do not wait for the sync.
  std::size_t maximum_sync_batch_requests{64U};
  std::size_t maximum_sync_batch_encoded_bytes{kMaximumRecordLength};
  std::chrono::microseconds maximum_sync_batch_delay{1000};
};

struct WalCommitResult {
  std::uint64_t admission_sequence{};
  WalDurabilityMode requested_durability{WalDurabilityMode::kAsync};
  WalDurabilityMode effective_durability{WalDurabilityMode::kAsync};
  WalAppendResult append;

  // Present only for LOCAL_SYNC. It is the writer frontier returned by, or observed after, the
  // successful synchronization that covers append.record_sequence. A rotation frontier can be in
  // the newly installed segment, so durable_record_sequence is the coverage authority rather than
  // a comparison of cross-file offsets.
  std::optional<PhysicalWalPosition> synchronization_position;
  std::optional<std::uint64_t> durable_record_sequence;

  friend bool operator==(const WalCommitResult&, const WalCommitResult&) = default;
};

struct WalCommitMetrics {
  // Cumulative uint64 counters saturate instead of wrapping. Current and high-water admission
  // values remain exact within the configured size_t bounds.
  std::uint64_t admitted_requests{};
  std::uint64_t admitted_encoded_bytes{};
  std::uint64_t rejected_requests{};
  std::uint64_t appended_requests{};
  std::uint64_t appended_encoded_bytes{};
  std::uint64_t acknowledged_async_requests{};
  std::uint64_t acknowledged_async_encoded_bytes{};
  std::uint64_t acknowledged_local_sync_requests{};
  std::uint64_t acknowledged_local_sync_encoded_bytes{};
  std::uint64_t failed_requests{};
  std::uint64_t failed_encoded_bytes{};

  // These count synchronize() calls made by the coordinator. Synchronization performed internally
  // by WalWriter during rotation is not mislabeled as a coordinator call.
  std::uint64_t synchronization_attempts{};
  std::uint64_t successful_synchronizations{};
  std::uint64_t failed_synchronizations{};

  // One batch is one durable frontier that releases one or more LOCAL_SYNC requests. The frontier
  // may come from an explicit coordinator synchronize() or the writer's required rotation sync.
  std::uint64_t local_sync_batches{};
  std::uint64_t local_sync_requests_in_batches{};
  std::size_t maximum_observed_local_sync_batch_requests{};
  std::size_t maximum_observed_local_sync_batch_encoded_bytes{};

  std::size_t pending_requests{};
  std::size_t pending_encoded_bytes{};
  std::size_t high_water_pending_requests{};
  std::size_t high_water_pending_encoded_bytes{};
  bool accepting{false};
  bool terminal_failure{false};
};

namespace detail {
class WalCommitCompletionState;
class WalCommitCoordinatorTestAccess;
} // namespace detail

// An owning completion handle. It can outlive the coordinator and may be observed by more than one
// waiting thread. wait() establishes the synchronization edge from the worker's completed append or
// sync operation to the caller reading the result.
class WalCommitCompletion {
public:
  WalCommitCompletion() noexcept;
  ~WalCommitCompletion();

  WalCommitCompletion(const WalCommitCompletion&) = delete;
  WalCommitCompletion& operator=(const WalCommitCompletion&) = delete;
  WalCommitCompletion(WalCommitCompletion&&) noexcept;
  WalCommitCompletion& operator=(WalCommitCompletion&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] bool is_ready() const;
  [[nodiscard]] common::Result<WalCommitResult> wait() const;

private:
  explicit WalCommitCompletion(std::shared_ptr<detail::WalCommitCompletionState> state) noexcept;

  std::shared_ptr<detail::WalCommitCompletionState> state_;

  friend class WalCommitCoordinator;
};

// The coordinator owns exactly one worker thread, and that worker exclusively owns and calls the
// supplied WalWriter. Producers serialize only bounded admission and never call the writer.
class WalCommitCoordinator {
public:
  WalCommitCoordinator() noexcept;
  ~WalCommitCoordinator();

  WalCommitCoordinator(const WalCommitCoordinator&) = delete;
  WalCommitCoordinator& operator=(const WalCommitCoordinator&) = delete;
  WalCommitCoordinator(WalCommitCoordinator&&) noexcept;
  WalCommitCoordinator& operator=(WalCommitCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<WalCommitCoordinator>
  start(WalWriter writer, const WalCommitCoordinatorConfig& config = {});

  // Copies payload into bounded coordinator ownership before returning. Full admission returns
  // kResourceExhausted immediately; it never creates an unbounded side queue or blocks a producer.
  [[nodiscard]] common::Result<WalCommitCompletion> try_submit(common::ByteView application_payload,
                                                               WalDurabilityMode durability);

  // Stops new admission, drains every accepted request in admission order, synchronizes any final
  // LOCAL_SYNC group, closes the writer, and joins the worker. It is idempotent.
  [[nodiscard]] common::Status shutdown();

  [[nodiscard]] WalCommitMetrics metrics() const;
  [[nodiscard]] bool is_accepting() const;
  [[nodiscard]] common::Status terminal_status() const;

private:
  class Impl;

  explicit WalCommitCoordinator(std::unique_ptr<Impl> implementation) noexcept;
  [[nodiscard]] static common::Result<WalCommitCoordinator>
  start_with_worker_hook(WalWriter writer, const WalCommitCoordinatorConfig& config,
                         void (*worker_start_hook)(void*), void* worker_start_context);

  std::unique_ptr<Impl> implementation_;

  friend class detail::WalCommitCoordinatorTestAccess;
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_COMMIT_COORDINATOR_HPP_
