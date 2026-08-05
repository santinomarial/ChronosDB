#ifndef CHRONOS_INGEST_RETRY_DIRECTORY_HPP_
#define CHRONOS_INGEST_RETRY_DIRECTORY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/identity.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/wal/types.hpp"

#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::ingest {

struct RetryIdentity {
  ClientId client_id;
  ClientBatchId client_batch_id;

  friend constexpr auto operator<=>(const RetryIdentity&, const RetryIdentity&) = default;
};

struct ColumnarAppendMutationIdentity {
  schema::TableId table_id;
  schema::TabletId tablet_id;
  Sha256Digest request_digest;

  friend constexpr auto operator<=>(const ColumnarAppendMutationIdentity&,
                                    const ColumnarAppendMutationIdentity&) = default;
};

// One immutable logical APPLIED outcome. Durability and response delivery are attempt metadata and
// deliberately do not belong here. wal_id plus record_sequence is the logical commit position.
struct ColumnarAppendRetryOutcome {
  ColumnarAppendMutationIdentity mutation;
  wal::WalId wal_id;
  std::uint64_t record_sequence;
  std::uint32_t applied_row_count;

  friend bool operator==(const ColumnarAppendRetryOutcome&,
                         const ColumnarAppendRetryOutcome&) = default;
};

struct RetryDirectoryConfig {
  // Includes both in-flight reservations and committed entries. Zero is invalid; callers must
  // choose an explicit process-memory bound because retention policy is not implemented yet.
  std::size_t maximum_entries{};
};

struct RetryDirectoryMetrics {
  std::size_t maximum_entries{};
  std::size_t entries{};
  std::size_t in_flight_entries{};
  std::size_t committed_entries{};
  std::size_t high_water_entries{};
};

namespace detail {
class RetryDirectoryState;
}

// Move-only ownership of one absent-to-in-flight transition. Destruction cancels a reservation
// only while it is still pre-WAL. Once mark_wal_started() succeeds, dropping the handle leaves the
// identity in-flight and blocked until the owning state is discarded for fresh recovery. One
// owner may transfer the handle between threads, but concurrent calls on the same handle are not
// supported.
class RetryReservation {
public:
  RetryReservation() noexcept;
  ~RetryReservation();

  RetryReservation(const RetryReservation&) = delete;
  RetryReservation& operator=(const RetryReservation&) = delete;
  RetryReservation(RetryReservation&&) noexcept;
  RetryReservation& operator=(RetryReservation&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] bool wal_started() const;

  [[nodiscard]] common::Status mark_wal_started();

  // Stores the exact immutable outcome object already published by tablet state. On success the
  // reservation becomes invalid and later matching lookups return the same shared pointer.
  [[nodiscard]] common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>
  commit_published(std::shared_ptr<const ColumnarAppendRetryOutcome> outcome);

  // Explicit pre-WAL rejection. It is invalid after mark_wal_started().
  [[nodiscard]] common::Status cancel_before_wal();

private:
  class Impl;
  explicit RetryReservation(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend class RetryDirectory;
  friend class detail::RetryDirectoryState;
};

enum class RetryDecisionKind : std::uint8_t {
  kReserved,
  kInFlight,
  kMatchingCommitted,
  kConflict,
};

class RetryDecision {
public:
  RetryDecision() = delete;
  RetryDecision(const RetryDecision&) = delete;
  RetryDecision& operator=(const RetryDecision&) = delete;
  RetryDecision(RetryDecision&&) noexcept = default;
  RetryDecision& operator=(RetryDecision&&) noexcept = default;

  [[nodiscard]] constexpr RetryDecisionKind kind() const noexcept {
    return kind_;
  }

  // Non-null only for kReserved. The caller moves the reservation out before this decision dies.
  [[nodiscard]] RetryReservation* reservation() noexcept;
  [[nodiscard]] const RetryReservation* reservation() const noexcept;

  // Non-null only for kMatchingCommitted. It is the exact tablet-published immutable object.
  [[nodiscard]] const std::shared_ptr<const ColumnarAppendRetryOutcome>&
  committed_outcome() const noexcept;

private:
  RetryDecision(RetryDecisionKind kind, std::optional<RetryReservation> reservation,
                std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) noexcept;

  RetryDecisionKind kind_;
  std::optional<RetryReservation> reservation_;
  std::shared_ptr<const ColumnarAppendRetryOutcome> committed_outcome_;

  friend class RetryDirectory;
  friend class detail::RetryDirectoryState;
};

// One process-local, database-wide identity directory. Calls through the directory and distinct
// reservation handles may be concurrent. All state operations linearize under one mutex;
// lookup/reservation never waits for another reservation and reports kInFlight immediately.
class RetryDirectory {
public:
  RetryDirectory() = delete;
  ~RetryDirectory();

  RetryDirectory(const RetryDirectory&) = delete;
  RetryDirectory& operator=(const RetryDirectory&) = delete;
  RetryDirectory(RetryDirectory&&) noexcept;
  RetryDirectory& operator=(RetryDirectory&&) noexcept;

  [[nodiscard]] static common::Result<RetryDirectory> create(RetryDirectoryConfig config);

  // An absent key reserves capacity or returns kResourceExhausted without changing state. Existing
  // keys return immediately as in-flight, matching committed, or conflicting.
  [[nodiscard]] common::Result<RetryDecision>
  try_reserve(const RetryIdentity& identity, const ColumnarAppendMutationIdentity& mutation);

  [[nodiscard]] RetryDirectoryMetrics metrics() const;

private:
  class Impl;
  explicit RetryDirectory(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RETRY_DIRECTORY_HPP_
