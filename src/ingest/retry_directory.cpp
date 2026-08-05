#include "chronos/ingest/retry_directory.hpp"

#include <algorithm>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <utility>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid_argument(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status internal_error(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status capacity_exhausted() {
  return common::Status{common::StatusCode::kResourceExhausted,
                        "retry identity directory reached its configured entry bound"};
}

enum class EntryState : std::uint8_t {
  kPreWal,
  kWalStarted,
  kCommitted,
};

struct RetryEntry {
  ColumnarAppendMutationIdentity mutation;
  std::uint64_t reservation_token;
  EntryState state{EntryState::kPreWal};
  std::shared_ptr<const ColumnarAppendRetryOutcome> committed_outcome;
};

[[nodiscard]] common::Status
validate_outcome(const ColumnarAppendMutationIdentity& expected,
                 const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome) {
  if (outcome == nullptr) {
    return invalid_argument("published retry outcome must be non-null");
  }
  if (outcome->mutation != expected) {
    return invalid_argument("published retry outcome does not match its reserved mutation");
  }
  if (!outcome->wal_id.is_valid() || outcome->record_sequence == 0U ||
      outcome->applied_row_count == 0U) {
    return invalid_argument("published retry outcome has an invalid commit position or row count");
  }
  return common::Status::ok();
}

} // namespace

namespace detail {

class RetryDirectoryState : public std::enable_shared_from_this<RetryDirectoryState> {
public:
  explicit RetryDirectoryState(const std::size_t maximum_entries)
      : maximum_entries_(maximum_entries) {}

  [[nodiscard]] common::Result<RetryDecision>
  try_reserve(const RetryIdentity& identity, const ColumnarAppendMutationIdentity& mutation);
  [[nodiscard]] bool wal_started(const RetryIdentity& identity, std::uint64_t token) const;
  [[nodiscard]] common::Status mark_wal_started(const RetryIdentity& identity, std::uint64_t token);
  [[nodiscard]] common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>
  commit_published(const RetryIdentity& identity, std::uint64_t token,
                   std::shared_ptr<const ColumnarAppendRetryOutcome> outcome);
  [[nodiscard]] common::Status cancel_before_wal(const RetryIdentity& identity,
                                                 std::uint64_t token);
  void cancel_if_pre_wal(const RetryIdentity& identity, std::uint64_t token) noexcept;
  [[nodiscard]] RetryDirectoryMetrics metrics() const;

private:
  [[nodiscard]] RetryEntry* find_owned(const RetryIdentity& identity, std::uint64_t token);

  const std::size_t maximum_entries_;
  mutable std::mutex mutex_;
  std::map<RetryIdentity, RetryEntry> entries_;
  std::uint64_t next_reservation_token_{1U};
  std::size_t high_water_entries_{0U};
};

} // namespace detail

class RetryReservation::Impl {
public:
  Impl(std::shared_ptr<detail::RetryDirectoryState> state, RetryIdentity identity,
       const std::uint64_t token) noexcept
      : state_(std::move(state)), identity_(identity), token_(token) {}

  ~Impl() {
    if (state_ != nullptr) {
      state_->cancel_if_pre_wal(identity_, token_);
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  [[nodiscard]] bool wal_started() const {
    return state_ != nullptr && state_->wal_started(identity_, token_);
  }

  [[nodiscard]] common::Status mark_wal_started() {
    if (state_ == nullptr) {
      return invalid_argument("retry reservation is invalid");
    }
    return state_->mark_wal_started(identity_, token_);
  }

  [[nodiscard]] common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>
  commit_published(std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) {
    if (state_ == nullptr) {
      return common::make_unexpected(invalid_argument("retry reservation is invalid"));
    }
    common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>> committed =
        state_->commit_published(identity_, token_, std::move(outcome));
    if (committed.has_value()) {
      state_.reset();
    }
    return committed;
  }

  [[nodiscard]] common::Status cancel_before_wal() {
    if (state_ == nullptr) {
      return invalid_argument("retry reservation is invalid");
    }
    const common::Status status = state_->cancel_before_wal(identity_, token_);
    if (status.is_ok()) {
      state_.reset();
    }
    return status;
  }

private:
  std::shared_ptr<detail::RetryDirectoryState> state_;
  RetryIdentity identity_;
  std::uint64_t token_;
};

class RetryDirectory::Impl {
public:
  explicit Impl(const std::size_t maximum_entries)
      : state_(std::make_shared<detail::RetryDirectoryState>(maximum_entries)) {}

  std::shared_ptr<detail::RetryDirectoryState> state_;
};

common::Result<RetryDecision>
detail::RetryDirectoryState::try_reserve(const RetryIdentity& identity,
                                         const ColumnarAppendMutationIdentity& mutation) {
  std::lock_guard lock{mutex_};
  const auto existing = entries_.find(identity);
  if (existing != entries_.end()) {
    if (existing->second.state != EntryState::kCommitted) {
      return RetryDecision{RetryDecisionKind::kInFlight, std::nullopt, nullptr};
    }
    if (existing->second.mutation == mutation) {
      return RetryDecision{RetryDecisionKind::kMatchingCommitted, std::nullopt,
                           existing->second.committed_outcome};
    }
    return RetryDecision{RetryDecisionKind::kConflict, std::nullopt, nullptr};
  }
  if (entries_.size() >= maximum_entries_ ||
      next_reservation_token_ == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(capacity_exhausted());
  }

  const std::uint64_t token = next_reservation_token_;
  ++next_reservation_token_;
  entries_.emplace(identity, RetryEntry{.mutation = mutation, .reservation_token = token});
  std::unique_ptr<RetryReservation::Impl> reservation;
  try {
    reservation = std::make_unique<RetryReservation::Impl>(shared_from_this(), identity, token);
  } catch (...) {
    entries_.erase(identity);
    throw;
  }
  high_water_entries_ = std::max(high_water_entries_, entries_.size());
  return RetryDecision{RetryDecisionKind::kReserved,
                       std::optional<RetryReservation>{RetryReservation{std::move(reservation)}},
                       nullptr};
}

RetryEntry* detail::RetryDirectoryState::find_owned(const RetryIdentity& identity,
                                                    const std::uint64_t token) {
  const auto entry = entries_.find(identity);
  if (entry == entries_.end() || entry->second.reservation_token != token) {
    return nullptr;
  }
  return &entry->second;
}

bool detail::RetryDirectoryState::wal_started(const RetryIdentity& identity,
                                              const std::uint64_t token) const {
  std::lock_guard lock{mutex_};
  const auto entry = entries_.find(identity);
  return entry != entries_.end() && entry->second.reservation_token == token &&
         entry->second.state != EntryState::kPreWal;
}

common::Status detail::RetryDirectoryState::mark_wal_started(const RetryIdentity& identity,
                                                             const std::uint64_t token) {
  std::lock_guard lock{mutex_};
  RetryEntry* const entry = find_owned(identity, token);
  if (entry == nullptr) {
    return internal_error("retry reservation ownership was lost");
  }
  if (entry->state != EntryState::kPreWal) {
    return invalid_argument("retry reservation has already crossed its pre-WAL boundary");
  }
  entry->state = EntryState::kWalStarted;
  return common::Status::ok();
}

common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>
detail::RetryDirectoryState::commit_published(
    const RetryIdentity& identity, const std::uint64_t token,
    std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) {
  std::lock_guard lock{mutex_};
  RetryEntry* const entry = find_owned(identity, token);
  if (entry == nullptr) {
    return common::make_unexpected(internal_error("retry reservation ownership was lost"));
  }
  if (entry->state != EntryState::kWalStarted) {
    return common::make_unexpected(
        invalid_argument("retry outcome cannot commit before WAL submission begins"));
  }
  const common::Status outcome_status = validate_outcome(entry->mutation, outcome);
  if (!outcome_status.is_ok()) {
    return common::make_unexpected(outcome_status);
  }
  entry->committed_outcome = std::move(outcome);
  entry->state = EntryState::kCommitted;
  return entry->committed_outcome;
}

common::Status detail::RetryDirectoryState::cancel_before_wal(const RetryIdentity& identity,
                                                              const std::uint64_t token) {
  std::lock_guard lock{mutex_};
  RetryEntry* const entry = find_owned(identity, token);
  if (entry == nullptr) {
    return internal_error("retry reservation ownership was lost");
  }
  if (entry->state != EntryState::kPreWal) {
    return invalid_argument("retry reservation cannot be removed after WAL submission begins");
  }
  entries_.erase(identity);
  return common::Status::ok();
}

void detail::RetryDirectoryState::cancel_if_pre_wal(const RetryIdentity& identity,
                                                    const std::uint64_t token) noexcept {
  try {
    std::lock_guard lock{mutex_};
    const auto entry = entries_.find(identity);
    if (entry != entries_.end() && entry->second.reservation_token == token &&
        entry->second.state == EntryState::kPreWal) {
      entries_.erase(entry);
    }
  } catch (...) {
    std::terminate();
  }
}

RetryDirectoryMetrics detail::RetryDirectoryState::metrics() const {
  std::lock_guard lock{mutex_};
  std::size_t in_flight = 0U;
  std::size_t committed = 0U;
  for (const auto& [identity, entry] : entries_) {
    static_cast<void>(identity);
    if (entry.state == EntryState::kCommitted) {
      ++committed;
    } else {
      ++in_flight;
    }
  }
  return RetryDirectoryMetrics{.maximum_entries = maximum_entries_,
                               .entries = entries_.size(),
                               .in_flight_entries = in_flight,
                               .committed_entries = committed,
                               .high_water_entries = high_water_entries_};
}

RetryReservation::RetryReservation() noexcept = default;
RetryReservation::~RetryReservation() = default;
RetryReservation::RetryReservation(RetryReservation&&) noexcept = default;
RetryReservation& RetryReservation::operator=(RetryReservation&&) noexcept = default;

RetryReservation::RetryReservation(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

bool RetryReservation::is_valid() const noexcept {
  return implementation_ != nullptr;
}

bool RetryReservation::wal_started() const {
  return implementation_ != nullptr && implementation_->wal_started();
}

common::Status RetryReservation::mark_wal_started() {
  if (implementation_ == nullptr) {
    return invalid_argument("retry reservation is invalid");
  }
  return implementation_->mark_wal_started();
}

common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>
RetryReservation::commit_published(std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid_argument("retry reservation is invalid"));
  }
  common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>> committed =
      implementation_->commit_published(std::move(outcome));
  if (committed.has_value()) {
    implementation_.reset();
  }
  return committed;
}

common::Status RetryReservation::cancel_before_wal() {
  if (implementation_ == nullptr) {
    return invalid_argument("retry reservation is invalid");
  }
  const common::Status status = implementation_->cancel_before_wal();
  if (status.is_ok()) {
    implementation_.reset();
  }
  return status;
}

RetryDecision::RetryDecision(const RetryDecisionKind kind,
                             std::optional<RetryReservation> reservation,
                             std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) noexcept
    : kind_(kind), reservation_(std::move(reservation)), committed_outcome_(std::move(outcome)) {}

RetryReservation* RetryDecision::reservation() noexcept {
  return reservation_ ? &*reservation_ : nullptr;
}

const RetryReservation* RetryDecision::reservation() const noexcept {
  return reservation_ ? &*reservation_ : nullptr;
}

const std::shared_ptr<const ColumnarAppendRetryOutcome>&
RetryDecision::committed_outcome() const noexcept {
  return committed_outcome_;
}

RetryDirectory::RetryDirectory(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RetryDirectory::~RetryDirectory() = default;
RetryDirectory::RetryDirectory(RetryDirectory&&) noexcept = default;
RetryDirectory& RetryDirectory::operator=(RetryDirectory&&) noexcept = default;

common::Result<RetryDirectory> RetryDirectory::create(const RetryDirectoryConfig config) {
  if (config.maximum_entries == 0U) {
    return common::make_unexpected(
        invalid_argument("retry identity directory requires a nonzero entry bound"));
  }
  return RetryDirectory{std::make_unique<Impl>(config.maximum_entries)};
}

common::Result<RetryDecision>
RetryDirectory::try_reserve(const RetryIdentity& identity,
                            const ColumnarAppendMutationIdentity& mutation) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid_argument("retry directory is invalid"));
  }
  return implementation_->state_->try_reserve(identity, mutation);
}

RetryDirectoryMetrics RetryDirectory::metrics() const {
  if (implementation_ == nullptr) {
    return {};
  }
  return implementation_->state_->metrics();
}

} // namespace chronos::ingest
