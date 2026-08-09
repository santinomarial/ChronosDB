#include "chronos/ingest/tablet_state.hpp"

#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "deduplication_key.hpp"
#include "sealed_head_flush_queue_internal.hpp"

#include <algorithm>
#include <atomic>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <new>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status already_exists(std::string message) {
  return common::Status{common::StatusCode::kAlreadyExists, std::move(message)};
}

enum class PreparedPhase : std::uint8_t {
  kPreWal,
  kWalStarted,
};

using RetryTable = std::map<RetryIdentity, std::shared_ptr<const ColumnarAppendRetryOutcome>>;
using GenerationSet = std::vector<head::HeadSnapshot>;

struct RegisteredSchema {
  std::shared_ptr<const schema::TableSchema> schema;
  head::MutableHeadCapacity capacity;
};

static_assert(std::is_nothrow_move_assignable_v<schema::SchemaLineage>);

} // namespace

namespace detail {

class TabletPublication {
public:
  TabletPublication(std::optional<head::HeadCommitPosition> applied_position,
                    std::shared_ptr<const GenerationSet> sealed_generations,
                    head::HeadSnapshot active_generation,
                    std::shared_ptr<const RetryTable> retries) noexcept
      : applied_position_(applied_position), sealed_generations_(std::move(sealed_generations)),
        active_generation_(std::move(active_generation)), retries_(std::move(retries)) {}

  std::optional<head::HeadCommitPosition> applied_position_;
  std::shared_ptr<const GenerationSet> sealed_generations_;
  head::HeadSnapshot active_generation_;
  std::shared_ptr<const RetryTable> retries_;
};

struct TabletStateCoreConfig {
  schema::SchemaLineage lineage;
  std::vector<RegisteredSchema> schemas;
  schema::TabletId tablet_id;
  TabletStateConfig limits;
  std::unique_ptr<head::MutableHead> active_head;
  std::shared_ptr<const TabletPublication> initial_publication;
  void (*publication_hook)(void*) noexcept;
  void* publication_hook_context;
};

class TabletStateCore : public std::enable_shared_from_this<TabletStateCore> {
public:
  explicit TabletStateCore(TabletStateCoreConfig config) noexcept
      : lineage_(std::move(config.lineage)), schemas_(std::move(config.schemas)),
        tablet_id_(config.tablet_id), limits_(std::move(config.limits)),
        active_head_(std::move(config.active_head)),
        publication_(std::move(config.initial_publication)),
        publication_hook_(config.publication_hook),
        publication_hook_context_(config.publication_hook_context) {}

  [[nodiscard]] common::Result<PreparedTabletAppend>
  prepare(const RetryIdentity& retry_identity, const ColumnarAppendMutationIdentity& mutation,
          std::shared_ptr<const columnar::OwnedColumnarBatch> batch);
  [[nodiscard]] common::Status register_schema(std::shared_ptr<const schema::TableSchema> successor,
                                               head::MutableHeadCapacity capacity);
  [[nodiscard]] bool wal_started(std::uint64_t token) const noexcept;
  [[nodiscard]] common::Status mark_wal_started(std::uint64_t token,
                                                head::PreparedHeadAppend& head_append);
  [[nodiscard]] common::Result<TabletAppendResult>
  publish(std::uint64_t token, head::PreparedHeadAppend& head_append,
          head::HeadCommitPosition position, const std::shared_ptr<const TabletPublication>& base,
          const std::shared_ptr<TabletPublication>& next_mutable,
          const std::shared_ptr<const TabletPublication>& next, const RetryIdentity& retry_identity,
          const std::shared_ptr<ColumnarAppendRetryOutcome>& outcome_mutable,
          const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome);
  [[nodiscard]] common::Status cancel_before_wal(std::uint64_t token,
                                                 head::PreparedHeadAppend& head_append);
  [[nodiscard]] common::Result<TabletSnapshot>
  advance_recovered_retry(const RetryIdentity& retry_identity,
                          const ColumnarAppendMutationIdentity& mutation,
                          const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome,
                          head::HeadCommitPosition position);
  [[nodiscard]] common::Status seed_recovered_prefix(
      schema::SchemaId recovery_schema_id, schema::SchemaVersion recovery_schema_version,
      head::HeadCommitPosition durable_position, std::span<const RetryIdentity> identities,
      std::span<const std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes);
  [[nodiscard]] common::Result<TabletSnapshot>
  retire_sealed_generation(const SealedGenerationRetirementReceipt& receipt);
  void abandon(std::uint64_t token) noexcept;

  [[nodiscard]] common::Result<TabletSnapshot> snapshot();
  [[nodiscard]] TabletStateMetrics metrics() const noexcept;
  [[nodiscard]] common::Status fail_closed() noexcept;

private:
  [[nodiscard]] static common::Status validate_position(const head::HeadCommitPosition& position,
                                                        const TabletPublication& base);
  [[nodiscard]] common::Result<std::size_t>
  validate_request(const ColumnarAppendMutationIdentity& mutation,
                   const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch) const;
  [[nodiscard]] std::optional<std::size_t>
  find_schema_index(const schema::TableSchema& candidate) const noexcept;

  schema::SchemaLineage lineage_;
  std::vector<RegisteredSchema> schemas_;
  std::size_t active_schema_index_{0U};
  schema::TabletId tablet_id_;
  TabletStateConfig limits_;
  std::unique_ptr<head::MutableHead> active_head_;
  std::shared_ptr<const TabletPublication> publication_;
  void (*publication_hook_)(void*) noexcept;
  void* publication_hook_context_;

  bool append_active_{false};
  std::uint64_t active_token_{};
  PreparedPhase active_phase_{PreparedPhase::kPreWal};
  std::uint64_t next_token_{1U};
  std::uint64_t recovery_skip_through_{};
  std::atomic<bool> failed_{false};
};

} // namespace detail

class PreparedTabletAppend::Impl {
public:
  Impl(std::shared_ptr<detail::TabletStateCore> state, const std::uint64_t token,
       head::PreparedHeadAppend head_append, std::shared_ptr<const detail::TabletPublication> base,
       std::shared_ptr<detail::TabletPublication> next_mutable,
       std::shared_ptr<const detail::TabletPublication> next, RetryIdentity retry_identity,
       std::shared_ptr<ColumnarAppendRetryOutcome> outcome_mutable,
       std::shared_ptr<const ColumnarAppendRetryOutcome> outcome) noexcept
      : state_(std::move(state)), token_(token), head_append_(std::move(head_append)),
        base_(std::move(base)), next_mutable_(std::move(next_mutable)), next_(std::move(next)),
        retry_identity_(retry_identity), outcome_mutable_(std::move(outcome_mutable)),
        outcome_(std::move(outcome)) {}

  ~Impl() {
    if (state_ != nullptr) {
      state_->abandon(token_);
    }
  }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  [[nodiscard]] bool wal_started() const noexcept {
    return state_ != nullptr && state_->wal_started(token_);
  }

  [[nodiscard]] common::Status mark_wal_started() {
    if (state_ == nullptr) {
      return invalid("prepared tablet append is invalid");
    }
    return state_->mark_wal_started(token_, head_append_);
  }

  [[nodiscard]] common::Result<TabletAppendResult>
  publish(const head::HeadCommitPosition position) {
    if (state_ == nullptr || !head_append_.is_valid() || base_ == nullptr ||
        next_mutable_ == nullptr || next_ == nullptr || outcome_mutable_ == nullptr ||
        outcome_ == nullptr) {
      return common::make_unexpected(invalid("prepared tablet append is invalid"));
    }
    common::Result<TabletAppendResult> published =
        state_->publish(token_, head_append_, position, base_, next_mutable_, next_,
                        retry_identity_, outcome_mutable_, outcome_);
    if (published.has_value()) {
      state_.reset();
      base_.reset();
      next_mutable_.reset();
      next_.reset();
      outcome_mutable_.reset();
      outcome_.reset();
    }
    return published;
  }

  [[nodiscard]] common::Status cancel_before_wal() {
    if (state_ == nullptr) {
      return invalid("prepared tablet append is invalid");
    }
    const common::Status status = state_->cancel_before_wal(token_, head_append_);
    if (status.is_ok()) {
      state_.reset();
      base_.reset();
      next_mutable_.reset();
      next_.reset();
      outcome_mutable_.reset();
      outcome_.reset();
    }
    return status;
  }

private:
  std::shared_ptr<detail::TabletStateCore> state_;
  std::uint64_t token_;
  head::PreparedHeadAppend head_append_;
  std::shared_ptr<const detail::TabletPublication> base_;
  std::shared_ptr<detail::TabletPublication> next_mutable_;
  std::shared_ptr<const detail::TabletPublication> next_;
  RetryIdentity retry_identity_;
  std::shared_ptr<ColumnarAppendRetryOutcome> outcome_mutable_;
  std::shared_ptr<const ColumnarAppendRetryOutcome> outcome_;
};

std::optional<std::size_t>
detail::TabletStateCore::find_schema_index(const schema::TableSchema& candidate) const noexcept {
  for (std::size_t index = 0U; index < schemas_.size(); ++index) {
    if (*schemas_[index].schema == candidate) {
      return index;
    }
  }
  return std::nullopt;
}

common::Result<std::size_t> detail::TabletStateCore::validate_request(
    const ColumnarAppendMutationIdentity& mutation,
    const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch) const {
  if (failed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(
        unavailable("tablet state is failed and requires fresh recovery"));
  }
  if (append_active_) {
    return common::make_unexpected(unavailable("tablet state already has a prepared append"));
  }
  if (batch == nullptr) {
    return common::make_unexpected(invalid("tablet append requires an owning batch pointer"));
  }
  if (batch->row_count() == 0U) {
    return common::make_unexpected(invalid("tablet append requires at least one row"));
  }
  if (mutation.table_id != schemas_.front().schema->table_id() ||
      mutation.tablet_id != tablet_id_) {
    return common::make_unexpected(
        invalid("tablet append mutation identity does not match the bound tablet"));
  }
  const std::optional<std::size_t> schema_index = find_schema_index(batch->schema());
  if (!schema_index.has_value()) {
    return common::make_unexpected(
        invalid("tablet append batch schema is not registered for this tablet"));
  }
  if (*schema_index < active_schema_index_) {
    return common::make_unexpected(
        invalid("new tablet append cannot return to an ancestor schema"));
  }
  return *schema_index;
}

common::Status
detail::TabletStateCore::register_schema(std::shared_ptr<const schema::TableSchema> successor,
                                         head::MutableHeadCapacity capacity) {
  if (failed_.load(std::memory_order_acquire)) {
    return unavailable("tablet state cannot register a schema after failure");
  }
  if (append_active_) {
    return unavailable("tablet state cannot register a schema during a prepared append");
  }
  if (successor == nullptr) {
    return invalid("tablet schema registration requires an owning schema pointer");
  }
  if (schemas_.size() >= limits_.maximum_schema_versions) {
    return exhausted("tablet schema registry reached its configured version bound");
  }

  try {
    schema::SchemaLineage next_lineage = lineage_;
    common::Status lineage_status = next_lineage.append(*successor);
    if (!lineage_status.is_ok()) {
      return lineage_status;
    }
    schemas_.push_back(
        RegisteredSchema{.schema = std::move(successor), .capacity = std::move(capacity)});
    // The non-throwing assignment commits the authoritative validation state only after the
    // caller-owned schema and capacity have been stored successfully.
    lineage_ = std::move(next_lineage);
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("tablet schema registration allocation failed");
  } catch (const std::length_error&) {
    return exhausted("tablet schema registration exceeds container limits");
  }
}

common::Status detail::TabletStateCore::validate_position(const head::HeadCommitPosition& position,
                                                          const TabletPublication& base) {
  if (!position.is_valid()) {
    return invalid("tablet append requires a valid commit-log identity and position");
  }
  if (base.applied_position_.has_value()) {
    const head::HeadCommitPosition& applied = *base.applied_position_;
    if (!position.same_log(applied) || position.record_sequence <= applied.record_sequence) {
      return invalid("tablet append position must advance within one commit-log history");
    }
  }
  return common::Status::ok();
}

common::Result<PreparedTabletAppend>
detail::TabletStateCore::prepare(const RetryIdentity& retry_identity,
                                 const ColumnarAppendMutationIdentity& mutation,
                                 std::shared_ptr<const columnar::OwnedColumnarBatch> batch) {
  const common::Result<std::size_t> requested_schema_index = validate_request(mutation, batch);
  if (!requested_schema_index.has_value()) {
    return common::make_unexpected(requested_schema_index.error());
  }
  if (next_token_ == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(exhausted("tablet state exhausted its append token space"));
  }
  const std::uint32_t row_count = batch->row_count();

  const std::shared_ptr<const TabletPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  const auto existing = current->retries_->find(retry_identity);
  if (existing != current->retries_->end()) {
    if (existing->second->mutation == mutation) {
      return common::make_unexpected(
          already_exists("tablet retry identity already has a matching published outcome"));
    }
    return common::make_unexpected(
        invalid("tablet retry identity conflicts with a published mutation"));
  }
  if (current->retries_->size() >= limits_.maximum_retry_entries) {
    return common::make_unexpected(
        exhausted("tablet retry table reached its configured entry bound"));
  }

  try {
    common::Status deduplication = validate_append_deduplication(
        *batch, *current->sealed_generations_, current->active_generation_);
    if (!deduplication.is_ok()) {
      return common::make_unexpected(std::move(deduplication));
    }
    std::unique_ptr<head::MutableHead> rotated_head;
    std::shared_ptr<const TabletPublication> base = current;
    std::shared_ptr<TabletPublication> topology_mutable;
    std::shared_ptr<const TabletPublication> topology;
    std::optional<detail::SealedHeadFlushReservation> flush_reservation;

    const bool schema_switch = *requested_schema_index != active_schema_index_;
    bool rotate = schema_switch;
    if (!schema_switch) {
      const common::Status active_fit = active_head_->check_append(*batch);
      if (!active_fit.is_ok()) {
        if (active_fit.code() != common::StatusCode::kResourceExhausted) {
          return common::make_unexpected(active_fit);
        }
        if (current->active_generation_.row_count() == 0U) {
          return common::make_unexpected(
              exhausted("tablet append does not fit an empty mutable-head generation"));
        }
        rotate = true;
      }
    }
    if (rotate) {
      const bool retain_active = current->active_generation_.row_count() != 0U;
      if (retain_active &&
          current->sealed_generations_->size() >= limits_.maximum_sealed_generations) {
        return common::make_unexpected(
            exhausted("tablet sealed-generation retention bound prevents rotation"));
      }
      if (current->active_generation_.generation() == std::numeric_limits<std::uint64_t>::max()) {
        return common::make_unexpected(
            exhausted("tablet exhausted its mutable-head generation number space"));
      }
      if (retain_active && limits_.flush_queue != nullptr) {
        common::Result<detail::SealedHeadFlushReservation> reserved =
            limits_.flush_queue->reserve();
        if (!reserved.has_value()) {
          return common::make_unexpected(reserved.error());
        }
        flush_reservation.emplace(std::move(*reserved));
      }
      const RegisteredSchema& destination_schema = schemas_[*requested_schema_index];
      auto created = head::MutableHead::create(destination_schema.schema, tablet_id_,
                                               current->active_generation_.generation() + 1U,
                                               destination_schema.capacity);
      if (!created.has_value()) {
        return common::make_unexpected(created.error());
      }
      rotated_head = std::make_unique<head::MutableHead>(std::move(*created));
      const common::Status empty_fit = rotated_head->check_append(*batch);
      if (!empty_fit.is_ok()) {
        if (empty_fit.code() == common::StatusCode::kResourceExhausted) {
          return common::make_unexpected(
              exhausted("tablet append does not fit an empty mutable-head generation"));
        }
        return common::make_unexpected(empty_fit);
      }

      auto sealed = std::make_shared<GenerationSet>(*current->sealed_generations_);
      if (retain_active) {
        sealed->push_back(current->active_generation_);
      }
      std::shared_ptr<const GenerationSet> sealed_const = sealed;
      common::Result<head::HeadSnapshot> rotated_snapshot = rotated_head->snapshot();
      if (!rotated_snapshot.has_value()) {
        return common::make_unexpected(rotated_snapshot.error());
      }
      topology_mutable =
          std::make_shared<TabletPublication>(current->applied_position_, std::move(sealed_const),
                                              std::move(*rotated_snapshot), current->retries_);
      topology = topology_mutable;
      base = topology;
    }

    head::MutableHead* const destination =
        rotated_head == nullptr ? active_head_.get() : rotated_head.get();
    auto head_prepared = destination->prepare_append(std::move(batch));
    if (!head_prepared.has_value()) {
      return common::make_unexpected(head_prepared.error());
    }

    auto outcome_mutable = std::make_shared<ColumnarAppendRetryOutcome>(
        ColumnarAppendRetryOutcome{.mutation = mutation,
                                   .commit_source = head::CommitSource::kWal,
                                   .wal_id = wal::WalId{},
                                   .record_sequence = 0U,
                                   .applied_row_count = row_count});
    std::shared_ptr<const ColumnarAppendRetryOutcome> outcome = outcome_mutable;
    auto retries = std::make_shared<RetryTable>(*base->retries_);
    const auto [entry, inserted] = retries->emplace(retry_identity, outcome);
    static_cast<void>(entry);
    if (!inserted) {
      return common::make_unexpected(internal("tablet retry preparation inserted a duplicate"));
    }
    std::shared_ptr<const RetryTable> retries_const = retries;
    auto next_mutable =
        std::make_shared<TabletPublication>(base->applied_position_, base->sealed_generations_,
                                            base->active_generation_, std::move(retries_const));
    std::shared_ptr<const TabletPublication> next = next_mutable;

    std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
    if (self == nullptr) {
      return common::make_unexpected(internal("tablet state lost its owning reference"));
    }
    const std::uint64_t token = next_token_;
    auto implementation = std::make_unique<PreparedTabletAppend::Impl>(
        std::move(self), token, std::move(*head_prepared), base, std::move(next_mutable),
        std::move(next), retry_identity, std::move(outcome_mutable), std::move(outcome));

    if (rotated_head != nullptr) {
      common::Result<head::HeadSnapshot> sealed_snapshot = active_head_->seal();
      if (!sealed_snapshot.has_value()) {
        failed_.store(true, std::memory_order_release);
        return common::make_unexpected(sealed_snapshot.error());
      }
      if (sealed_snapshot->generation() != current->active_generation_.generation() ||
          sealed_snapshot->row_count() != current->active_generation_.row_count() ||
          sealed_snapshot->applied_position() != current->active_generation_.applied_position()) {
        failed_.store(true, std::memory_order_release);
        return common::make_unexpected(
            internal("tablet rotation sealed an unexpected generation boundary"));
      }
      if (flush_reservation.has_value()) {
        flush_reservation->stage(std::move(*sealed_snapshot));
      }
      std::atomic_store_explicit(&publication_, topology, std::memory_order_release);
      active_head_ = std::move(rotated_head);
      active_schema_index_ = *requested_schema_index;
      if (flush_reservation.has_value()) {
        flush_reservation->publish();
      }
    }

    ++next_token_;
    append_active_ = true;
    active_token_ = token;
    active_phase_ = PreparedPhase::kPreWal;
    return PreparedTabletAppend{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("tablet append preparation could not allocate bounded publication state"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tablet append preparation exceeds container limits"));
  }
}

bool detail::TabletStateCore::wal_started(const std::uint64_t token) const noexcept {
  return append_active_ && active_token_ == token && active_phase_ == PreparedPhase::kWalStarted;
}

common::Status detail::TabletStateCore::mark_wal_started(const std::uint64_t token,
                                                         head::PreparedHeadAppend& head_append) {
  if (!append_active_ || active_token_ != token) {
    return internal("prepared tablet append ownership was lost");
  }
  if (active_phase_ != PreparedPhase::kPreWal) {
    return invalid("prepared tablet append has already crossed its pre-WAL boundary");
  }
  if (failed_.load(std::memory_order_acquire)) {
    return unavailable("tablet state cannot start WAL work after failure");
  }
  common::Status head_status = head_append.mark_wal_started();
  if (!head_status.is_ok()) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return head_status;
  }
  active_phase_ = PreparedPhase::kWalStarted;
  return common::Status::ok();
}

common::Result<TabletAppendResult> detail::TabletStateCore::publish(
    const std::uint64_t token, head::PreparedHeadAppend& head_append,
    const head::HeadCommitPosition position, const std::shared_ptr<const TabletPublication>& base,
    const std::shared_ptr<TabletPublication>& next_mutable,
    const std::shared_ptr<const TabletPublication>& next, const RetryIdentity& retry_identity,
    const std::shared_ptr<ColumnarAppendRetryOutcome>& outcome_mutable,
    const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome) {
  if (!append_active_ || active_token_ != token) {
    return common::make_unexpected(internal("prepared tablet append ownership was lost"));
  }
  if (active_phase_ != PreparedPhase::kWalStarted) {
    return common::make_unexpected(
        invalid("tablet append cannot publish before WAL submission begins"));
  }
  if (failed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(unavailable("tablet state cannot publish after failure"));
  }
  if (std::atomic_load_explicit(&publication_, std::memory_order_acquire) != base) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(internal("tablet publication base changed unexpectedly"));
  }
  const common::Status position_status = validate_position(position, *base);
  if (!position_status.is_ok()) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(position_status);
  }
  const auto retry = next->retries_->find(retry_identity);
  if (next.get() != next_mutable.get() || outcome.get() != outcome_mutable.get() ||
      retry == next->retries_->end() || retry->second != outcome ||
      outcome_mutable->record_sequence != 0U || outcome_mutable->wal_id.is_valid() ||
      !outcome_mutable->raft_group_id.is_nil()) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(internal("tablet prepared publication state is inconsistent"));
  }

  std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
  if (self == nullptr) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(internal("tablet state lost its owning reference"));
  }
  common::Result<head::HeadSnapshot> head_snapshot = head_append.publish(position);
  if (!head_snapshot.has_value()) {
    failed_.store(true, std::memory_order_release);
    append_active_ = false;
    return common::make_unexpected(head_snapshot.error());
  }
  if (publication_hook_ != nullptr) {
    publication_hook_(publication_hook_context_);
  }

  outcome_mutable->commit_source = position.source;
  outcome_mutable->wal_id = position.wal_id;
  outcome_mutable->raft_group_id = position.raft_group_id;
  outcome_mutable->record_sequence = position.record_sequence;
  next_mutable->active_generation_ = *head_snapshot;
  next_mutable->applied_position_ = position;
  std::atomic_store_explicit(&publication_, next, std::memory_order_release);
  append_active_ = false;
  return TabletAppendResult{.snapshot = TabletSnapshot{std::move(self), next}, .outcome = outcome};
}

common::Status detail::TabletStateCore::cancel_before_wal(const std::uint64_t token,
                                                          head::PreparedHeadAppend& head_append) {
  if (!append_active_ || active_token_ != token) {
    return internal("prepared tablet append ownership was lost");
  }
  if (active_phase_ != PreparedPhase::kPreWal) {
    return invalid("prepared tablet append cannot cancel after WAL submission begins");
  }
  common::Status head_status = head_append.cancel_before_wal();
  if (!head_status.is_ok()) {
    return head_status;
  }
  append_active_ = false;
  return common::Status::ok();
}

common::Result<TabletSnapshot> detail::TabletStateCore::advance_recovered_retry(
    const RetryIdentity& retry_identity, const ColumnarAppendMutationIdentity& mutation,
    const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome,
    const head::HeadCommitPosition position) {
  if (failed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(
        unavailable("tablet state cannot advance recovered retry after failure"));
  }
  if (append_active_) {
    return common::make_unexpected(
        unavailable("tablet state cannot advance recovered retry during a prepared append"));
  }
  if (mutation.table_id != schemas_.front().schema->table_id() ||
      mutation.tablet_id != tablet_id_ || outcome == nullptr || outcome->mutation != mutation) {
    return common::make_unexpected(
        invalid("recovered retry does not match the bound tablet mutation"));
  }
  const std::shared_ptr<const TabletPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  const auto existing = current->retries_->find(retry_identity);
  if (existing == current->retries_->end() || existing->second != outcome) {
    return common::make_unexpected(
        invalid("recovered retry outcome is not the tablet's exact published object"));
  }
  const std::optional<head::HeadCommitPosition> applied_position = current->applied_position_;
  if (position.source == head::CommitSource::kWal && position.wal_id.is_valid() &&
      applied_position.has_value() && recovery_skip_through_ != 0U &&
      position.same_log(*applied_position) &&
      position.record_sequence >= outcome->record_sequence &&
      position.record_sequence <= recovery_skip_through_) {
    std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
    if (self == nullptr) {
      return common::make_unexpected(internal("tablet state lost its owning reference"));
    }
    return TabletSnapshot{std::move(self), current};
  }
  const common::Status position_status = validate_position(position, *current);
  if (!position_status.is_ok()) {
    return common::make_unexpected(position_status);
  }

  try {
    std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
    if (self == nullptr) {
      return common::make_unexpected(internal("tablet state lost its owning reference"));
    }
    auto next = std::make_shared<const TabletPublication>(
        position, current->sealed_generations_, current->active_generation_, current->retries_);
    std::atomic_store_explicit(&publication_, next, std::memory_order_release);
    return TabletSnapshot{std::move(self), std::move(next)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("recovered retry position publication could not allocate its outer epoch"));
  }
}

common::Status detail::TabletStateCore::seed_recovered_prefix(
    const schema::SchemaId recovery_schema_id, const schema::SchemaVersion recovery_schema_version,
    const head::HeadCommitPosition durable_position,
    const std::span<const RetryIdentity> identities,
    const std::span<const std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes) {
  if (failed_.load(std::memory_order_acquire) || append_active_) {
    return unavailable("tablet durable prefix requires fresh unpublished state");
  }
  if (!durable_position.wal_id.is_valid() || durable_position.record_sequence == 0U) {
    return invalid("tablet durable prefix requires a nonzero WAL boundary");
  }
  if (identities.size() != outcomes.size()) {
    return invalid("tablet durable prefix retry identities and outcomes disagree in count");
  }
  if (identities.size() > limits_.maximum_retry_entries) {
    return exhausted("tablet durable prefix exceeds the configured retry-entry bound");
  }

  const std::shared_ptr<const TabletPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  if (current->applied_position_.has_value() || !current->retries_->empty() ||
      !current->sealed_generations_->empty() || current->active_generation_.row_count() != 0U) {
    return invalid("tablet durable prefix can be restored only once into empty state");
  }

  try {
    const auto recovered_schema =
        std::find_if(schemas_.begin(), schemas_.end(), [&](const RegisteredSchema& retained) {
          return retained.schema->schema_id() == recovery_schema_id &&
                 retained.schema->version() == recovery_schema_version;
        });
    if (recovered_schema == schemas_.end()) {
      return invalid("tablet durable recovery schema is absent from its registered lineage");
    }
    const std::size_t recovered_schema_index =
        static_cast<std::size_t>(std::distance(schemas_.begin(), recovered_schema));
    auto recovered_head = head::MutableHead::create(recovered_schema->schema, tablet_id_, 1U,
                                                    recovered_schema->capacity);
    if (!recovered_head.has_value()) {
      return recovered_head.error();
    }
    auto active_head = std::make_unique<head::MutableHead>(std::move(*recovered_head));
    common::Result<head::HeadSnapshot> active_snapshot = active_head->snapshot();
    if (!active_snapshot.has_value()) {
      return active_snapshot.error();
    }
    auto retries = std::make_shared<RetryTable>();
    for (std::size_t index = 0U; index < identities.size(); ++index) {
      const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome = outcomes[index];
      if (outcome == nullptr || outcome->mutation.table_id != schemas_.front().schema->table_id() ||
          outcome->mutation.tablet_id != tablet_id_ ||
          outcome->commit_source != head::CommitSource::kWal ||
          outcome->wal_id != durable_position.wal_id || !outcome->raft_group_id.is_nil() ||
          outcome->record_sequence == 0U ||
          outcome->record_sequence > durable_position.record_sequence ||
          outcome->applied_row_count == 0U) {
        return invalid("tablet durable prefix contains an invalid retry outcome");
      }
      const auto [entry, inserted] = retries->emplace(identities[index], outcome);
      static_cast<void>(entry);
      if (!inserted) {
        return invalid("tablet durable prefix repeats a retry identity");
      }
    }
    std::shared_ptr<const RetryTable> retries_const = retries;
    auto next = std::make_shared<const TabletPublication>(
        durable_position, current->sealed_generations_, std::move(*active_snapshot),
        std::move(retries_const));
    std::atomic_store_explicit(&publication_, next, std::memory_order_release);
    active_head_ = std::move(active_head);
    active_schema_index_ = recovered_schema_index;
    recovery_skip_through_ = durable_position.record_sequence;
    return common::Status::ok();
  } catch (const std::bad_alloc&) {
    return exhausted("tablet durable prefix allocation failed");
  } catch (const std::length_error&) {
    return exhausted("tablet durable prefix exceeds container limits");
  }
}

common::Result<TabletSnapshot> detail::TabletStateCore::retire_sealed_generation(
    const SealedGenerationRetirementReceipt& receipt) {
  if (failed_.load(std::memory_order_acquire)) {
    return common::make_unexpected(
        unavailable("tablet state cannot retire a sealed generation after failure"));
  }
  if (append_active_) {
    return common::make_unexpected(
        unavailable("tablet state cannot retire a sealed generation during a prepared append"));
  }
  if (receipt.table_id() != schemas_.front().schema->table_id() ||
      receipt.tablet_id() != tablet_id_) {
    return common::make_unexpected(
        invalid("sealed-generation retirement receipt names a different tablet"));
  }

  const std::shared_ptr<const TabletPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  const auto found =
      std::ranges::find_if(*current->sealed_generations_, [&](const head::HeadSnapshot& candidate) {
        return candidate.generation() == receipt.head_generation();
      });
  std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
  if (self == nullptr) {
    return common::make_unexpected(internal("tablet state lost its owning reference"));
  }
  if (found == current->sealed_generations_->end()) {
    if (receipt.head_generation() >= current->active_generation_.generation()) {
      return common::make_unexpected(
          invalid("sealed-generation retirement receipt does not name a past generation"));
    }
    return TabletSnapshot{std::move(self), current};
  }
  if (found->schema_ptr()->schema_id() != receipt.schema_id() ||
      found->schema_ptr()->version() != receipt.schema_version() ||
      found->row_count() != receipt.row_count() || !found->is_sealed() ||
      receipt.minimum_record_sequence() == 0U ||
      receipt.maximum_record_sequence() < receipt.minimum_record_sequence()) {
    return common::make_unexpected(
        invalid("sealed-generation retirement receipt disagrees with the retained head"));
  }
  std::uint64_t minimum_sequence = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t maximum_sequence = 0U;
  for (std::uint32_t row = 0U; row < found->row_count(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = found->row_metadata(row);
    if (!metadata.has_value()) {
      return common::make_unexpected(metadata.error());
    }
    if (metadata->commit_position.wal_id != receipt.wal_id()) {
      return common::make_unexpected(
          invalid("sealed-generation retirement receipt belongs to a different WAL history"));
    }
    minimum_sequence = std::min(minimum_sequence, metadata->commit_position.record_sequence);
    maximum_sequence = std::max(maximum_sequence, metadata->commit_position.record_sequence);
  }
  if (minimum_sequence != receipt.minimum_record_sequence() ||
      maximum_sequence != receipt.maximum_record_sequence()) {
    return common::make_unexpected(
        invalid("sealed-generation retirement receipt record bounds disagree with the head"));
  }

  try {
    auto sealed = std::make_shared<GenerationSet>(*current->sealed_generations_);
    const std::size_t index =
        static_cast<std::size_t>(std::distance(current->sealed_generations_->begin(), found));
    sealed->erase(sealed->begin() + static_cast<std::ptrdiff_t>(index));
    std::shared_ptr<const GenerationSet> sealed_const = std::move(sealed);
    auto next = std::make_shared<const TabletPublication>(
        current->applied_position_, std::move(sealed_const), current->active_generation_,
        current->retries_);
    if (publication_hook_ != nullptr) {
      publication_hook_(publication_hook_context_);
    }
    std::atomic_store_explicit(&publication_, next, std::memory_order_release);
    return TabletSnapshot{std::move(self), std::move(next)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("sealed-generation retirement publication could not allocate its outer epoch"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("sealed-generation retirement publication exceeds container limits"));
  }
}

void detail::TabletStateCore::abandon(const std::uint64_t token) noexcept {
  if (!append_active_ || active_token_ != token) {
    return;
  }
  if (active_phase_ == PreparedPhase::kWalStarted) {
    failed_.store(true, std::memory_order_release);
  }
  append_active_ = false;
}

common::Result<TabletSnapshot> detail::TabletStateCore::snapshot() {
  std::shared_ptr<TabletStateCore> self = weak_from_this().lock();
  if (self == nullptr) {
    return common::make_unexpected(internal("tablet state lost its owning reference"));
  }
  return TabletSnapshot{std::move(self),
                        std::atomic_load_explicit(&publication_, std::memory_order_acquire)};
}

TabletStateMetrics detail::TabletStateCore::metrics() const noexcept {
  const std::shared_ptr<const TabletPublication> current =
      std::atomic_load_explicit(&publication_, std::memory_order_acquire);
  std::size_t visible_rows = current->active_generation_.row_count();
  for (const head::HeadSnapshot& sealed : *current->sealed_generations_) {
    visible_rows += sealed.row_count();
  }
  return TabletStateMetrics{.maximum_schema_versions = limits_.maximum_schema_versions,
                            .maximum_sealed_generations = limits_.maximum_sealed_generations,
                            .sealed_generations = current->sealed_generations_->size(),
                            .maximum_retry_entries = limits_.maximum_retry_entries,
                            .retry_entries = current->retries_->size(),
                            .active_schema_version =
                                current->active_generation_.schema_ptr()->version().value(),
                            .active_generation = current->active_generation_.generation(),
                            .active_rows = current->active_generation_.row_count(),
                            .visible_rows = visible_rows,
                            .failed = failed_.load(std::memory_order_acquire)};
}

common::Status detail::TabletStateCore::fail_closed() noexcept {
  failed_.store(true, std::memory_order_release);
  return common::Status::ok();
}

TabletSnapshot::TabletSnapshot(
    std::shared_ptr<detail::TabletStateCore> state,
    std::shared_ptr<const detail::TabletPublication> publication) noexcept
    : state_(std::move(state)), publication_(std::move(publication)) {}

SealedGenerationRetirementReceipt::SealedGenerationRetirementReceipt(Fields fields) noexcept
    : table_id_(fields.table_id), tablet_id_(fields.tablet_id), schema_id_(fields.schema_id),
      schema_version_(fields.schema_version), head_generation_(fields.head_generation),
      row_count_(fields.row_count), wal_id_(fields.wal_id),
      minimum_record_sequence_(fields.minimum_record_sequence),
      maximum_record_sequence_(fields.maximum_record_sequence) {}

const schema::TableId& SealedGenerationRetirementReceipt::table_id() const noexcept {
  return table_id_;
}

const schema::TabletId& SealedGenerationRetirementReceipt::tablet_id() const noexcept {
  return tablet_id_;
}

const schema::SchemaId& SealedGenerationRetirementReceipt::schema_id() const noexcept {
  return schema_id_;
}

schema::SchemaVersion SealedGenerationRetirementReceipt::schema_version() const noexcept {
  return schema_version_;
}

std::uint64_t SealedGenerationRetirementReceipt::head_generation() const noexcept {
  return head_generation_;
}

std::uint32_t SealedGenerationRetirementReceipt::row_count() const noexcept {
  return row_count_;
}

const wal::WalId& SealedGenerationRetirementReceipt::wal_id() const noexcept {
  return wal_id_;
}

std::uint64_t SealedGenerationRetirementReceipt::minimum_record_sequence() const noexcept {
  return minimum_record_sequence_;
}

std::uint64_t SealedGenerationRetirementReceipt::maximum_record_sequence() const noexcept {
  return maximum_record_sequence_;
}

const schema::TableId& TabletSnapshot::table_id() const noexcept {
  return publication_->active_generation_.table_id();
}

const schema::TabletId& TabletSnapshot::tablet_id() const noexcept {
  return publication_->active_generation_.tablet_id();
}

const std::shared_ptr<const schema::TableSchema>& TabletSnapshot::schema_ptr() const noexcept {
  return publication_->active_generation_.schema_ptr();
}

const std::optional<head::HeadCommitPosition>& TabletSnapshot::applied_position() const noexcept {
  return publication_->applied_position_;
}

const head::HeadSnapshot& TabletSnapshot::active_generation() const noexcept {
  return publication_->active_generation_;
}

std::span<const head::HeadSnapshot> TabletSnapshot::sealed_generations() const noexcept {
  return *publication_->sealed_generations_;
}

std::size_t TabletSnapshot::visible_row_count() const noexcept {
  std::size_t rows = publication_->active_generation_.row_count();
  for (const head::HeadSnapshot& sealed : *publication_->sealed_generations_) {
    rows += sealed.row_count();
  }
  return rows;
}

std::size_t TabletSnapshot::retry_entry_count() const noexcept {
  return publication_->retries_->size();
}

std::shared_ptr<const ColumnarAppendRetryOutcome>
TabletSnapshot::retry_outcome(const RetryIdentity& identity) const noexcept {
  const auto found = publication_->retries_->find(identity);
  return found == publication_->retries_->end() ? nullptr : found->second;
}

PreparedTabletAppend::PreparedTabletAppend() noexcept = default;
PreparedTabletAppend::~PreparedTabletAppend() = default;
PreparedTabletAppend::PreparedTabletAppend(PreparedTabletAppend&&) noexcept = default;
PreparedTabletAppend& PreparedTabletAppend::operator=(PreparedTabletAppend&&) noexcept = default;

PreparedTabletAppend::PreparedTabletAppend(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

bool PreparedTabletAppend::is_valid() const noexcept {
  return implementation_ != nullptr;
}

bool PreparedTabletAppend::wal_started() const {
  return implementation_ != nullptr && implementation_->wal_started();
}

common::Status PreparedTabletAppend::mark_wal_started() {
  if (implementation_ == nullptr) {
    return invalid("prepared tablet append is invalid");
  }
  return implementation_->mark_wal_started();
}

common::Result<TabletAppendResult>
PreparedTabletAppend::publish(const head::HeadCommitPosition position) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid("prepared tablet append is invalid"));
  }
  common::Result<TabletAppendResult> published = implementation_->publish(position);
  if (published.has_value()) {
    implementation_.reset();
  }
  return published;
}

common::Status PreparedTabletAppend::cancel_before_wal() {
  if (implementation_ == nullptr) {
    return invalid("prepared tablet append is invalid");
  }
  const common::Status status = implementation_->cancel_before_wal();
  if (status.is_ok()) {
    implementation_.reset();
  }
  return status;
}

TabletState::TabletState(std::shared_ptr<detail::TabletStateCore> state) noexcept
    : state_(std::move(state)) {}
TabletState::~TabletState() = default;
TabletState::TabletState(TabletState&&) noexcept = default;
TabletState& TabletState::operator=(TabletState&&) noexcept = default;

common::Result<TabletState> TabletState::create(std::shared_ptr<const schema::TableSchema> schema,
                                                const schema::TabletId tablet_id,
                                                TabletStateConfig config) {
  return create_with_publication_hook(std::move(schema), tablet_id, std::move(config), nullptr,
                                      nullptr);
}

common::Result<TabletState> TabletState::create_with_publication_hook(
    std::shared_ptr<const schema::TableSchema> schema, const schema::TabletId tablet_id,
    TabletStateConfig config, const PublicationHook hook, void* const hook_context) {
  if (schema == nullptr) {
    return common::make_unexpected(invalid("tablet state requires an owning schema pointer"));
  }
  if (config.maximum_schema_versions == 0U || config.maximum_sealed_generations == 0U ||
      config.maximum_retry_entries == 0U) {
    return common::make_unexpected(
        invalid("tablet schema, sealed-generation, and retry-entry bounds must be nonzero"));
  }

  auto created_head = head::MutableHead::create(schema, tablet_id, 1U, config.head_capacity);
  if (!created_head.has_value()) {
    return common::make_unexpected(created_head.error());
  }
  try {
    common::Result<schema::SchemaLineage> lineage = schema::SchemaLineage::create(*schema);
    if (!lineage.has_value()) {
      return common::make_unexpected(lineage.error());
    }
    auto active_head = std::make_unique<head::MutableHead>(std::move(*created_head));
    auto sealed = std::make_shared<const GenerationSet>();
    auto retries = std::make_shared<const RetryTable>();
    common::Result<head::HeadSnapshot> active_snapshot = active_head->snapshot();
    if (!active_snapshot.has_value()) {
      return common::make_unexpected(active_snapshot.error());
    }
    auto initial = std::make_shared<const detail::TabletPublication>(
        std::nullopt, std::move(sealed), std::move(*active_snapshot), std::move(retries));
    std::vector<RegisteredSchema> schemas;
    schemas.push_back(
        RegisteredSchema{.schema = std::move(schema), .capacity = config.head_capacity});
    auto state = std::make_shared<detail::TabletStateCore>(
        detail::TabletStateCoreConfig{.lineage = std::move(*lineage),
                                      .schemas = std::move(schemas),
                                      .tablet_id = tablet_id,
                                      .limits = std::move(config),
                                      .active_head = std::move(active_head),
                                      .initial_publication = std::move(initial),
                                      .publication_hook = hook,
                                      .publication_hook_context = hook_context});
    return TabletState{std::move(state)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tablet state allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("tablet state configuration exceeds container limits"));
  }
}

common::Status TabletState::register_schema(std::shared_ptr<const schema::TableSchema> successor,
                                            head::MutableHeadCapacity head_capacity) {
  if (state_ == nullptr) {
    return invalid("tablet state is invalid");
  }
  return state_->register_schema(std::move(successor), std::move(head_capacity));
}

common::Result<PreparedTabletAppend>
TabletState::prepare_append(const RetryIdentity& retry_identity,
                            const ColumnarAppendMutationIdentity& mutation,
                            std::shared_ptr<const columnar::OwnedColumnarBatch> batch) {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("tablet state is invalid"));
  }
  return state_->prepare(retry_identity, mutation, std::move(batch));
}

common::Result<TabletSnapshot> TabletState::advance_recovered_retry(
    const RetryIdentity& retry_identity, const ColumnarAppendMutationIdentity& mutation,
    const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome,
    const head::HeadCommitPosition position) {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("tablet state is invalid"));
  }
  return state_->advance_recovered_retry(retry_identity, mutation, outcome, position);
}

common::Status TabletState::seed_recovered_prefix(
    const schema::SchemaId recovery_schema_id, const schema::SchemaVersion recovery_schema_version,
    const head::HeadCommitPosition durable_position,
    const std::span<const RetryIdentity> identities,
    const std::span<const std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes) {
  if (state_ == nullptr) {
    return invalid("tablet state is invalid");
  }
  return state_->seed_recovered_prefix(recovery_schema_id, recovery_schema_version,
                                       durable_position, identities, outcomes);
}

common::Result<TabletSnapshot> TabletState::snapshot() const {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("tablet state is invalid"));
  }
  return state_->snapshot();
}

common::Result<TabletSnapshot>
TabletState::retire_sealed_generation(const SealedGenerationRetirementReceipt& receipt) {
  if (state_ == nullptr) {
    return common::make_unexpected(unavailable("tablet state was moved from"));
  }
  return state_->retire_sealed_generation(receipt);
}

TabletStateMetrics TabletState::metrics() const {
  if (state_ == nullptr) {
    return {};
  }
  return state_->metrics();
}

common::Status TabletState::fail_closed() {
  if (state_ == nullptr) {
    return invalid("tablet state is invalid");
  }
  return state_->fail_closed();
}

} // namespace chronos::ingest
