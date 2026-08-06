#include "chronos/ingest/tablet_state.hpp"

#include <atomic>
#include <exception>
#include <limits>
#include <map>
#include <new>
#include <stdexcept>
#include <string>
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
  std::shared_ptr<const schema::TableSchema> schema;
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
      : schema_(std::move(config.schema)), tablet_id_(config.tablet_id),
        limits_(std::move(config.limits)), active_head_(std::move(config.active_head)),
        publication_(std::move(config.initial_publication)),
        publication_hook_(config.publication_hook),
        publication_hook_context_(config.publication_hook_context) {}

  [[nodiscard]] common::Result<PreparedTabletAppend>
  prepare(const RetryIdentity& retry_identity, const ColumnarAppendMutationIdentity& mutation,
          std::shared_ptr<const columnar::OwnedColumnarBatch> batch);
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
  void abandon(std::uint64_t token) noexcept;

  [[nodiscard]] common::Result<TabletSnapshot> snapshot();
  [[nodiscard]] TabletStateMetrics metrics() const noexcept;
  [[nodiscard]] common::Status fail_closed() noexcept;

private:
  [[nodiscard]] static common::Status validate_position(const head::HeadCommitPosition& position,
                                                        const TabletPublication& base);
  [[nodiscard]] common::Status
  validate_request(const ColumnarAppendMutationIdentity& mutation,
                   const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch) const;

  std::shared_ptr<const schema::TableSchema> schema_;
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

common::Status detail::TabletStateCore::validate_request(
    const ColumnarAppendMutationIdentity& mutation,
    const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch) const {
  if (failed_.load(std::memory_order_acquire)) {
    return unavailable("tablet state is failed and requires fresh recovery");
  }
  if (append_active_) {
    return unavailable("tablet state already has a prepared append");
  }
  if (batch == nullptr) {
    return invalid("tablet append requires an owning batch pointer");
  }
  if (batch->row_count() == 0U) {
    return invalid("tablet append requires at least one row");
  }
  if (mutation.table_id != schema_->table_id() || mutation.tablet_id != tablet_id_) {
    return invalid("tablet append mutation identity does not match the bound tablet");
  }
  if (batch->schema() != *schema_) {
    return invalid("tablet append batch does not match the bound schema");
  }
  return common::Status::ok();
}

common::Status detail::TabletStateCore::validate_position(const head::HeadCommitPosition& position,
                                                          const TabletPublication& base) {
  if (!position.wal_id.is_valid() || position.record_sequence == 0U) {
    return invalid("tablet append requires a nonzero WAL identity and record sequence");
  }
  if (base.applied_position_.has_value()) {
    const head::HeadCommitPosition& applied = *base.applied_position_;
    if (position.wal_id != applied.wal_id || position.record_sequence <= applied.record_sequence) {
      return invalid("tablet append position must advance within one WAL history");
    }
  }
  return common::Status::ok();
}

common::Result<PreparedTabletAppend>
detail::TabletStateCore::prepare(const RetryIdentity& retry_identity,
                                 const ColumnarAppendMutationIdentity& mutation,
                                 std::shared_ptr<const columnar::OwnedColumnarBatch> batch) {
  const common::Status request_status = validate_request(mutation, batch);
  if (!request_status.is_ok()) {
    return common::make_unexpected(request_status);
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
    std::unique_ptr<head::MutableHead> rotated_head;
    std::shared_ptr<const TabletPublication> base = current;
    std::shared_ptr<TabletPublication> topology_mutable;
    std::shared_ptr<const TabletPublication> topology;

    const common::Status active_fit = active_head_->check_append(*batch);
    if (!active_fit.is_ok()) {
      if (active_fit.code() != common::StatusCode::kResourceExhausted) {
        return common::make_unexpected(active_fit);
      }
      if (current->active_generation_.row_count() == 0U) {
        return common::make_unexpected(
            exhausted("tablet append does not fit an empty mutable-head generation"));
      }
      if (current->sealed_generations_->size() >= limits_.maximum_sealed_generations) {
        return common::make_unexpected(
            exhausted("tablet sealed-generation retention bound prevents rotation"));
      }
      if (current->active_generation_.generation() == std::numeric_limits<std::uint64_t>::max()) {
        return common::make_unexpected(
            exhausted("tablet exhausted its mutable-head generation number space"));
      }

      auto created = head::MutableHead::create(schema_, tablet_id_,
                                               current->active_generation_.generation() + 1U,
                                               limits_.head_capacity);
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
      sealed->push_back(current->active_generation_);
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
      const common::Result<head::HeadSnapshot> sealed_snapshot = active_head_->seal();
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
      std::atomic_store_explicit(&publication_, topology, std::memory_order_release);
      active_head_ = std::move(rotated_head);
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
      outcome_mutable->record_sequence != 0U || outcome_mutable->wal_id.is_valid()) {
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

  outcome_mutable->wal_id = position.wal_id;
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
  return TabletStateMetrics{.maximum_sealed_generations = limits_.maximum_sealed_generations,
                            .sealed_generations = current->sealed_generations_->size(),
                            .maximum_retry_entries = limits_.maximum_retry_entries,
                            .retry_entries = current->retries_->size(),
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
  if (config.maximum_sealed_generations == 0U || config.maximum_retry_entries == 0U) {
    return common::make_unexpected(
        invalid("tablet sealed-generation and retry-entry bounds must be nonzero"));
  }

  auto created_head = head::MutableHead::create(schema, tablet_id, 1U, config.head_capacity);
  if (!created_head.has_value()) {
    return common::make_unexpected(created_head.error());
  }
  try {
    auto active_head = std::make_unique<head::MutableHead>(std::move(*created_head));
    auto sealed = std::make_shared<const GenerationSet>();
    auto retries = std::make_shared<const RetryTable>();
    common::Result<head::HeadSnapshot> active_snapshot = active_head->snapshot();
    if (!active_snapshot.has_value()) {
      return common::make_unexpected(active_snapshot.error());
    }
    auto initial = std::make_shared<const detail::TabletPublication>(
        std::nullopt, std::move(sealed), std::move(*active_snapshot), std::move(retries));
    auto state = std::make_shared<detail::TabletStateCore>(
        detail::TabletStateCoreConfig{.schema = std::move(schema),
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

common::Result<PreparedTabletAppend>
TabletState::prepare_append(const RetryIdentity& retry_identity,
                            const ColumnarAppendMutationIdentity& mutation,
                            std::shared_ptr<const columnar::OwnedColumnarBatch> batch) {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("tablet state is invalid"));
  }
  return state_->prepare(retry_identity, mutation, std::move(batch));
}

common::Result<TabletSnapshot> TabletState::snapshot() const {
  if (state_ == nullptr) {
    return common::make_unexpected(invalid("tablet state is invalid"));
  }
  return state_->snapshot();
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
