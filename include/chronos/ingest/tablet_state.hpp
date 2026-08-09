#ifndef CHRONOS_INGEST_TABLET_STATE_HPP_
#define CHRONOS_INGEST_TABLET_STATE_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>

namespace chronos::manifest::detail {
class DatabaseStoragePublisherImpl;
}

namespace chronos::ingest {

class SealedHeadFlushQueue;

namespace detail {
class ColumnarRecoveryStateBuilder;
class TabletStateTestAccess;
} // namespace detail

// Non-forgeable proof that one exact sealed generation has been removed from a successfully
// release-published database storage epoch and replaced by durable Manifest-selected state.
// Copies may be consumed repeatedly; retirement is idempotent.
class SealedGenerationRetirementReceipt {
public:
  SealedGenerationRetirementReceipt() = delete;
  SealedGenerationRetirementReceipt(const SealedGenerationRetirementReceipt&) noexcept = default;
  SealedGenerationRetirementReceipt&
  operator=(const SealedGenerationRetirementReceipt&) noexcept = default;
  SealedGenerationRetirementReceipt(SealedGenerationRetirementReceipt&&) noexcept = default;
  SealedGenerationRetirementReceipt&
  operator=(SealedGenerationRetirementReceipt&&) noexcept = default;
  ~SealedGenerationRetirementReceipt() = default;

  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const schema::SchemaId& schema_id() const noexcept;
  [[nodiscard]] schema::SchemaVersion schema_version() const noexcept;
  [[nodiscard]] std::uint64_t head_generation() const noexcept;
  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] const wal::WalId& wal_id() const noexcept;
  [[nodiscard]] std::uint64_t minimum_record_sequence() const noexcept;
  [[nodiscard]] std::uint64_t maximum_record_sequence() const noexcept;

private:
  struct Fields {
    schema::TableId table_id;
    schema::TabletId tablet_id;
    schema::SchemaId schema_id;
    schema::SchemaVersion schema_version;
    std::uint64_t head_generation;
    std::uint32_t row_count;
    wal::WalId wal_id;
    std::uint64_t minimum_record_sequence;
    std::uint64_t maximum_record_sequence;
  };

  explicit SealedGenerationRetirementReceipt(Fields fields) noexcept;

  schema::TableId table_id_;
  schema::TabletId tablet_id_;
  schema::SchemaId schema_id_;
  schema::SchemaVersion schema_version_;
  std::uint64_t head_generation_{};
  std::uint32_t row_count_{};
  wal::WalId wal_id_;
  std::uint64_t minimum_record_sequence_{};
  std::uint64_t maximum_record_sequence_{};

  friend class chronos::manifest::detail::DatabaseStoragePublisherImpl;
  friend class detail::TabletStateTestAccess;
};

struct TabletStateConfig {
  head::MutableHeadCapacity head_capacity;

  // Sealed generations remain owned until an authorized post-Manifest-publication retirement.
  // All bounds must be nonzero; reaching one rejects registration or append before WAL.
  std::size_t maximum_schema_versions{1U};
  std::size_t maximum_sealed_generations{};
  std::size_t maximum_retry_entries{};
  // When present, every nonempty rotation reserves this queue before changing tablet topology and
  // publishes the exact sealed pin after the new topology becomes visible.
  std::shared_ptr<SealedHeadFlushQueue> flush_queue;
};

struct TabletStateMetrics {
  std::size_t maximum_schema_versions{};
  std::size_t maximum_sealed_generations{};
  std::size_t sealed_generations{};
  std::size_t maximum_retry_entries{};
  std::size_t retry_entries{};
  std::uint64_t active_schema_version{};
  std::uint64_t active_generation{};
  std::uint32_t active_rows{};
  std::size_t visible_rows{};
  bool failed{false};
};

namespace detail {
class TabletPublication;
class TabletStateCore;
} // namespace detail

// One owning acquire-observed tablet epoch. The active boundary, sealed-generation set, applied
// position, and retry table all come from the same outer publication. Returned generation views
// and retry outcomes remain pinned by this object or by their own shared ownership.
class TabletSnapshot {
public:
  TabletSnapshot() = delete;
  TabletSnapshot(const TabletSnapshot&) noexcept = default;
  TabletSnapshot& operator=(const TabletSnapshot&) noexcept = default;
  TabletSnapshot(TabletSnapshot&&) noexcept = default;
  TabletSnapshot& operator=(TabletSnapshot&&) noexcept = default;
  ~TabletSnapshot() = default;

  [[nodiscard]] const schema::TableId& table_id() const noexcept;
  [[nodiscard]] const schema::TabletId& tablet_id() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] const std::optional<head::HeadCommitPosition>& applied_position() const noexcept;
  [[nodiscard]] const head::HeadSnapshot& active_generation() const noexcept;
  [[nodiscard]] std::span<const head::HeadSnapshot> sealed_generations() const noexcept;
  [[nodiscard]] std::size_t visible_row_count() const noexcept;
  [[nodiscard]] std::size_t retry_entry_count() const noexcept;
  [[nodiscard]] std::shared_ptr<const ColumnarAppendRetryOutcome>
  retry_outcome(const RetryIdentity& identity) const noexcept;

private:
  TabletSnapshot(std::shared_ptr<detail::TabletStateCore> state,
                 std::shared_ptr<const detail::TabletPublication> publication) noexcept;

  std::shared_ptr<detail::TabletStateCore> state_;
  std::shared_ptr<const detail::TabletPublication> publication_;

  friend class detail::TabletStateCore;
};

struct TabletAppendResult {
  TabletSnapshot snapshot;
  // This is the exact immutable object installed in snapshot's retry table. A caller may pass it
  // directly to RetryReservation::commit_published after tablet publication succeeds.
  std::shared_ptr<const ColumnarAppendRetryOutcome> outcome;
};

// Move-only ownership of all state reserved for one tablet append. The shard writer must use it
// serially. A pre-WAL drop cancels; a post-WAL drop fails both tablet and generation closed while
// preserving the previous complete outer publication.
class PreparedTabletAppend {
public:
  PreparedTabletAppend() noexcept;
  ~PreparedTabletAppend();

  PreparedTabletAppend(const PreparedTabletAppend&) = delete;
  PreparedTabletAppend& operator=(const PreparedTabletAppend&) = delete;
  PreparedTabletAppend(PreparedTabletAppend&&) noexcept;
  PreparedTabletAppend& operator=(PreparedTabletAppend&&) noexcept;

  [[nodiscard]] bool is_valid() const noexcept;
  [[nodiscard]] bool wal_started() const;
  [[nodiscard]] common::Status mark_wal_started();
  [[nodiscard]] common::Result<TabletAppendResult> publish(head::HeadCommitPosition position);
  [[nodiscard]] common::Status cancel_before_wal();

private:
  class Impl;
  explicit PreparedTabletAppend(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend class detail::TabletStateCore;
};

// One tablet's bounded in-memory publication owner. Every generation is schema-bound. Exactly one
// shard writer registers accepted direct successors and calls prepare/mark/publish/cancel; readers
// may acquire and scan snapshots concurrently. WAL I/O, global retry reservation, routing, active
// schema admission and replay are external. An optional configured flush queue receives every
// nonempty sealed generation through a capacity reservation made before WAL admission.
class TabletState {
public:
  TabletState() = delete;
  ~TabletState();

  TabletState(const TabletState&) = delete;
  TabletState& operator=(const TabletState&) = delete;
  TabletState(TabletState&&) noexcept;
  TabletState& operator=(TabletState&&) noexcept;

  [[nodiscard]] static common::Result<TabletState>
  create(std::shared_ptr<const schema::TableSchema> schema, schema::TabletId tablet_id,
         TabletStateConfig config);

  // Registers one accepted direct v1 successor and records its empty-generation capacity. This is
  // a bounded pre-WAL catalog handoff: it validates the lineage now but does not activate the
  // schema or publish a tablet epoch. Head-specific capacity validation occurs when the first
  // append prepares that generation, still before WAL admission. That append seals the active
  // ancestor and activates the successor. Registered lineages cannot branch or skip versions.
  [[nodiscard]] common::Status register_schema(std::shared_ptr<const schema::TableSchema> successor,
                                               head::MutableHeadCapacity head_capacity);

  // Validates the tablet/mutation/schema identity, enforces all local bounds, rotates when the
  // whole batch fits only an empty generation, and allocates the complete outer descriptor and
  // retry outcome before returning. It does not perform WAL I/O or publish rows.
  [[nodiscard]] common::Result<PreparedTabletAppend>
  prepare_append(const RetryIdentity& retry_identity,
                 const ColumnarAppendMutationIdentity& mutation,
                 std::shared_ptr<const columnar::OwnedColumnarBatch> batch);

  // Recovery-only position advance for a matching duplicate command. The supplied outcome must be
  // the exact object already present in this tablet's retry table. No rows or retry entries are
  // added; one new outer epoch publishes only the later verified WAL position.
  [[nodiscard]] common::Result<TabletSnapshot>
  advance_recovered_retry(const RetryIdentity& retry_identity,
                          const ColumnarAppendMutationIdentity& mutation,
                          const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome,
                          head::HeadCommitPosition position);

  // Recovery/application-only publication of a later committed position that carries no tablet
  // row or retry mutation (for example a Raft membership entry or snapshot boundary). The current
  // rows, generations, and retry table are retained exactly.
  [[nodiscard]] common::Result<TabletSnapshot>
  advance_recovered_position(head::HeadCommitPosition position);

  // Removes exactly one sealed generation only after aggregate database publication issued this
  // receipt. Repeated consumption succeeds without another publication. This is a shard-writer
  // operation and cannot overlap a prepared append.
  [[nodiscard]] common::Result<TabletSnapshot>
  retire_sealed_generation(const SealedGenerationRetirementReceipt& receipt);

  [[nodiscard]] common::Result<TabletSnapshot> snapshot() const;
  [[nodiscard]] TabletStateMetrics metrics() const;

  // Idempotently prevents later mutations while preserving the latest complete publication.
  // The shard owner uses this after an unexpected failure in post-publication coordination.
  [[nodiscard]] common::Status fail_closed();

private:
  using PublicationHook = void (*)(void*) noexcept;

  explicit TabletState(std::shared_ptr<detail::TabletStateCore> state) noexcept;
  [[nodiscard]] static common::Result<TabletState>
  create_with_publication_hook(std::shared_ptr<const schema::TableSchema> schema,
                               schema::TabletId tablet_id, TabletStateConfig config,
                               PublicationHook hook, void* hook_context);
  [[nodiscard]] common::Status seed_recovered_prefix(
      schema::SchemaId recovery_schema_id, schema::SchemaVersion recovery_schema_version,
      head::HeadCommitPosition durable_position, std::span<const RetryIdentity> identities,
      std::span<const std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes);

  std::shared_ptr<detail::TabletStateCore> state_;

  friend class detail::ColumnarRecoveryStateBuilder;
  friend class detail::TabletStateTestAccess;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_TABLET_STATE_HPP_
