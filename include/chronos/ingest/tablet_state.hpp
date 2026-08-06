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

namespace chronos::ingest {

struct TabletStateConfig {
  head::MutableHeadCapacity head_capacity;

  // Sealed generations remain query-visible because flush handoff is not implemented yet.
  // Both bounds must be nonzero; reaching either one rejects a new append before WAL.
  std::size_t maximum_sealed_generations{};
  std::size_t maximum_retry_entries{};
};

struct TabletStateMetrics {
  std::size_t maximum_sealed_generations{};
  std::size_t sealed_generations{};
  std::size_t maximum_retry_entries{};
  std::size_t retry_entries{};
  std::uint64_t active_generation{};
  std::uint32_t active_rows{};
  std::size_t visible_rows{};
  bool failed{false};
};

namespace detail {
class TabletPublication;
class TabletStateCore;
class TabletStateTestAccess;
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

// One schema-bound tablet's bounded in-memory publication owner. Exactly one shard writer calls
// prepare/mark/publish/cancel; readers may acquire and scan snapshots concurrently. WAL I/O,
// global retry reservation, routing, schema admission, replay, and flush handoff are external.
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

  // Validates the tablet/mutation/schema identity, enforces all local bounds, rotates when the
  // whole batch fits only an empty generation, and allocates the complete outer descriptor and
  // retry outcome before returning. It does not perform WAL I/O or publish rows.
  [[nodiscard]] common::Result<PreparedTabletAppend>
  prepare_append(const RetryIdentity& retry_identity,
                 const ColumnarAppendMutationIdentity& mutation,
                 std::shared_ptr<const columnar::OwnedColumnarBatch> batch);

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

  std::shared_ptr<detail::TabletStateCore> state_;

  friend class detail::TabletStateTestAccess;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_TABLET_STATE_HPP_
