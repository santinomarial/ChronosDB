#include "chronos/ingest/columnar_append_recovery.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "columnar_append_recovery_internal.hpp"

#include <algorithm>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status not_found(std::string message) {
  return common::Status{common::StatusCode::kNotFound, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status replay_inconsistency(const common::Status& status,
                                                  const std::string& context) {
  switch (status.code()) {
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kAlreadyExists:
  case common::StatusCode::kUnavailable:
    return corruption(context + ": " + status.message());
  default:
    return status;
  }
}

[[nodiscard]] common::Result<std::shared_ptr<const columnar::OwnedColumnarBatch>>
own_batch(const columnar::DecodedColumnarBatchView& decoded,
          std::shared_ptr<const schema::TableSchema> schema,
          const ColumnarAppendDecodeLimits& limits) {
  try {
    std::vector<columnar::OwnedColumnVector> columns;
    columns.reserve(decoded.columns().size());
    for (const columnar::ColumnVectorView& view : decoded.columns()) {
      columnar::ColumnVectorBuffers buffers{
          .validity = std::vector<std::byte>{view.validity().begin(), view.validity().end()},
          .offsets = std::vector<std::byte>{view.offsets().begin(), view.offsets().end()},
          .values = std::vector<std::byte>{view.values().begin(), view.values().end()},
      };
      common::Result<columnar::OwnedColumnVector> column = columnar::OwnedColumnVector::create(
          columnar::ColumnVectorMetadata{.column_id = view.column_id(),
                                         .type = view.type(),
                                         .nullable = view.nullable(),
                                         .row_count = view.row_count(),
                                         .null_count = view.null_count()},
          std::move(buffers));
      if (!column.has_value()) {
        return common::make_unexpected(
            replay_inconsistency(column.error(), "decoded column could not become owned"));
      }
      columns.push_back(std::move(*column));
    }

    common::Result<columnar::OwnedColumnarBatch> batch = columnar::OwnedColumnarBatch::create(
        std::move(schema), std::move(columns),
        columnar::ColumnarBatchLimits{.max_rows = limits.batch.max_rows,
                                      .max_columns = limits.batch.max_columns,
                                      .max_buffer_bytes = limits.batch.max_batch_length,
                                      .max_retained_buffer_bytes = limits.batch.max_batch_length});
    if (!batch.has_value()) {
      return common::make_unexpected(
          replay_inconsistency(batch.error(), "decoded batch could not become owned"));
    }
    return std::make_shared<const columnar::OwnedColumnarBatch>(std::move(*batch));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("recovered batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("recovered batch exceeds container limits"));
  }
}

} // namespace

class RecoveredColumnarAppendState::Impl {
public:
  struct TabletEntry {
    std::vector<std::shared_ptr<const schema::TableSchema>> schemas;
    schema::TabletId tablet_id;
    TabletState state;
  };

  struct SeedRetryExpectation {
    RetryIdentity identity;
    std::uint64_t record_sequence{};
    bool observed{false};
  };

  struct TabletBoundaryExpectation {
    schema::TabletId tablet_id;
    std::uint64_t record_sequence{};
    bool observed{false};
  };

  class ReplaySink final : public wal::WalReplaySink {
  public:
    explicit ReplaySink(Impl& owner) noexcept : owner_(owner) {}

    [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) override {
      try {
        return owner_.preflight(record);
      } catch (const std::bad_alloc&) {
        return exhausted("COLUMNAR_APPEND recovery preflight allocation failed");
      } catch (const std::length_error&) {
        return exhausted("COLUMNAR_APPEND recovery preflight exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"COLUMNAR_APPEND recovery preflight threw: "} + error.what());
      } catch (...) {
        return internal("COLUMNAR_APPEND recovery preflight threw an unknown exception");
      }
    }

    [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) override {
      try {
        return owner_.replay(record);
      } catch (const std::bad_alloc&) {
        return exhausted("COLUMNAR_APPEND recovery replay allocation failed");
      } catch (const std::length_error&) {
        return exhausted("COLUMNAR_APPEND recovery replay exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"COLUMNAR_APPEND recovery replay threw: "} + error.what());
      } catch (...) {
        return internal("COLUMNAR_APPEND recovery replay threw an unknown exception");
      }
    }

  private:
    Impl& owner_;
  };

  Impl(RetryDirectory retry_directory, std::vector<TabletEntry> tablets,
       std::vector<SeedRetryExpectation> seed_retry_expectations,
       std::vector<TabletBoundaryExpectation> tablet_boundary_expectations,
       const ColumnarAppendDecodeLimits decode_limits) noexcept
      : retry_directory_(std::move(retry_directory)), tablets_(std::move(tablets)),
        seed_retry_expectations_(std::move(seed_retry_expectations)),
        tablet_boundary_expectations_(std::move(tablet_boundary_expectations)),
        decode_limits_(decode_limits) {}

  [[nodiscard]] common::Status validate_seed_observations() const {
    if (std::ranges::any_of(seed_retry_expectations_,
                            [](const SeedRetryExpectation& item) { return !item.observed; })) {
      return corruption("durable retry outcome is absent from the required WAL suffix");
    }
    if (std::ranges::any_of(tablet_boundary_expectations_,
                            [](const TabletBoundaryExpectation& item) { return !item.observed; })) {
      return corruption("tablet durable boundary is absent from the required WAL suffix");
    }
    return common::Status::ok();
  }

  [[nodiscard]] TabletEntry* find_tablet(const schema::TabletId& tablet_id) noexcept {
    const auto found =
        std::find_if(tablets_.begin(), tablets_.end(),
                     [&tablet_id](TabletEntry& item) { return item.tablet_id == tablet_id; });
    return found == tablets_.end() ? nullptr : &*found;
  }

  [[nodiscard]] const TabletEntry* find_tablet(const schema::TabletId& tablet_id) const noexcept {
    const auto found =
        std::find_if(tablets_.begin(), tablets_.end(),
                     [&tablet_id](const TabletEntry& item) { return item.tablet_id == tablet_id; });
    return found == tablets_.end() ? nullptr : &*found;
  }

  [[nodiscard]] common::Result<DecodedColumnarAppendView>
  decode(const wal::WalReplayRecord& record) const {
    ColumnarAppendDecodeResult decoded = decode_columnar_append_v1_record(
        wal::DecodedRecord{.header = record.header, .payload = record.payload}, decode_limits_);
    if (!decoded.has_value()) {
      if (decoded.error().kind() == ColumnarAppendDecodeErrorKind::kIncomplete) {
        return common::make_unexpected(
            corruption("complete WAL record contains an incomplete COLUMNAR_APPEND payload"));
      }
      return common::make_unexpected(decoded.error().status());
    }
    return std::move(*decoded);
  }

  [[nodiscard]] common::Result<std::shared_ptr<const schema::TableSchema>>
  resolve_schema(const DecodedColumnarAppendView& command) const {
    const TabletEntry* const target = find_tablet(command.tablet_id());
    if (target == nullptr) {
      return common::make_unexpected(
          not_found("COLUMNAR_APPEND references an unconfigured recovery tablet"));
    }
    if (target->schemas.empty()) {
      return common::make_unexpected(
          internal("configured recovery tablet lost its schema lineage"));
    }
    if (command.table_id() != target->schemas.front()->table_id()) {
      return common::make_unexpected(
          corruption("COLUMNAR_APPEND table identity does not match its recovery tablet"));
    }
    const auto found = std::find_if(
        target->schemas.begin(), target->schemas.end(),
        [&command](const auto& retained) { return retained->schema_id() == command.schema_id(); });
    if (found == target->schemas.end()) {
      return common::make_unexpected(
          not_found("COLUMNAR_APPEND schema is absent from the retained recovery lineage"));
    }
    const common::Status schema_status = validate_columnar_append_schema(command, **found);
    if (!schema_status.is_ok()) {
      return common::make_unexpected(
          corruption("COLUMNAR_APPEND does not match its retained recovery schema: " +
                     schema_status.message()));
    }
    return *found;
  }

  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) const {
    common::Result<DecodedColumnarAppendView> command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    common::Result<std::shared_ptr<const schema::TableSchema>> retained = resolve_schema(*command);
    return retained.has_value() ? common::Status::ok() : retained.error();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) {
    common::Result<DecodedColumnarAppendView> command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    common::Result<std::shared_ptr<const schema::TableSchema>> retained_schema =
        resolve_schema(*command);
    if (!retained_schema.has_value()) {
      return retained_schema.error();
    }
    TabletEntry* const target = find_tablet(command->tablet_id());
    if (target == nullptr) {
      return internal("preflighted recovery tablet disappeared before replay");
    }

    const RetryIdentity retry_identity{.client_id = command->client_id(),
                                       .client_batch_id = command->client_batch_id()};
    const ColumnarAppendMutationIdentity mutation{.table_id = command->table_id(),
                                                  .tablet_id = command->tablet_id(),
                                                  .request_digest = command->request_digest()};
    common::Result<RetryDecision> decision = retry_directory_.try_reserve(retry_identity, mutation);
    if (!decision.has_value()) {
      return decision.error();
    }
    const head::HeadCommitPosition position{.wal_id = record.record_start.wal_id,
                                            .record_sequence = record.header.record_sequence};
    switch (decision->kind()) {
    case RetryDecisionKind::kConflict:
      return corruption("WAL history reuses a client batch identity for a different mutation");
    case RetryDecisionKind::kInFlight:
      return corruption("WAL replay encountered an impossible in-flight retry identity");
    case RetryDecisionKind::kMatchingCommitted: {
      const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome =
          decision->committed_outcome();
      if (outcome == nullptr || outcome->applied_row_count != command->row_count()) {
        return corruption("matching recovered retry disagrees with its published outcome");
      }
      common::Result<TabletSnapshot> advanced =
          target->state.advance_recovered_retry(retry_identity, mutation, outcome, position);
      if (!advanced.has_value()) {
        return replay_inconsistency(advanced.error(),
                                    "matching recovered retry could not advance tablet position");
      }
      if (record.header.record_sequence == outcome->record_sequence) {
        const auto expected =
            std::ranges::find_if(seed_retry_expectations_, [&](const SeedRetryExpectation& item) {
              return item.identity == retry_identity &&
                     item.record_sequence == record.header.record_sequence;
            });
        if (expected != seed_retry_expectations_.end()) {
          expected->observed = true;
        }
      }
      const auto boundary = std::ranges::find_if(
          tablet_boundary_expectations_, [&](const TabletBoundaryExpectation& item) {
            return item.tablet_id == command->tablet_id() &&
                   item.record_sequence == record.header.record_sequence;
          });
      if (boundary != tablet_boundary_expectations_.end()) {
        boundary->observed = true;
      }
      return common::Status::ok();
    }
    case RetryDecisionKind::kReserved:
      break;
    }
    if (decision->reservation() == nullptr) {
      return internal("recovery retry reservation has no owner handle");
    }
    RetryReservation reservation = std::move(*decision->reservation());
    common::Result<std::shared_ptr<const columnar::OwnedColumnarBatch>> batch =
        own_batch(command->batch(), std::move(*retained_schema), decode_limits_);
    if (!batch.has_value()) {
      return batch.error();
    }
    common::Result<PreparedTabletAppend> prepared =
        target->state.prepare_append(retry_identity, mutation, std::move(*batch));
    if (!prepared.has_value()) {
      return replay_inconsistency(prepared.error(),
                                  "recovered append could not reserve tablet state");
    }
    common::Status retry_started = reservation.mark_wal_started();
    if (!retry_started.is_ok()) {
      return replay_inconsistency(retry_started,
                                  "recovered retry could not cross its WAL boundary");
    }
    common::Status tablet_started = prepared->mark_wal_started();
    if (!tablet_started.is_ok()) {
      return replay_inconsistency(tablet_started,
                                  "recovered tablet append could not cross its WAL boundary");
    }
    common::Result<TabletAppendResult> published = prepared->publish(position);
    if (!published.has_value()) {
      return replay_inconsistency(published.error(), "recovered tablet append could not publish");
    }
    common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>> committed =
        reservation.commit_published(published->outcome);
    if (!committed.has_value()) {
      return replay_inconsistency(committed.error(),
                                  "recovered retry outcome could not become globally visible");
    }
    if (committed->get() != published->outcome.get()) {
      return internal("recovery changed the tablet-published retry outcome object");
    }
    return common::Status::ok();
  }

  RetryDirectory retry_directory_;
  std::vector<TabletEntry> tablets_;
  std::vector<SeedRetryExpectation> seed_retry_expectations_;
  std::vector<TabletBoundaryExpectation> tablet_boundary_expectations_;
  ColumnarAppendDecodeLimits decode_limits_;
  std::optional<wal::WalWriter> writer_;
};

RecoveredColumnarAppendState::RecoveredColumnarAppendState(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

RecoveredColumnarAppendState::~RecoveredColumnarAppendState() = default;
RecoveredColumnarAppendState::RecoveredColumnarAppendState(
    RecoveredColumnarAppendState&&) noexcept = default;
RecoveredColumnarAppendState&
RecoveredColumnarAppendState::operator=(RecoveredColumnarAppendState&&) noexcept = default;

RetryDirectory& RecoveredColumnarAppendState::retry_directory() noexcept {
  return implementation_->retry_directory_;
}

const RetryDirectory& RecoveredColumnarAppendState::retry_directory() const noexcept {
  return implementation_->retry_directory_;
}

TabletState* RecoveredColumnarAppendState::tablet(const schema::TabletId& tablet_id) noexcept {
  Impl::TabletEntry* const found = implementation_->find_tablet(tablet_id);
  return found == nullptr ? nullptr : &found->state;
}

const TabletState*
RecoveredColumnarAppendState::tablet(const schema::TabletId& tablet_id) const noexcept {
  const Impl::TabletEntry* const found = implementation_->find_tablet(tablet_id);
  return found == nullptr ? nullptr : &found->state;
}

std::size_t RecoveredColumnarAppendState::tablet_count() const noexcept {
  return implementation_->tablets_.size();
}

common::Result<wal::WalSegmentReclamationReport>
RecoveredColumnarAppendState::reclaim_checkpointed_segments(
    const wal::WalReplayCheckpoint& checkpoint) {
  std::optional<wal::WalWriter>& writer = implementation_->writer_;
  if (!writer.has_value()) {
    return common::make_unexpected(invalid("recovered WAL writer has already been released"));
  }
  return writer->reclaim_checkpointed_segments(checkpoint);
}

common::Result<wal::WalWriter> RecoveredColumnarAppendState::release_writer() {
  std::optional<wal::WalWriter>& retained_writer = implementation_->writer_;
  if (!retained_writer.has_value()) {
    return common::make_unexpected(invalid("recovered WAL writer has already been released"));
  }
  wal::WalWriter writer = std::move(*retained_writer);
  retained_writer.reset();
  return writer;
}

common::Result<RecoveredColumnarAppendState>
recover_columnar_append_wal(const wal::WalWriterConfig& writer_config,
                            const wal::WalRecoveryOptions& recovery_options,
                            ColumnarAppendRecoveryConfig recovery_config) {
  if (recovery_config.tablets.empty()) {
    return common::make_unexpected(
        invalid("COLUMNAR_APPEND recovery requires at least one configured tablet"));
  }
  if (recovery_config.decode_limits.max_application_payload_length == 0U ||
      recovery_config.decode_limits.max_application_payload_length >
          columnar_append_v1::kMaximumApplicationPayloadLength ||
      recovery_config.decode_limits.batch.max_batch_length == 0U ||
      recovery_config.decode_limits.batch.max_batch_length >
          columnar::format::kMaximumEmbeddedBatchLength ||
      recovery_config.decode_limits.batch.max_rows == 0U ||
      recovery_config.decode_limits.batch.max_columns == 0U ||
      recovery_config.decode_limits.batch.max_columns > columnar::format::kMaximumColumnCount) {
    return common::make_unexpected(
        invalid("COLUMNAR_APPEND recovery decode limits are outside v1 bounds"));
  }

  try {
    common::Result<RetryDirectory> retry_directory =
        RetryDirectory::create(recovery_config.retry_directory);
    if (!retry_directory.has_value()) {
      return common::make_unexpected(retry_directory.error());
    }
    std::vector<RecoveredColumnarAppendState::Impl::TabletEntry> tablets;
    tablets.reserve(recovery_config.tablets.size());
    std::vector<RecoveredColumnarAppendState::Impl::SeedRetryExpectation> seed_retry_expectations;
    std::vector<RecoveredColumnarAppendState::Impl::TabletBoundaryExpectation>
        tablet_boundary_expectations;
    for (ColumnarRecoveryTabletConfig& configured : recovery_config.tablets) {
      if (configured.schema == nullptr) {
        return common::make_unexpected(
            invalid("COLUMNAR_APPEND recovery tablet requires a retained schema"));
      }
      const bool duplicate =
          std::any_of(tablets.begin(), tablets.end(), [&configured](const auto& existing) {
            return existing.tablet_id == configured.tablet_id;
          });
      if (duplicate) {
        return common::make_unexpected(
            invalid("COLUMNAR_APPEND recovery configuration repeats a tablet identity"));
      }

      std::vector<std::shared_ptr<const schema::TableSchema>> schemas;
      std::vector<head::MutableHeadCapacity> capacities;
      schemas.reserve(configured.successors.size() + 1U);
      capacities.reserve(configured.successors.size() + 1U);
      schemas.push_back(configured.schema);
      capacities.push_back(configured.state.head_capacity);
      common::Result<schema::SchemaLineage> lineage =
          schema::SchemaLineage::create(*configured.schema);
      if (!lineage.has_value()) {
        return common::make_unexpected(lineage.error());
      }
      for (const ColumnarRecoverySuccessorSchemaConfig& successor : configured.successors) {
        if (successor.schema == nullptr) {
          return common::make_unexpected(
              invalid("COLUMNAR_APPEND recovery successor requires a retained schema"));
        }
        common::Status appended = lineage->append(*successor.schema);
        if (!appended.is_ok()) {
          return common::make_unexpected(std::move(appended));
        }
        schemas.push_back(successor.schema);
        capacities.push_back(successor.head_capacity);
      }

      if (configured.durable_seed.has_value()) {
        if (!recovery_config.checkpoint.has_value()) {
          return common::make_unexpected(
              invalid("tablet durable seed requires a global WAL checkpoint"));
        }
        const auto found =
            std::find_if(schemas.begin(), schemas.end(), [&configured](const auto& retained) {
              return retained->schema_id() == configured.durable_seed->recovery_schema_id &&
                     retained->version() == configured.durable_seed->recovery_schema_version;
            });
        if (found == schemas.end()) {
          return common::make_unexpected(
              invalid("tablet durable recovery schema is absent from its retained lineage"));
        }
      }

      common::Result<TabletState> state =
          TabletState::create(configured.schema, configured.tablet_id, std::move(configured.state));
      if (!state.has_value()) {
        return common::make_unexpected(state.error());
      }
      for (std::size_t index = 1U; index < schemas.size(); ++index) {
        common::Status registered = state->register_schema(schemas[index], capacities[index]);
        if (!registered.is_ok()) {
          return common::make_unexpected(std::move(registered));
        }
      }

      if (configured.durable_seed.has_value()) {
        const wal::WalReplayCheckpoint& checkpoint = *recovery_config.checkpoint;
        const ColumnarRecoveryTabletSeed& seed = *configured.durable_seed;
        if (!checkpoint.wal_id.is_valid() || seed.durable_record_sequence == 0U) {
          return common::make_unexpected(
              invalid("durable recovery seed requires nonzero WAL and tablet boundaries"));
        }
        std::vector<RetryIdentity> identities;
        std::vector<std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes;
        identities.reserve(seed.retries.size());
        outcomes.reserve(seed.retries.size());
        for (const ColumnarRecoveryRetrySeed& retry : seed.retries) {
          auto outcome = std::make_shared<const ColumnarAppendRetryOutcome>(retry.outcome);
          if (outcome->mutation.table_id != schemas.front()->table_id() ||
              outcome->mutation.tablet_id != configured.tablet_id ||
              outcome->wal_id != checkpoint.wal_id || outcome->record_sequence == 0U ||
              outcome->record_sequence > seed.durable_record_sequence ||
              outcome->applied_row_count == 0U) {
            return common::make_unexpected(
                invalid("durable recovery retry does not bind its tablet WAL boundary"));
          }
          common::Result<RetryDecision> decision =
              retry_directory->try_reserve(retry.identity, outcome->mutation);
          if (!decision.has_value()) {
            return common::make_unexpected(decision.error());
          }
          if (decision->kind() != RetryDecisionKind::kReserved ||
              decision->reservation() == nullptr) {
            return common::make_unexpected(
                invalid("durable recovery retry identity is not uniquely reservable"));
          }
          RetryReservation reservation = std::move(*decision->reservation());
          common::Status started = reservation.mark_wal_started();
          if (!started.is_ok()) {
            return common::make_unexpected(std::move(started));
          }
          common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>> committed =
              reservation.commit_published(outcome);
          if (!committed.has_value()) {
            return common::make_unexpected(committed.error());
          }
          identities.push_back(retry.identity);
          outcomes.push_back(std::move(*committed));
          if (outcome->record_sequence > checkpoint.record_sequence) {
            seed_retry_expectations.push_back(
                RecoveredColumnarAppendState::Impl::SeedRetryExpectation{
                    .identity = retry.identity, .record_sequence = outcome->record_sequence});
          }
        }
        if (seed.durable_record_sequence > checkpoint.record_sequence) {
          tablet_boundary_expectations.push_back(
              RecoveredColumnarAppendState::Impl::TabletBoundaryExpectation{
                  .tablet_id = configured.tablet_id,
                  .record_sequence = seed.durable_record_sequence});
        }
        common::Status seeded = detail::ColumnarRecoveryStateBuilder::seed_tablet(
            *state, seed.recovery_schema_id, seed.recovery_schema_version,
            head::HeadCommitPosition{.wal_id = checkpoint.wal_id,
                                     .record_sequence = seed.durable_record_sequence},
            identities, outcomes);
        if (!seeded.is_ok()) {
          return common::make_unexpected(std::move(seeded));
        }
      }
      tablets.push_back(RecoveredColumnarAppendState::Impl::TabletEntry{
          .schemas = std::move(schemas),
          .tablet_id = configured.tablet_id,
          .state = std::move(*state),
      });
    }
    auto implementation = std::make_unique<RecoveredColumnarAppendState::Impl>(
        std::move(*retry_directory), std::move(tablets), std::move(seed_retry_expectations),
        std::move(tablet_boundary_expectations), recovery_config.decode_limits);
    RecoveredColumnarAppendState::Impl::ReplaySink sink{*implementation};
    common::Result<wal::WalWriter> writer =
        recovery_config.checkpoint.has_value()
            ? wal::WalWriter::open_existing_from_checkpoint(writer_config, recovery_options,
                                                            *recovery_config.checkpoint, sink)
            : wal::WalWriter::open_existing(writer_config, recovery_options, sink);
    if (!writer.has_value()) {
      return common::make_unexpected(writer.error());
    }
    common::Status observations = implementation->validate_seed_observations();
    if (!observations.is_ok()) {
      return common::make_unexpected(std::move(observations));
    }
    implementation->writer_.emplace(std::move(*writer));
    return RecoveredColumnarAppendState{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("COLUMNAR_APPEND recovery state allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("COLUMNAR_APPEND recovery configuration exceeds container limits"));
  }
}

} // namespace chronos::ingest
