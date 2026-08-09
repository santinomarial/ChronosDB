#include "chronos/query/temporal_recovery.hpp"

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/query/committed_temporal_command.hpp"
#include "chronos/wal/wal_replay_sink.hpp"

#include <algorithm>
#include <exception>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status not_found(std::string message) {
  return common::Status{common::StatusCode::kNotFound, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status replay_error(const common::Status& status) {
  switch (status.code()) {
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kAlreadyExists:
    return corruption("recovered temporal command violates committed history: " + status.message());
  default:
    return status;
  }
}

} // namespace

class RecoveredTemporalState::Impl {
public:
  struct TableEntry {
    std::shared_ptr<const schema::TableSchema> schema;
    std::unique_ptr<TemporalSnapshotProvider> provider;
  };

  class ReplaySink final : public wal::WalReplaySink {
  public:
    explicit ReplaySink(Impl& owner) noexcept : owner_(owner) {}

    [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) override {
      try {
        return owner_.preflight(record);
      } catch (const std::bad_alloc&) {
        return exhausted("temporal recovery preflight allocation failed");
      } catch (const std::length_error&) {
        return exhausted("temporal recovery preflight exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"temporal recovery preflight threw: "} + error.what());
      } catch (...) {
        return internal("temporal recovery preflight threw an unknown exception");
      }
    }

    [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) override {
      try {
        return owner_.replay(record);
      } catch (const std::bad_alloc&) {
        return exhausted("temporal recovery replay allocation failed");
      } catch (const std::length_error&) {
        return exhausted("temporal recovery replay exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"temporal recovery replay threw: "} + error.what());
      } catch (...) {
        return internal("temporal recovery replay threw an unknown exception");
      }
    }

  private:
    Impl& owner_;
  };

  Impl(std::vector<TableEntry> tables, const TemporalCommandLimits decode_limits) noexcept
      : tables_(std::move(tables)), decode_limits_(decode_limits) {}

  [[nodiscard]] TableEntry* find(const schema::TableId table_id) noexcept {
    const auto found = std::ranges::find_if(tables_, [table_id](const TableEntry& entry) {
      return entry.schema->table_id() == table_id;
    });
    return found == tables_.end() ? nullptr : &*found;
  }

  [[nodiscard]] const TableEntry* find(const schema::TableId table_id) const noexcept {
    const auto found = std::ranges::find_if(tables_, [table_id](const TableEntry& entry) {
      return entry.schema->table_id() == table_id;
    });
    return found == tables_.end() ? nullptr : &*found;
  }

  [[nodiscard]] common::Result<DecodedTemporalCommandView>
  decode(const wal::WalReplayRecord& record) const {
    auto command = decode_temporal_command_v1(record.payload, decode_limits_);
    if (!command.has_value()) {
      return common::make_unexpected(command.error());
    }
    return std::move(*command);
  }

  [[nodiscard]] common::Result<TableEntry*> resolve(const DecodedTemporalCommandView& command) {
    TableEntry* const target = find(command.batch().table_id());
    if (target == nullptr) {
      return common::make_unexpected(
          not_found("temporal command references an unconfigured recovery table"));
    }
    const common::Status schema_status =
        columnar::validate_columnar_batch_schema(command.batch(), *target->schema);
    if (!schema_status.is_ok()) {
      return common::make_unexpected(
          corruption("temporal command does not match its retained recovery schema: " +
                     schema_status.message()));
    }
    return target;
  }

  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) {
    auto command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    auto target = resolve(*command);
    return target.has_value() ? common::Status::ok() : target.error();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) {
    auto command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    auto target = resolve(*command);
    if (!target.has_value()) {
      return target.error();
    }
    auto applied = apply_committed_temporal_command(
        *command, *(*target)->schema, record.header.record_sequence, record.record_start.wal_id,
        *(*target)->provider);
    return applied.has_value() ? common::Status::ok() : replay_error(applied.error());
  }

  std::vector<TableEntry> tables_;
  TemporalCommandLimits decode_limits_;
  std::optional<wal::WalWriter> writer_;
};

RecoveredTemporalState::RecoveredTemporalState(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RecoveredTemporalState::~RecoveredTemporalState() = default;
RecoveredTemporalState::RecoveredTemporalState(RecoveredTemporalState&&) noexcept = default;
RecoveredTemporalState&
RecoveredTemporalState::operator=(RecoveredTemporalState&&) noexcept = default;

TemporalSnapshotProvider*
RecoveredTemporalState::provider(const schema::TableId table_id) noexcept {
  Impl::TableEntry* const entry = implementation_->find(table_id);
  return entry == nullptr ? nullptr : entry->provider.get();
}

const TemporalSnapshotProvider*
RecoveredTemporalState::provider(const schema::TableId table_id) const noexcept {
  const Impl::TableEntry* const entry = implementation_->find(table_id);
  return entry == nullptr ? nullptr : entry->provider.get();
}

std::size_t RecoveredTemporalState::table_count() const noexcept {
  return implementation_->tables_.size();
}

common::Result<wal::WalWriter> RecoveredTemporalState::release_writer() {
  if (!implementation_->writer_.has_value()) {
    return common::make_unexpected(invalid("recovered temporal WAL writer was already released"));
  }
  wal::WalWriter writer = std::move(*implementation_->writer_);
  implementation_->writer_.reset();
  return writer;
}

common::Result<RecoveredTemporalState>
recover_temporal_wal(const wal::WalWriterConfig& writer_config,
                     const wal::WalRecoveryOptions& recovery_options,
                     TemporalRecoveryConfig recovery_config) {
  if (recovery_config.tables.empty()) {
    return common::make_unexpected(invalid("temporal recovery requires at least one table"));
  }
  try {
    std::vector<RecoveredTemporalState::Impl::TableEntry> tables;
    tables.reserve(recovery_config.tables.size());
    for (TemporalRecoveryTableConfig& configured : recovery_config.tables) {
      if (configured.schema == nullptr) {
        return common::make_unexpected(invalid("temporal recovery table requires a schema"));
      }
      if (std::ranges::any_of(tables, [&configured](const auto& existing) {
            return existing.schema->table_id() == configured.schema->table_id() ||
                   existing.schema->schema_id() == configured.schema->schema_id();
          })) {
        return common::make_unexpected(
            invalid("temporal recovery configuration repeats a table or schema identity"));
      }
      auto provider = TemporalSnapshotProvider::create(configured.schema, configured.store_limits);
      if (!provider.has_value()) {
        return common::make_unexpected(provider.error());
      }
      tables.push_back(RecoveredTemporalState::Impl::TableEntry{
          .schema = std::move(configured.schema), .provider = std::move(*provider)});
    }
    auto implementation = std::make_unique<RecoveredTemporalState::Impl>(
        std::move(tables), recovery_config.decode_limits);
    RecoveredTemporalState::Impl::ReplaySink sink{*implementation};
    auto writer = wal::WalWriter::open_existing(writer_config, recovery_options, sink);
    if (!writer.has_value()) {
      return common::make_unexpected(writer.error());
    }
    implementation->writer_.emplace(std::move(*writer));
    return RecoveredTemporalState{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("temporal recovery state allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("temporal recovery configuration exceeded container limits"));
  }
}

} // namespace chronos::query
