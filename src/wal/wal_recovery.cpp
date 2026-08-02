#include "chronos/wal/wal_recovery.hpp"

#include "chronos/wal/codec.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_recovery_internal.hpp"
#include "wal/wal_scan_internal.hpp"
#include "wal/wal_writer_config_internal.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return common::Status{status.code(), std::move(message)};
}

[[nodiscard]] common::Status read_exact(const io::PosixFile& file, const std::uint64_t offset,
                                        const common::MutableByteView destination,
                                        const std::string_view context) {
  const common::Result<std::size_t> count = file.read_at(offset, destination);
  if (!count.has_value()) {
    return with_context(context, count.error());
  }
  if (*count != destination.size()) {
    return common::Status{common::StatusCode::kIoError,
                          std::string{context} + ": file changed or returned premature EOF"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<WalRecoveryReport> repair_tail(detail::LockedWalDirectory& locked,
                                                            const WalRecoveryReport& incomplete) {
  if (incomplete.classification != WalScanClassification::kIncompleteFinalTail ||
      locked.discovery.segments.empty() ||
      incomplete.valid_end.segment_number != locked.discovery.segments.back().number ||
      incomplete.valid_end.byte_offset >= incomplete.observed_final_size) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal,
                       "tail repair requested without a valid incomplete-tail report"});
  }
  const detail::DiscoveredWalSegment& final = locked.discovery.segments.back();
  common::Result<io::PosixFile> file =
      locked.directory.open_regular_file(final.file_name, io::FileOpenMode::kReadWrite);
  if (!file.has_value()) {
    return common::make_unexpected(with_context("open final WAL segment for repair", file.error()));
  }
  const common::Result<std::uint64_t> size = file->size();
  if (!size.has_value()) {
    return common::make_unexpected(with_context("recheck final WAL size for repair", size.error()));
  }
  if (*size != incomplete.observed_final_size) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "final WAL segment changed before tail repair"});
  }
  EncodedSegmentHeader encoded_header{};
  common::Status status =
      read_exact(*file, 0U, encoded_header, "recheck final WAL header for repair");
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  const common::Result<SegmentHeader> header = decode_segment_header(encoded_header);
  if (!header.has_value()) {
    return common::make_unexpected(
        with_context("decode final WAL header for repair", header.error()));
  }
  if (header->wal_id != incomplete.wal_id || header->segment_number != final.number) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "final WAL identity changed before tail repair"});
  }

  status = file->truncate(incomplete.valid_end.byte_offset);
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("truncate incomplete final WAL tail", status));
  }
  status = file->sync_all();
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("synchronize repaired final WAL segment", status));
  }
  status = locked.directory.sync();
  if (!status.is_ok()) {
    return common::make_unexpected(
        with_context("synchronize WAL directory after tail repair", status));
  }

  common::Result<WalRecoveryReport> verified =
      detail::scan_discovered_wal(locked.directory, locked.discovery);
  if (!verified.has_value()) {
    return common::make_unexpected(with_context("reverify repaired WAL", verified.error()));
  }
  if (verified->classification != WalScanClassification::kClean) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "tail repair did not produce a clean WAL"});
  }
  verified->repaired = true;
  verified->repair_original_size = incomplete.observed_final_size;
  verified->repair_new_size = incomplete.valid_end.byte_offset;
  return verified;
}

[[nodiscard]] common::Result<WalRecoveryReport>
preflight_and_replay(detail::LockedWalDirectory& locked, const WalRecoveryReport& verified,
                     WalReplaySink& sink) {
  common::Result<WalRecoveryReport> preflight = detail::scan_discovered_wal(
      locked.directory, locked.discovery, detail::ScanPass::kPreflight, &sink);
  if (!preflight.has_value()) {
    return common::make_unexpected(preflight.error());
  }
  common::Status status = detail::require_same_verified_history(verified, *preflight);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }

  common::Result<WalRecoveryReport> replay = detail::scan_discovered_wal(
      locked.directory, locked.discovery, detail::ScanPass::kReplay, &sink);
  if (!replay.has_value()) {
    return common::make_unexpected(replay.error());
  }
  status = detail::require_same_verified_history(verified, *replay);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  return replay;
}

} // namespace

common::Result<WalRecoveryReport> inspect_wal(const std::string_view directory_path,
                                              WalReplaySink& sink) {
  common::Result<detail::LockedWalDirectory> locked = detail::open_locked_wal_directory(
      directory_path, 0600U, false, io::detail::system_posix_syscalls());
  if (!locked.has_value()) {
    return common::make_unexpected(locked.error());
  }
  common::Result<WalRecoveryReport> verified =
      detail::scan_discovered_wal(locked->directory, locked->discovery);
  if (!verified.has_value() ||
      verified->classification == WalScanClassification::kIncompleteFinalTail) {
    return verified;
  }
  return preflight_and_replay(*locked, *verified, sink);
}

common::Result<WalRecoveryReport>
recover_wal(const WalWriterConfig& config, const WalRecoveryOptions& options, WalReplaySink& sink) {
  return detail::recover_wal_with(config, options, sink, io::detail::system_posix_syscalls());
}

namespace detail {

common::Result<RecoveredWalState> recover_existing_for_writer(const WalWriterConfig& config,
                                                              const WalRecoveryOptions& options,
                                                              WalReplaySink& replay_sink,
                                                              io::detail::PosixSyscalls& syscalls) {
  const common::Status config_status = validate_writer_config(config);
  if (!config_status.is_ok()) {
    return common::make_unexpected(config_status);
  }
  common::Result<LockedWalDirectory> locked =
      open_locked_wal_directory(config.directory_path, config.file_permissions, true, syscalls);
  if (!locked.has_value()) {
    return common::make_unexpected(locked.error());
  }
  common::Result<WalRecoveryReport> report =
      scan_discovered_wal(locked->directory, locked->discovery);
  if (!report.has_value()) {
    return common::make_unexpected(report.error());
  }
  if (report->classification == WalScanClassification::kIncompleteFinalTail) {
    if (!options.repair_incomplete_final_tail) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kOutOfRange,
          "WAL has an incomplete final tail; explicit repair authorization is required"});
    }
    report = repair_tail(*locked, *report);
    if (!report.has_value()) {
      return common::make_unexpected(report.error());
    }
  }

  const bool repaired = report->repaired;
  const std::uint64_t repair_original_size = report->repair_original_size;
  const std::uint64_t repair_new_size = report->repair_new_size;
  common::Result<WalRecoveryReport> replayed = preflight_and_replay(*locked, *report, replay_sink);
  if (!replayed.has_value()) {
    return common::make_unexpected(replayed.error());
  }

  common::Result<WalRecoveryReport> final_verification =
      scan_discovered_wal(locked->directory, locked->discovery);
  if (!final_verification.has_value()) {
    return common::make_unexpected(
        with_context("verify WAL after replay", final_verification.error()));
  }
  common::Status status = require_same_verified_history(*replayed, *final_verification);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  final_verification->repaired = repaired;
  final_verification->repair_original_size = repair_original_size;
  final_verification->repair_new_size = repair_new_size;

  const DiscoveredWalSegment& final = locked->discovery.segments.back();
  common::Result<io::PosixFile> active_file =
      locked->directory.open_regular_file(final.file_name, io::FileOpenMode::kReadWrite);
  if (!active_file.has_value()) {
    return common::make_unexpected(
        with_context("open recovered active WAL segment", active_file.error()));
  }
  const common::Result<std::uint64_t> active_size = active_file->size();
  if (!active_size.has_value()) {
    return common::make_unexpected(
        with_context("recheck recovered active WAL size", active_size.error()));
  }
  if (*active_size != final_verification->valid_end.byte_offset) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "active WAL segment changed after final verification"});
  }
  EncodedSegmentHeader encoded_header{};
  status = read_exact(*active_file, 0U, encoded_header, "read recovered active WAL header");
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  const common::Result<SegmentHeader> active_header = decode_segment_header(encoded_header);
  if (!active_header.has_value()) {
    return common::make_unexpected(active_header.error());
  }
  if (active_header->wal_id != final_verification->wal_id ||
      active_header->segment_number != final.number) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "active WAL identity changed after final verification"});
  }

  // This stronger startup barrier makes any surviving process-crash page-cache bytes and any
  // prior interrupted repair durable before the recovered state is published.
  status = active_file->sync_all();
  if (!status.is_ok()) {
    return common::make_unexpected(
        with_context("synchronize recovered active WAL segment", status));
  }
  status = locked->directory.sync();
  if (!status.is_ok()) {
    return common::make_unexpected(
        with_context("synchronize WAL startup namespace barrier", status));
  }

  ActiveWalSegment active_segment{
      .metadata = WalSegment{.file_name = final.file_name,
                             .header = *active_header,
                             .end_offset = final_verification->valid_end.byte_offset},
      .file = std::move(*active_file),
  };
  return RecoveredWalState{.report = *final_verification,
                           .directory = std::move(locked->directory),
                           .lock = std::move(locked->lock),
                           .active_segment = std::move(active_segment)};
}

common::Result<WalRecoveryReport> recover_wal_with(const WalWriterConfig& config,
                                                   const WalRecoveryOptions& options,
                                                   WalReplaySink& replay_sink,
                                                   io::detail::PosixSyscalls& syscalls) {
  common::Result<RecoveredWalState> recovered =
      recover_existing_for_writer(config, options, replay_sink, syscalls);
  if (!recovered.has_value()) {
    return common::make_unexpected(recovered.error());
  }
  const WalRecoveryReport report = recovered->report;
  common::Status first_error = recovered->active_segment.file.close();
  const auto preserve = [&first_error](const common::Status& status) {
    if (first_error.is_ok() && !status.is_ok()) {
      first_error = status;
    }
  };
  preserve(recovered->lock.close());
  preserve(recovered->directory.close());
  if (!first_error.is_ok()) {
    return common::make_unexpected(with_context("close recovered WAL handles", first_error));
  }
  return report;
}

} // namespace detail
} // namespace chronos::wal
