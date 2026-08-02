#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "wal/wal_segment_internal.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::wal::detail {
namespace {

[[nodiscard]] common::Status with_context(std::string_view context, const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return common::Status{status.code(), std::move(message)};
}

} // namespace

common::Result<ActiveWalSegment> install_segment(io::PosixDirectory& directory, const WalId& wal_id,
                                                 const std::uint64_t segment_number,
                                                 const std::uint64_t first_record_sequence,
                                                 const std::uint16_t file_permissions) {
  const SegmentHeader header{.wal_id = wal_id,
                             .segment_number = segment_number,
                             .first_record_sequence = first_record_sequence};
  const common::Result<EncodedSegmentHeader> encoded_header = encode_segment_header(header);
  if (!encoded_header.has_value()) {
    return common::make_unexpected(encoded_header.error());
  }
  const common::Result<std::string> final_name = wal_segment_file_name(segment_number);
  if (!final_name.has_value()) {
    return common::make_unexpected(final_name.error());
  }
  const common::Result<std::string> temporary_name =
      wal_temporary_segment_file_name(segment_number, wal_id);
  if (!temporary_name.has_value()) {
    return common::make_unexpected(temporary_name.error());
  }

  common::Result<io::PosixFile> temporary =
      directory.create_exclusive_regular_file(*temporary_name, file_permissions);
  if (!temporary.has_value()) {
    return common::make_unexpected(with_context("create temporary WAL segment", temporary.error()));
  }
  common::Status status = temporary->write_all_at(0U, *encoded_header);
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("write WAL segment header", status));
  }
  const common::Result<std::uint64_t> size = temporary->size();
  if (!size.has_value()) {
    return common::make_unexpected(with_context("verify WAL segment header size", size.error()));
  }
  if (*size != kSegmentHeaderSize) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kIoError, "temporary WAL segment size is not exactly 64 bytes"});
  }
  status = temporary->sync_all();
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("synchronize temporary WAL segment", status));
  }
  status = directory.rename_no_replace({.old_name = *temporary_name, .new_name = *final_name});
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("install WAL segment final name", status));
  }
  status = directory.sync();
  if (!status.is_ok()) {
    return common::make_unexpected(with_context("synchronize WAL directory after install", status));
  }

  return ActiveWalSegment{
      .metadata =
          WalSegment{.file_name = *final_name, .header = header, .end_offset = kSegmentHeaderSize},
      .file = std::move(*temporary),
  };
}

} // namespace chronos::wal::detail
