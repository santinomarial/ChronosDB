#ifndef CHRONOS_WAL_WAL_WRITER_CONFIG_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_WRITER_CONFIG_INTERNAL_HPP_

#include "chronos/common/status.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <cstdint>
#include <string>

namespace chronos::wal::detail {

[[nodiscard]] inline common::Status validate_writer_config(const WalWriterConfig& config) {
  if (config.directory_path.empty()) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL directory path must not be empty"};
  }
  if (config.directory_path.find('\0') != std::string::npos) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL directory path contains an embedded NUL byte"};
  }
  if ((config.file_permissions & static_cast<std::uint16_t>(~0777U)) != 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL file permissions contain bits outside 0777"};
  }
  if (config.maximum_application_payload < kApplicationEnvelopeSize) {
    return common::Status{
        common::StatusCode::kInvalidArgument,
        "configured WAL payload maximum is smaller than the application envelope"};
  }
  if (config.maximum_application_payload > kMaximumPayloadLength) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "configured WAL payload maximum exceeds the v1 format limit"};
  }
  const common::Result<RecordLayout> maximum_layout =
      calculate_record_layout(config.maximum_application_payload);
  if (!maximum_layout.has_value()) {
    return maximum_layout.error();
  }
  if (config.target_segment_size <= kSegmentHeaderSize) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "target WAL segment size must exceed the segment header size"};
  }
  if (config.target_segment_size > kSegmentSizeLimit) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "target WAL segment size exceeds the 64 MiB v1 format limit"};
  }
  const std::uint64_t required_size =
      static_cast<std::uint64_t>(kSegmentHeaderSize) + maximum_layout->total_length;
  if (config.target_segment_size < required_size) {
    return common::Status{
        common::StatusCode::kInvalidArgument,
        "target WAL segment size cannot hold one maximum configured application record"};
  }
  return common::Status::ok();
}

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_WRITER_CONFIG_INTERNAL_HPP_
