#ifndef CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_
#define CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_

#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

namespace chronos::wal {

struct WalWriterConfig {
  // The caller must supply an existing, dedicated WAL directory whose own directory entry has
  // already crossed the parent-directory durability boundary. Only an empty directory or one
  // containing a regular LOCK file is accepted. create_new() never cleans, opens, or repairs an
  // existing or interrupted WAL history.
  std::string directory_path;
  std::uint16_t file_permissions{0600U};

  // Runtime rotation policy. This value is not serialized: every v1 header continues to carry the
  // frozen kSegmentSizeLimit. A writer may rotate earlier but never beyond that format limit.
  std::uint64_t target_segment_size{kSegmentSizeLimit};

  // Maximum complete APPLICATION_ENTRY payload accepted by this writer, including the 16-byte
  // application envelope. The immutable physical-format hard limit remains kMaximumPayloadLength.
  std::size_t maximum_application_payload{kMaximumPayloadLength};
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_
