#ifndef CHRONOS_WAL_WAL_WRITER_INTERNAL_HPP_
#define CHRONOS_WAL_WAL_WRITER_INTERNAL_HPP_

#include "chronos/wal/wal_writer.hpp"
#include "io/posix_syscalls.hpp"

namespace chronos::wal::detail {

class WalWriterTestAccess {
public:
  [[nodiscard]] static common::Result<WalWriter> create_new(const WalWriterConfig& config,
                                                            WalLogIdGenerator& id_generator,
                                                            io::detail::PosixSyscalls& syscalls) {
    return WalWriter::create_new_with(config, id_generator, syscalls);
  }

  static void set_sequence_state(WalWriter& writer, std::uint64_t next_record_sequence,
                                 bool sequence_exhausted);
  static void set_active_segment_number(WalWriter& writer, std::uint64_t segment_number);
  static void set_active_end_offset(WalWriter& writer, std::uint64_t end_offset);
};

} // namespace chronos::wal::detail

#endif // CHRONOS_WAL_WAL_WRITER_INTERNAL_HPP_
