#ifndef CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_
#define CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_

#include <cstdint>
#include <string>

namespace chronos::wal {

struct WalWriterConfig {
  // The caller must supply an existing, dedicated WAL directory whose own directory entry has
  // already crossed the parent-directory durability boundary. create_new() never opens or repairs
  // an existing WAL history.
  std::string directory_path;
  std::uint16_t file_permissions{0600U};
};

} // namespace chronos::wal

#endif // CHRONOS_WAL_WAL_WRITER_CONFIG_HPP_
