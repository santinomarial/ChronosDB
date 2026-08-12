#include "chronos/wal/wal_log_id_generator.hpp"

#include "chronos/common/uuid_generator.hpp"

namespace chronos::wal {

common::Result<WalId> SystemWalLogIdGenerator::generate() {
  common::SystemUuidGenerator generator;
  auto id = generator.generate();
  if (!id.has_value())
    return common::make_unexpected(id.error());
  return WalId{.bytes = id->bytes()};
}

} // namespace chronos::wal
