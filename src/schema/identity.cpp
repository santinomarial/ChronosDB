#include "chronos/schema/identity.hpp"

#include <limits>

namespace chronos::schema {

common::Result<SchemaVersion> SchemaVersion::from_value(const std::uint64_t value) {
  if (value == 0) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                   "schema version must be positive"});
  }
  return SchemaVersion{value};
}

common::Result<SchemaVersion> SchemaVersion::next() const {
  if (value_ == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(common::Status{common::StatusCode::kOutOfRange,
                                                   "schema version is exhausted"});
  }
  return SchemaVersion{value_ + 1U};
}

} // namespace chronos::schema
