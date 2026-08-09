#ifndef CHRONOS_CSEG_TEMPORAL_SYSTEM_ROW_HPP_
#define CHRONOS_CSEG_TEMPORAL_SYSTEM_ROW_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/temporal_format.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::cseg {

struct TemporalSystemRowView {
  temporal_format::CommitSource commit_source{temporal_format::CommitSource::kWal};
  common::Uuid source_id;
  std::uint64_t commit_position{};
  std::uint32_t row_ordinal{};
  temporal_format::Operation operation{temporal_format::Operation::kOriginal};
  common::ByteView logical_identity;
  std::int64_t receive_time_ns{};
  std::int64_t system_commit_time_ns{};
};

struct TemporalSystemRowLimits {
  std::size_t maximum_logical_identity_bytes{temporal_format::kMaximumLogicalIdentityBytes};
};

// Schema-independent semantic validation for one decoded CSEG v2 system-row tuple. Event time is a
// required user column and is therefore validated by ordinary schema/page binding rather than this
// system suffix.
[[nodiscard]] common::Status validate_temporal_system_row(const TemporalSystemRowView& row,
                                                          TemporalSystemRowLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_TEMPORAL_SYSTEM_ROW_HPP_
