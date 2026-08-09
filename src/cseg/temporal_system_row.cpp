#include "chronos/cseg/temporal_system_row.hpp"

#include <string>
#include <string_view>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

} // namespace

common::Status validate_temporal_system_row(const TemporalSystemRowView& row,
                                            const TemporalSystemRowLimits limits) {
  if (limits.maximum_logical_identity_bytes == 0U ||
      limits.maximum_logical_identity_bytes > temporal_format::kMaximumLogicalIdentityBytes) {
    return invalid("CSEG v2 temporal identity limit is outside format bounds");
  }
  if (row.commit_source != temporal_format::CommitSource::kWal &&
      row.commit_source != temporal_format::CommitSource::kRaft) {
    return invalid("CSEG v2 temporal commit source is invalid");
  }
  if (row.source_id.is_nil() || row.commit_position == 0U) {
    return invalid("CSEG v2 temporal source identity or commit position is invalid");
  }
  if (row.operation < temporal_format::Operation::kOriginal ||
      row.operation > temporal_format::Operation::kTombstone) {
    return invalid("CSEG v2 temporal operation is invalid");
  }
  if (row.logical_identity.empty() ||
      row.logical_identity.size() > limits.maximum_logical_identity_bytes) {
    return invalid("CSEG v2 temporal logical identity is outside configured bounds");
  }
  return common::Status::ok();
}

} // namespace chronos::cseg
