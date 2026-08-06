#ifndef CHRONOS_CSEG_VALIDATOR_HPP_
#define CHRONOS_CSEG_VALIDATOR_HPP_

#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstdint>

namespace chronos::cseg {

struct CsegValidationLimits {
  // Bounds simultaneously resident uncompressed ordering/system pages plus one copied boundary
  // row. A caller may raise this explicit runtime limit for wider valid schemas.
  std::uint64_t max_working_bytes{4U * format::kMaximumUncompressedPageLength};
};

// Completes schema-independent acceptance of an already structurally decoded part. It validates
// every WAL identity, record sequence, operation code, event-time extremum, and adjacent physical
// row tuple across page and granule boundaries. Zero/invalid known values are corruption; an
// unknown nonzero operation is not supported; configured working-memory excess is resource
// exhaustion. The part's encoded owner must remain alive and immutable throughout the call.
[[nodiscard]] common::Status validate_cseg_v1_part_contents(const DecodedCsegPartView& part,
                                                            CsegValidationLimits limits = {});

// Complete pre-installation acceptance: exact schema/tablet binding followed by full content
// validation. A binding mismatch is invalid caller/catalog context rather than byte corruption.
[[nodiscard]] common::Status validate_cseg_v1_part(const DecodedCsegPartView& part,
                                                   const schema::TableSchema& schema,
                                                   const schema::TabletId& target_tablet,
                                                   CsegValidationLimits limits = {});

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_VALIDATOR_HPP_
