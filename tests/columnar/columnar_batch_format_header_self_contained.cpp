#include "chronos/columnar/columnar_batch_format.hpp"

namespace {
static_assert(chronos::columnar::format::kBatchHeaderLength == 96U);
static_assert(chronos::columnar::format::kColumnDescriptorLength == 80U);
static_assert(chronos::columnar::format::kMaximumEmbeddedBatchLength == 16'776'992U);
} // namespace
