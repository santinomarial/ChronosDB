#include "chronos/cseg/format.hpp"

namespace {
static_assert(chronos::cseg::format::kFileHeaderLength == 256U);
static_assert(chronos::cseg::format::kColumnDescriptorLength == 96U);
static_assert(chronos::cseg::format::kGranuleDescriptorLength == 64U);
static_assert(chronos::cseg::format::kPageDescriptorLength == 80U);
} // namespace
