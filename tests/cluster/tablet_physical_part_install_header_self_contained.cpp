#include "chronos/cluster/tablet_physical_part_install.hpp"

static_assert(chronos::cluster::kDefaultMaximumPhysicalPartMaterializedBytes ==
              std::uint64_t{256U} * 1024U * 1024U);

[[maybe_unused]] auto* const kInstallTabletPhysicalPart =
    &chronos::cluster::install_tablet_physical_part;
