#include "chronos/service/replicated_ingest_database.hpp"

#include <type_traits>

static_assert(std::is_base_of_v<
              chronos::service::ReplicatedDistributedMutableVectorQueryWorkerContextProvider,
              chronos::service::ReplicatedIngestDatabase>);

namespace {

[[maybe_unused]] void header_is_self_contained() {}

} // namespace
