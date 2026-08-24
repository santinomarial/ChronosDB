#include "chronos/service/native_protocol_service.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <type_traits>

static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::SingleNodeDatabase&>);
static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::SingleNodeDatabase&,
                                      chronos::service::NativeIdentityGenerator&>);
static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::ReplicatedIngestDatabase&>);
static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::ReplicatedIngestDatabase&,
                                      chronos::service::ReplicatedReadBarrier&>);
static_assert(std::is_constructible_v<
              chronos::service::NativeProtocolService, chronos::service::ReplicatedIngestDatabase&,
              chronos::service::ReplicatedReadBarrier&,
              const chronos::service::NativeDistributedMutableVectorRowsQueryConfig&>);
static_assert(std::is_base_of_v<chronos::service::NativeQueryDispatcher,
                                chronos::service::NativeProtocolService>);
static_assert(std::is_default_constructible_v<chronos::service::NativeQueryCancellation>);
