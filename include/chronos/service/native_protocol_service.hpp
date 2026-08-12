#ifndef CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
#define CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/single_node_database.hpp"

namespace chronos::service {

struct NativeProtocolServiceLimits {
  network::ProtocolLimits protocol{};
  ingest::ColumnarAppendDecodeLimits columnar_append{};
};

// Thread-affine synchronous translation between an already accepted native request and the
// single-node database owner. Returned tasks retain the connection/principal routing envelope and
// contain one codec-validated terminal response. Queueing and socket backpressure remain owned by
// the reactor worker.
class NativeProtocolService {
public:
  explicit NativeProtocolService(SingleNodeDatabase& database,
                                 NativeProtocolServiceLimits limits = {}) noexcept;

  [[nodiscard]] common::Result<network::NetworkTask> execute_ingest(network::NetworkTask request);

private:
  SingleNodeDatabase* database_;
  NativeProtocolServiceLimits limits_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_PROTOCOL_SERVICE_HPP_
