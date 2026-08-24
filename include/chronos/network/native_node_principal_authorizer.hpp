#ifndef CHRONOS_NETWORK_NATIVE_NODE_PRINCIPAL_AUTHORIZER_HPP_
#define CHRONOS_NETWORK_NATIVE_NODE_PRINCIPAL_AUTHORIZER_HPP_

#include "chronos/common/result.hpp"

#include <cstdint>

namespace chronos::network {

class NativeNodePrincipalAuthorizer {
public:
  virtual ~NativeNodePrincipalAuthorizer() = default;
  [[nodiscard]] virtual common::Result<bool> authorize_node(std::uint64_t principal_id,
                                                            std::uint64_t node_id) const = 0;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_NODE_PRINCIPAL_AUTHORIZER_HPP_
