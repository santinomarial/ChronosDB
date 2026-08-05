#ifndef CHRONOS_HEAD_MUTABLE_HEAD_INTERNAL_HPP_
#define CHRONOS_HEAD_MUTABLE_HEAD_INTERNAL_HPP_

#include "chronos/head/mutable_head.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace chronos::head::detail {

class MutableHeadTestAccess {
public:
  using MaterializationHook = void (*)(void*, std::size_t) noexcept;

  [[nodiscard]] static common::Result<MutableHead>
  create(std::shared_ptr<const schema::TableSchema> schema, const schema::TabletId tablet_id,
         const std::uint64_t generation, MutableHeadCapacity capacity,
         const MaterializationHook hook, void* const hook_context) {
    return MutableHead::create_with_materialization_hook(std::move(schema), tablet_id, generation,
                                                         std::move(capacity), hook, hook_context);
  }
};

} // namespace chronos::head::detail

#endif // CHRONOS_HEAD_MUTABLE_HEAD_INTERNAL_HPP_
