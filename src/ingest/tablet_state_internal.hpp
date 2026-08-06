#ifndef CHRONOS_INGEST_TABLET_STATE_INTERNAL_HPP_
#define CHRONOS_INGEST_TABLET_STATE_INTERNAL_HPP_

#include "chronos/ingest/tablet_state.hpp"

namespace chronos::ingest::detail {

class TabletStateTestAccess {
public:
  using PublicationHook = void (*)(void*) noexcept;

  [[nodiscard]] static common::Result<TabletState>
  create(std::shared_ptr<const schema::TableSchema> schema, schema::TabletId tablet_id,
         TabletStateConfig config, PublicationHook hook, void* hook_context) {
    return TabletState::create_with_publication_hook(std::move(schema), tablet_id,
                                                     std::move(config), hook, hook_context);
  }
};

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_TABLET_STATE_INTERNAL_HPP_
