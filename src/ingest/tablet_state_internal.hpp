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

  [[nodiscard]] static SealedGenerationRetirementReceipt
  retirement_receipt(schema::TableId table_id, schema::TabletId tablet_id,
                     schema::SchemaId schema_id, schema::SchemaVersion schema_version,
                     std::uint64_t generation, std::uint32_t row_count, wal::WalId wal_id,
                     std::uint64_t minimum_sequence, std::uint64_t maximum_sequence) {
    return SealedGenerationRetirementReceipt{
        SealedGenerationRetirementReceipt::Fields{.table_id = table_id,
                                                  .tablet_id = tablet_id,
                                                  .schema_id = schema_id,
                                                  .schema_version = schema_version,
                                                  .head_generation = generation,
                                                  .row_count = row_count,
                                                  .wal_id = wal_id,
                                                  .minimum_record_sequence = minimum_sequence,
                                                  .maximum_record_sequence = maximum_sequence}};
  }
};

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_TABLET_STATE_INTERNAL_HPP_
