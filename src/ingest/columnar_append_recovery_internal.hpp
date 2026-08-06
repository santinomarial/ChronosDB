#ifndef CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_INTERNAL_HPP_
#define CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_INTERNAL_HPP_

#include "chronos/ingest/tablet_state.hpp"

namespace chronos::ingest::detail {

class ColumnarRecoveryStateBuilder {
public:
  [[nodiscard]] static common::Status
  seed_tablet(TabletState& state, const schema::SchemaId recovery_schema_id,
              const schema::SchemaVersion recovery_schema_version,
              const head::HeadCommitPosition durable_position,
              const std::span<const RetryIdentity> identities,
              const std::span<const std::shared_ptr<const ColumnarAppendRetryOutcome>> outcomes) {
    return state.seed_recovered_prefix(recovery_schema_id, recovery_schema_version,
                                       durable_position, identities, outcomes);
  }
};

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_COLUMNAR_APPEND_RECOVERY_INTERNAL_HPP_
