#ifndef CHRONOS_INGEST_DEDUPLICATION_KEY_HPP_
#define CHRONOS_INGEST_DEDUPLICATION_KEY_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/status.hpp"
#include "chronos/head/mutable_head.hpp"

#include <span>

namespace chronos::ingest::detail {

// Validates APPEND_ROWS logical-key uniqueness within the incoming batch and against every
// currently visible tablet generation. Schema validation guarantees that key columns are non-null
// and retain their identities, ordinals, types, and nullability across the supplied lineage.
[[nodiscard]] common::Status
validate_append_deduplication(const columnar::OwnedColumnarBatch& batch,
                              std::span<const head::HeadSnapshot> sealed_generations,
                              const head::HeadSnapshot& active_generation);

} // namespace chronos::ingest::detail

#endif // CHRONOS_INGEST_DEDUPLICATION_KEY_HPP_
