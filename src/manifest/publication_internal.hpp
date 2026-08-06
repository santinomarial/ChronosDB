#ifndef CHRONOS_MANIFEST_PUBLICATION_INTERNAL_HPP_
#define CHRONOS_MANIFEST_PUBLICATION_INTERNAL_HPP_

#include "chronos/manifest/publication.hpp"

namespace chronos::manifest::detail {

class DatabaseStoragePublisherTestAccess {
public:
  [[nodiscard]] static common::Result<DatabaseStoragePublisher>
  create(std::shared_ptr<const LoadedManifestGeneration> selected_manifest,
         std::span<const DatabaseStorageTabletInput> tablets,
         DatabaseStoragePublisher::PublicationHook hook, void* hook_context) {
    return DatabaseStoragePublisher::create_with_publication_hook(std::move(selected_manifest),
                                                                  tablets, hook, hook_context);
  }
};

} // namespace chronos::manifest::detail

#endif // CHRONOS_MANIFEST_PUBLICATION_INTERNAL_HPP_
