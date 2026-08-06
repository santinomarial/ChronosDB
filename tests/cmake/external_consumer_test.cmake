if(NOT DEFINED CHRONOS_TEST_BINARY_DIR OR NOT DEFINED CHRONOS_TEST_INSTALL_LIBDIR)
  message(FATAL_ERROR "external consumer test requires the build and install library directories")
endif()

set(install_prefix "${CHRONOS_TEST_BINARY_DIR}/external-consumer-install")
set(consumer_source "${CHRONOS_TEST_BINARY_DIR}/external-consumer-source")
set(consumer_build "${CHRONOS_TEST_BINARY_DIR}/external-consumer-build")
file(REMOVE_RECURSE "${install_prefix}" "${consumer_source}" "${consumer_build}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" --install "${CHRONOS_TEST_BINARY_DIR}" --prefix "${install_prefix}"
  RESULT_VARIABLE install_result
)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "external consumer staging install failed with status ${install_result}")
endif()

file(MAKE_DIRECTORY "${consumer_source}")
file(WRITE "${consumer_source}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.25)
project(ChronosIngestConsumer LANGUAGES CXX)
find_package(ChronosDB 0.1 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE chronos::cseg chronos::head chronos::ingest chronos::manifest)
target_compile_features(consumer PRIVATE cxx_std_23)
set(consumer_sanitizers "")
if(CHRONOS_TEST_ENABLE_ASAN)
  list(APPEND consumer_sanitizers address)
endif()
if(CHRONOS_TEST_ENABLE_UBSAN)
  list(APPEND consumer_sanitizers undefined)
endif()
if(CHRONOS_TEST_ENABLE_TSAN)
  list(APPEND consumer_sanitizers thread)
endif()
if(consumer_sanitizers)
  list(JOIN consumer_sanitizers "," consumer_sanitizer_flags)
  target_compile_options(consumer PRIVATE "-fsanitize=${consumer_sanitizer_flags}")
  target_link_options(consumer PRIVATE "-fsanitize=${consumer_sanitizer_flags}")
endif()
]=])
file(WRITE "${consumer_source}/main.cpp" [=[
#include <chronos/columnar/columnar_batch.hpp>
#include <chronos/columnar/columnar_batch_codec.hpp>
#include <chronos/columnar/columnar_batch_format.hpp>
#include <chronos/columnar/column_vector.hpp>
#include <chronos/cseg/compression.hpp>
#include <chronos/cseg/format.hpp>
#include <chronos/cseg/inspection.hpp>
#include <chronos/cseg/layout.hpp>
#include <chronos/cseg/metadata_codec.hpp>
#include <chronos/cseg/part_codec.hpp>
#include <chronos/cseg/page_codec.hpp>
#include <chronos/cseg/plain_page.hpp>
#include <chronos/cseg/projected_reader.hpp>
#include <chronos/cseg/types.hpp>
#include <chronos/cseg/validator.hpp>
#include <chronos/head/mutable_head.hpp>
#include <chronos/ingest/columnar_append.hpp>
#include <chronos/ingest/columnar_append_executor.hpp>
#include <chronos/ingest/columnar_append_format.hpp>
#include <chronos/ingest/columnar_append_recovery.hpp>
#include <chronos/ingest/identity.hpp>
#include <chronos/ingest/retry_directory.hpp>
#include <chronos/ingest/sha256.hpp>
#include <chronos/ingest/tablet_state.hpp>
#include <chronos/manifest/format.hpp>
#include <chronos/manifest/checkpoint_builder.hpp>
#include <chronos/manifest/codec.hpp>
#include <chronos/manifest/generation_builder.hpp>
#include <chronos/manifest/layout.hpp>
#include <chronos/manifest/naming.hpp>
#include <chronos/manifest/part_validation.hpp>
#include <chronos/manifest/sealed_head_flush.hpp>
#include <chronos/manifest/storage.hpp>
#include <chronos/manifest/types.hpp>
#include <chronos/manifest/validation.hpp>
#include <chronos/wal/application.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

int main() {
  using ExecuteFunction = chronos::common::Result<chronos::ingest::ColumnarAppendExecutionResult> (*)(
      const chronos::ingest::ColumnarAppendExecutionInput&, chronos::ingest::RetryDirectory&,
      chronos::ingest::TabletState&, chronos::wal::WalCommitCoordinator&);
  const ExecuteFunction execute = &chronos::ingest::execute_columnar_append;
  using RecoverFunction =
      chronos::common::Result<chronos::ingest::RecoveredColumnarAppendState> (*)(
          const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
          chronos::ingest::ColumnarAppendRecoveryConfig);
  const RecoverFunction recover = &chronos::ingest::recover_columnar_append_wal;
  using InspectWalSuffixFunction = chronos::common::Result<chronos::wal::WalRecoveryReport> (*)(
      std::string_view, const chronos::wal::WalReplayCheckpoint&,
      chronos::wal::WalReplaySink&);
  const InspectWalSuffixFunction inspect_wal_suffix = &chronos::wal::inspect_wal_suffix;
  using RecoverWalCheckpointFunction =
      chronos::common::Result<chronos::wal::WalRecoveryReport> (*)(
          const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
          const chronos::wal::WalReplayCheckpoint&, chronos::wal::WalReplaySink&);
  const RecoverWalCheckpointFunction recover_wal_checkpoint =
      &chronos::wal::recover_wal_from_checkpoint;
  using OpenWalCheckpointFunction = chronos::common::Result<chronos::wal::WalWriter> (*)(
      const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
      const chronos::wal::WalReplayCheckpoint&, chronos::wal::WalReplaySink&);
  const OpenWalCheckpointFunction open_wal_checkpoint =
      &chronos::wal::WalWriter::open_existing_from_checkpoint;
  using ReclaimWalFunction = chronos::common::Result<chronos::wal::WalSegmentReclamationReport> (
      chronos::wal::WalWriter::*)(const chronos::wal::WalReplayCheckpoint&);
  const ReclaimWalFunction reclaim_wal =
      &chronos::wal::WalWriter::reclaim_checkpointed_segments;
  using WalReclamationMetricsFunction = chronos::wal::WalSegmentReclamationMetrics (
      chronos::wal::WalWriter::*)() const noexcept;
  const WalReclamationMetricsFunction wal_reclamation_metrics =
      &chronos::wal::WalWriter::reclamation_metrics;
  using RegisterSchemaFunction = chronos::common::Status (chronos::ingest::TabletState::*)(
      std::shared_ptr<const chronos::schema::TableSchema>, chronos::head::MutableHeadCapacity);
  const RegisterSchemaFunction register_schema = &chronos::ingest::TabletState::register_schema;
  static_assert(chronos::columnar::bitmap_size(9U) == 2U);
  static_assert(chronos::columnar::format::kBatchHeaderLength == 96U);
  static_assert(chronos::cseg::format::kFileHeaderLength == 256U);
  const std::array<std::uint64_t, 5> page_lengths{8U, 8U, 8U, 8U, 8U};
  const auto cseg_layout = chronos::cseg::plan_cseg_v1_layout(
      {.user_column_count = 1U, .granule_count = 1U}, page_lengths);
  const std::array<std::byte, 1> page{std::byte{0x41}};
  const auto stored_page =
      chronos::cseg::compress_cseg_page_v1(page, chronos::cseg::PageCompression::kNone);
  const auto cseg_metadata = chronos::cseg::decode_cseg_v1_metadata_prefix({});
  const std::array<std::byte, 1> bool_values{std::byte{1U}};
  const auto physical = chronos::columnar::PhysicalColumnView::create(
      {.type = chronos::schema::LogicalType::create(
                   chronos::schema::LogicalTypeKind::kBool)
                   .value(),
       .nullable = false,
       .row_count = 1U,
       .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = bool_values});
  const auto plain_page = physical.has_value()
                              ? chronos::cseg::encode_cseg_v1_plain_page(*physical)
                              : chronos::common::Result<chronos::cseg::EncodedCsegPlainPage>{
                                    chronos::common::make_unexpected(physical.error())};
  const auto encoded_cseg_page =
      physical.has_value()
          ? chronos::cseg::encode_cseg_v1_page(*physical,
                                               chronos::cseg::PageCompression::kNone)
          : chronos::common::Result<chronos::cseg::EncodedCsegPage>{
                chronos::common::make_unexpected(physical.error())};
  using DecodeCsegPageFunction = chronos::common::Result<chronos::cseg::DecodedCsegPage> (*)(
      chronos::common::ByteView, const chronos::cseg::CsegColumnDescriptor&,
      const chronos::cseg::CsegPageDescriptor&);
  const DecodeCsegPageFunction decode_cseg_page = &chronos::cseg::decode_cseg_v1_page;
  using DecodeCsegPartFunction = chronos::cseg::CsegPartDecodeResult (*)(
      chronos::common::ByteView, chronos::cseg::CsegMetadataDecodeLimits);
  const DecodeCsegPartFunction decode_cseg_part = &chronos::cseg::decode_cseg_v1_part_prefix;
  const auto cseg_part = decode_cseg_part({}, {});
  using ValidateCsegPartFunction = chronos::common::Status (*)(
      const chronos::cseg::DecodedCsegPartView&, chronos::cseg::CsegValidationLimits);
  const ValidateCsegPartFunction validate_cseg_part =
      &chronos::cseg::validate_cseg_v1_part_contents;
  using InspectCsegPartFunction = chronos::cseg::CsegInspectionResult (*)(
      chronos::common::ByteView, chronos::cseg::CsegInspectionLimits);
  const InspectCsegPartFunction inspect_cseg_part = &chronos::cseg::inspect_cseg_v1_part;
  using OpenProjectedReaderFunction = chronos::cseg::CsegProjectedReaderOpenResult (*)(
      chronos::common::ByteView, const chronos::schema::SchemaLineage&,
      chronos::schema::SchemaId, const chronos::schema::TabletId&,
      chronos::cseg::CsegProjectedReaderLimits);
  const OpenProjectedReaderFunction open_projected_reader =
      &chronos::cseg::open_cseg_v1_projected_reader_prefix;
  chronos::columnar::ColumnarBatchLimits limits;
  std::array<std::byte, 0> empty{};
  const auto decoded = chronos::columnar::decode_columnar_batch_v1_exact(empty);
  const auto digest = chronos::ingest::sha256(chronos::common::ByteView{});
  const auto retry_directory = chronos::ingest::RetryDirectory::create(
      chronos::ingest::RetryDirectoryConfig{.maximum_entries = 8U});
  const chronos::head::MutableHeadCapacity head_capacity{.row_capacity = 4U,
                                                        .variable_value_bytes = {}};
  const chronos::ingest::TabletStateConfig tablet_config{
      .head_capacity = head_capacity,
      .maximum_schema_versions = 2U,
      .maximum_sealed_generations = 2U,
      .maximum_retry_entries = 8U};
  static_assert(chronos::ingest::columnar_append_v1::kCommandHeaderLength == 160U);
  static_assert(chronos::manifest::format::kFileHeaderLength == 256U);
  const auto manifest_layout = chronos::manifest::plan_manifest_v1_layout({});
  const auto manifest_name = chronos::manifest::manifest_file_name(1U);
  using DecodeManifestFunction = chronos::manifest::ManifestDecodeResult (*)(
      chronos::common::ByteView, chronos::manifest::ManifestDecodeLimits);
  const DecodeManifestFunction decode_manifest = &chronos::manifest::decode_manifest_v1_exact;
  using ValidateManifestTransitionFunction = chronos::common::Status (*)(
      const chronos::manifest::DecodedManifestView&, const chronos::manifest::DecodedManifestView&,
      std::span<const chronos::manifest::TabletSchemaBinding>);
  const ValidateManifestTransitionFunction validate_manifest_transition =
      &chronos::manifest::validate_manifest_v1_transition;
  using ValidateReferencedPartsFunction = chronos::common::Status (*)(
      const chronos::manifest::DecodedManifestView&,
      std::span<const chronos::manifest::TabletSchemaBinding>,
      std::span<const chronos::manifest::ReferencedPartImage>,
      chronos::manifest::ReferencedPartValidationLimits);
  const ValidateReferencedPartsFunction validate_referenced_parts =
      &chronos::manifest::validate_manifest_v1_referenced_parts;
  using OpenManifestStorageFunction = chronos::common::Result<chronos::manifest::ManifestStorage> (*)(
      const chronos::manifest::ManifestStorageConfig&);
  const OpenManifestStorageFunction open_manifest_storage =
      &chronos::manifest::ManifestStorage::open_existing;
  using ScanManifestNamespaceFunction =
      chronos::common::Result<chronos::manifest::ManifestNamespaceSnapshot> (
          chronos::manifest::ManifestStorage::*)() const;
  const ScanManifestNamespaceFunction scan_manifest_namespace =
      &chronos::manifest::ManifestStorage::scan_namespace;
  using CleanupManifestTemporariesFunction =
      chronos::common::Result<chronos::manifest::TemporaryCleanupReport> (
          chronos::manifest::ManifestStorage::*)();
  const CleanupManifestTemporariesFunction cleanup_manifest_temporaries =
      &chronos::manifest::ManifestStorage::cleanup_temporaries;
  using InstallManifestFunction = chronos::common::Result<chronos::manifest::InstalledManifest> (
      chronos::manifest::ManifestStorage::*)(const chronos::manifest::ManifestInstallRequest&);
  const InstallManifestFunction install_manifest =
      &chronos::manifest::ManifestStorage::install_manifest;
  using ManifestMetricsFunction = chronos::manifest::ManifestInstallationMetrics (
      chronos::manifest::ManifestStorage::*)() const noexcept;
  const ManifestMetricsFunction manifest_metrics =
      &chronos::manifest::ManifestStorage::manifest_metrics;
  using LoadSelectedManifestFunction =
      chronos::common::Result<chronos::manifest::LoadedManifestGeneration> (
          chronos::manifest::ManifestStorage::*)(
          const chronos::manifest::ManifestLoadRequest&) const;
  const LoadSelectedManifestFunction load_selected_manifest =
      &chronos::manifest::ManifestStorage::load_selected_manifest;
  using FlushSealedHeadFunction =
      chronos::common::Result<chronos::manifest::EncodedSealedHeadPart> (*)(
          const chronos::manifest::SealedHeadFlushRequest&);
  const FlushSealedHeadFunction flush_sealed_head = &chronos::manifest::encode_sealed_head_v1;
  using BuildManifestFunction =
      chronos::common::Result<chronos::manifest::EncodedManifest> (*)(
          const chronos::manifest::SealedHeadManifestBuildInput&);
  const BuildManifestFunction build_manifest =
      &chronos::manifest::build_manifest_v1_for_sealed_head;
  using BuildCheckpointFunction =
      chronos::common::Result<chronos::manifest::CheckpointedManifestGeneration> (*)(
          const chronos::manifest::ManifestCheckpointBuildInput&);
  const BuildCheckpointFunction build_checkpoint =
      &chronos::manifest::build_manifest_v1_checkpointed_generation;
  return execute != nullptr && recover != nullptr && inspect_wal_suffix != nullptr &&
                 recover_wal_checkpoint != nullptr && open_wal_checkpoint != nullptr &&
                 reclaim_wal != nullptr && wal_reclamation_metrics != nullptr &&
                 register_schema != nullptr &&
                 limits.max_columns == 4096U &&
                 head_capacity.row_capacity == 4U &&
                 tablet_config.maximum_schema_versions == 2U &&
                 tablet_config.maximum_sealed_generations == 2U && digest.has_value() &&
                 retry_directory.has_value() &&
                 retry_directory->metrics().maximum_entries == 8U && !decoded.has_value() &&
                 cseg_layout.has_value() && cseg_layout->total_length == 1'248U &&
                 manifest_layout.has_value() && manifest_layout->total_length == 264U &&
                 manifest_name.has_value() &&
                 *manifest_name == "manifest-00000000000000000001.cman" &&
                 decode_manifest != nullptr && validate_manifest_transition != nullptr &&
                 validate_referenced_parts != nullptr &&
                 open_manifest_storage != nullptr &&
                 scan_manifest_namespace != nullptr &&
                 cleanup_manifest_temporaries != nullptr &&
                 install_manifest != nullptr && manifest_metrics != nullptr &&
                 load_selected_manifest != nullptr && flush_sealed_head != nullptr &&
                 build_manifest != nullptr && build_checkpoint != nullptr &&
                 stored_page.has_value() && stored_page->bytes().size() == 1U &&
                 plain_page.has_value() && plain_page->bytes().size() == 1U &&
                 encoded_cseg_page.has_value() && encoded_cseg_page->bytes().size() == 1U &&
                 decode_cseg_page != nullptr &&
                 !cseg_part.has_value() &&
                 cseg_part.error().kind() ==
                     chronos::cseg::CsegPartDecodeErrorKind::kIncomplete &&
                 validate_cseg_part != nullptr &&
                 inspect_cseg_part != nullptr &&
                 open_projected_reader != nullptr &&
                 !cseg_metadata.has_value() &&
                 cseg_metadata.error().kind() ==
                     chronos::cseg::CsegMetadataDecodeErrorKind::kIncomplete &&
                 decoded.error().kind() ==
                     chronos::columnar::ColumnarBatchDecodeErrorKind::kIncomplete
             ? 0
             : 1;
}
]=])

set(configure_command
    "${CMAKE_COMMAND}" -S "${consumer_source}" -B "${consumer_build}"
    "-DChronosDB_DIR=${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB"
    "-DCHRONOS_TEST_ENABLE_ASAN=${CHRONOS_TEST_ENABLE_ASAN}"
    "-DCHRONOS_TEST_ENABLE_UBSAN=${CHRONOS_TEST_ENABLE_UBSAN}"
    "-DCHRONOS_TEST_ENABLE_TSAN=${CHRONOS_TEST_ENABLE_TSAN}")
if(DEFINED CHRONOS_TEST_GENERATOR AND NOT CHRONOS_TEST_GENERATOR STREQUAL "")
  list(APPEND configure_command -G "${CHRONOS_TEST_GENERATOR}")
endif()
execute_process(COMMAND ${configure_command} RESULT_VARIABLE configure_result)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer configure failed with status ${configure_result}")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${consumer_build}"
  RESULT_VARIABLE build_result
)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer build failed with status ${build_result}")
endif()
execute_process(
  COMMAND "${consumer_build}/consumer${CHRONOS_TEST_EXECUTABLE_SUFFIX}"
  RESULT_VARIABLE run_result
)
if(NOT run_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer execution failed with status ${run_result}")
endif()
