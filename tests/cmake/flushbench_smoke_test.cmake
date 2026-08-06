if(NOT DEFINED CHRONOS_FLUSHBENCH)
  message(FATAL_ERROR "CHRONOS_FLUSHBENCH is required")
endif()
if(NOT DEFINED CHRONOS_TEST_BINARY_DIR)
  message(FATAL_ERROR "CHRONOS_TEST_BINARY_DIR is required")
endif()

set(smoke_root "${CHRONOS_TEST_BINARY_DIR}/flushbench-smoke-test")
file(REMOVE_RECURSE "${smoke_root}")
file(MAKE_DIRECTORY "${smoke_root}")
set(output_directory "${smoke_root}/run")

execute_process(
  COMMAND
    "${CHRONOS_FLUSHBENCH}"
    --output-dir "${output_directory}"
    --flushes 2
    --warmup-flushes 1
    --repetitions 1
    --rows-per-head 8
    --snapshot-readers 1
    --baseline-snapshots 16
    --compression ZSTD
    --maximum-foreground-samples 10000
    --maximum-artifact-bytes 16777216
    --allow-dirty
    --allow-non-release
  RESULT_VARIABLE run_result
  OUTPUT_VARIABLE run_output
  ERROR_VARIABLE run_error
)
if(NOT run_result EQUAL 0)
  message(
    FATAL_ERROR
      "chronos-flushbench smoke run failed (${run_result})\nstdout:\n${run_output}\nstderr:\n${run_error}"
  )
endif()

foreach(artifact IN ITEMS manifest.json raw-flushes.csv raw-foreground-snapshots.csv summary.json)
  if(NOT EXISTS "${output_directory}/${artifact}")
    message(FATAL_ERROR "chronos-flushbench omitted artifact ${artifact}")
  endif()
endforeach()

file(READ "${output_directory}/manifest.json" manifest)
foreach(required IN ITEMS
        "\"validation_status\":\"passed\""
        "\"byte_identical_repeated_manifest_recovery_required\":true"
        "\"complete_repeated_wal_suffix_replay_required\":true")
  string(FIND "${manifest}" "${required}" position)
  if(position EQUAL -1)
    message(FATAL_ERROR "chronos-flushbench manifest omitted ${required}")
  endif()
endforeach()

file(READ "${output_directory}/summary.json" summary)
foreach(required IN ITEMS
        "\"manifest_generation\":4"
        "\"part_count\":3"
        "\"retry_count\":3"
        "\"replayed_records\":4"
        "\"replayed_rows\":32"
        "\"syncs_per_flush\":4.000000")
  string(FIND "${summary}" "${required}" position)
  if(position EQUAL -1)
    message(FATAL_ERROR "chronos-flushbench summary omitted ${required}")
  endif()
endforeach()

file(READ "${output_directory}/raw-flushes.csv" samples)
string(REGEX MATCHALL "\n" sample_newlines "${samples}")
list(LENGTH sample_newlines sample_line_count)
if(NOT sample_line_count EQUAL 3)
  message(FATAL_ERROR "chronos-flushbench emitted ${sample_line_count} flush lines, expected 3")
endif()

foreach(path IN ITEMS
        "repetition-1/database/manifest/manifest-00000000000000000004.cman"
        "repetition-1/database/wal/wal-00000000000000000001.cwal")
  if(NOT EXISTS "${output_directory}/${path}")
    message(FATAL_ERROR "chronos-flushbench omitted verified image ${path}")
  endif()
endforeach()
