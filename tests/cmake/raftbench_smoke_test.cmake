if(NOT DEFINED CHRONOS_RAFTBENCH)
  message(FATAL_ERROR "CHRONOS_RAFTBENCH is required")
endif()
if(NOT DEFINED CHRONOS_TEST_BINARY_DIR)
  message(FATAL_ERROR "CHRONOS_TEST_BINARY_DIR is required")
endif()

set(smoke_root "${CHRONOS_TEST_BINARY_DIR}/raftbench-smoke-test")
file(REMOVE_RECURSE "${smoke_root}")
file(MAKE_DIRECTORY "${smoke_root}")

foreach(mode IN ITEMS APPEND_ONLY LOCAL_SYNC)
  string(TOLOWER "${mode}" mode_lower)
  set(output_directory "${smoke_root}/${mode_lower}")
  execute_process(
    COMMAND
      "${CHRONOS_RAFTBENCH}"
      --output-dir "${output_directory}"
      --mode "${mode}"
      --operations 16
      --warmup-operations 8
      --repetitions 1
      --batch-records 4
      --payload-bytes 32
      --logical-groups 4
      --target-segment-bytes 65536
      --maximum-artifact-bytes 1048576
      --allow-dirty
      --allow-non-release
    RESULT_VARIABLE run_result
    OUTPUT_VARIABLE run_output
    ERROR_VARIABLE run_error
  )
  if(NOT run_result EQUAL 0)
    message(
      FATAL_ERROR
        "chronos-raftbench ${mode} smoke run failed (${run_result})\nstdout:\n${run_output}\nstderr:\n${run_error}"
    )
  endif()

  foreach(artifact IN ITEMS manifest.json raw-latencies.csv summary.json)
    if(NOT EXISTS "${output_directory}/${artifact}")
      message(FATAL_ERROR "chronos-raftbench omitted ${mode} artifact ${artifact}")
    endif()
  endforeach()

  file(READ "${output_directory}/manifest.json" manifest)
  string(JSON manifest_mode GET "${manifest}" raft_log requested_mode)
  string(JSON validation_status GET "${manifest}" correctness validation_status)
  string(JSON exact_reopen GET "${manifest}" correctness exact_reopen_required)
  if(NOT "${manifest_mode}" STREQUAL "${mode}" OR NOT "${validation_status}" STREQUAL "passed" OR
     NOT exact_reopen)
    message(FATAL_ERROR "chronos-raftbench ${mode} manifest failed its smoke contract")
  endif()

  file(READ "${output_directory}/summary.json" summary)
  if(mode STREQUAL "LOCAL_SYNC")
    set(expected_sync_calls 4)
  else()
    set(expected_sync_calls 0)
  endif()
  string(JSON measured_sync_calls GET "${summary}" repetitions 0
              measured_explicit_synchronize_calls)
  if(NOT measured_sync_calls EQUAL expected_sync_calls)
    message(FATAL_ERROR
            "chronos-raftbench ${mode} summary omitted ${expected_sync_calls} measured sync calls")
  endif()

  file(READ "${output_directory}/raw-latencies.csv" samples)
  string(REGEX MATCHALL "\n" sample_newlines "${samples}")
  list(LENGTH sample_newlines sample_line_count)
  if(NOT sample_line_count EQUAL 5)
    message(FATAL_ERROR "chronos-raftbench emitted ${sample_line_count} lines, expected 5")
  endif()
endforeach()
