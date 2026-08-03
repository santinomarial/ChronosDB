if(NOT DEFINED CHRONOS_WALBENCH)
  message(FATAL_ERROR "CHRONOS_WALBENCH is required")
endif()
if(NOT DEFINED CHRONOS_TEST_BINARY_DIR)
  message(FATAL_ERROR "CHRONOS_TEST_BINARY_DIR is required")
endif()

set(smoke_root "${CHRONOS_TEST_BINARY_DIR}/walbench-smoke-test")
file(REMOVE_RECURSE "${smoke_root}")
file(MAKE_DIRECTORY "${smoke_root}")

foreach(mode IN ITEMS ASYNC LOCAL_SYNC)
  string(TOLOWER "${mode}" mode_lower)
  set(output_directory "${smoke_root}/${mode_lower}")
  execute_process(
    COMMAND
      "${CHRONOS_WALBENCH}"
      --output-dir "${output_directory}"
      --mode "${mode}"
      --operations 32
      --warmup-operations 4
      --repetitions 1
      --producers 2
      --payload-bytes 24
      --target-segment-bytes 256
      --max-pending-requests 2
      --max-pending-bytes 144
      --max-sync-batch-requests 2
      --max-sync-batch-bytes 144
      --max-sync-delay-us 100
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
        "chronos-walbench ${mode} smoke run failed (${run_result})\nstdout:\n${run_output}\nstderr:\n${run_error}"
    )
  endif()

  foreach(artifact IN ITEMS manifest.json raw-latencies.csv summary.json)
    if(NOT EXISTS "${output_directory}/${artifact}")
      message(FATAL_ERROR "chronos-walbench omitted ${mode} artifact ${artifact}")
    endif()
  endforeach()

  file(READ "${output_directory}/manifest.json" manifest)
  string(FIND "${manifest}" "\"requested_mode\":\"${mode}\"" mode_position)
  if(mode_position EQUAL -1)
    message(FATAL_ERROR "chronos-walbench manifest omitted requested ${mode}")
  endif()
  string(FIND "${manifest}" "\"exact_record_sequence_required\":true" sequence_position)
  if(sequence_position EQUAL -1)
    message(FATAL_ERROR "chronos-walbench manifest omitted recovery verification metadata")
  endif()

  file(READ "${output_directory}/raw-latencies.csv" samples)
  string(REGEX MATCHALL "\n" sample_newlines "${samples}")
  list(LENGTH sample_newlines sample_line_count)
  if(NOT sample_line_count EQUAL 33)
    message(FATAL_ERROR "chronos-walbench emitted ${sample_line_count} lines, expected 33")
  endif()
endforeach()
