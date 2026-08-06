include_guard(GLOBAL)

if(CHRONOS_ENABLE_CLANG_TIDY)
  find_program(
    CHRONOS_CLANG_TIDY_EXECUTABLE
    NAMES clang-tidy-18
    PATHS /opt/homebrew/opt/llvm@18/bin /usr/local/opt/llvm@18/bin
  )
  if(NOT CHRONOS_CLANG_TIDY_EXECUTABLE)
    message(FATAL_ERROR "CHRONOS_ENABLE_CLANG_TIDY=ON, but clang-tidy 18 was not found")
  endif()
  execute_process(
    COMMAND "${CHRONOS_CLANG_TIDY_EXECUTABLE}" --version
    RESULT_VARIABLE CHRONOS_CLANG_TIDY_VERSION_RESULT
    OUTPUT_VARIABLE CHRONOS_CLANG_TIDY_VERSION_OUTPUT
    ERROR_VARIABLE CHRONOS_CLANG_TIDY_VERSION_ERROR
  )
  if(
    NOT CHRONOS_CLANG_TIDY_VERSION_RESULT EQUAL 0
    OR NOT CHRONOS_CLANG_TIDY_VERSION_OUTPUT MATCHES "version[ \t]+18\\."
  )
    message(
      FATAL_ERROR
        "ChronosDB requires clang-tidy 18.x, found: ${CHRONOS_CLANG_TIDY_VERSION_OUTPUT}${CHRONOS_CLANG_TIDY_VERSION_ERROR}"
    )
  endif()
endif()

function(chronos_enable_static_analysis target)
  if(CHRONOS_ENABLE_CLANG_TIDY)
    set_target_properties(
      "${target}"
      PROPERTIES
        CXX_CLANG_TIDY "${CHRONOS_CLANG_TIDY_EXECUTABLE};--config-file=${PROJECT_SOURCE_DIR}/.clang-tidy"
    )
  endif()
endfunction()
