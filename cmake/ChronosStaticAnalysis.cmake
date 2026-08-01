include_guard(GLOBAL)

if(CHRONOS_ENABLE_CLANG_TIDY)
  find_program(
    CHRONOS_CLANG_TIDY_EXECUTABLE
    NAMES clang-tidy clang-tidy-19 clang-tidy-18 clang-tidy-17
  )
  if(NOT CHRONOS_CLANG_TIDY_EXECUTABLE)
    message(FATAL_ERROR "CHRONOS_ENABLE_CLANG_TIDY=ON, but clang-tidy was not found")
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
