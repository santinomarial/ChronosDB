include_guard(GLOBAL)

function(chronos_set_project_warnings target)
  if(MSVC)
    target_compile_options("${target}" PRIVATE /W4 /permissive-)
    if(CHRONOS_WARNINGS_AS_ERRORS)
      target_compile_options("${target}" PRIVATE /WX)
    endif()
    return()
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    target_compile_options(
      "${target}"
      PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wsign-conversion
        -Wshadow
        -Wformat=2
        -Wundef
        -Wnon-virtual-dtor
        -Wold-style-cast
        -Woverloaded-virtual
    )
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      target_compile_options("${target}" PRIVATE -Wduplicated-cond -Wduplicated-branches -Wlogical-op)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
      target_compile_options("${target}" PRIVATE -Wextra-semi -Wimplicit-fallthrough)
    endif()
    if(CHRONOS_WARNINGS_AS_ERRORS)
      target_compile_options("${target}" PRIVATE -Werror)
    endif()
  endif()
endfunction()
