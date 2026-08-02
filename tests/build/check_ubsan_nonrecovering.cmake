if(NOT DEFINED COMPILE_COMMANDS OR NOT EXISTS "${COMPILE_COMMANDS}")
  message(FATAL_ERROR "compile_commands.json is required to verify UBSan configuration")
endif()

file(READ "${COMPILE_COMMANDS}" compile_commands)
string(FIND "${compile_commands}" "-fno-sanitize-recover=undefined" flag_offset)
if(flag_offset EQUAL -1)
  message(FATAL_ERROR "ChronosDB UBSan targets permit recovery after undefined behavior")
endif()
