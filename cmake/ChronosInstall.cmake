include_guard(GLOBAL)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

function(chronos_configure_installation)
  install(
    TARGETS
      chronos_common
      chronos_schema
      chronos_columnar
      chronos_io
      chronos_wal
      chronosctl
      chronos-waldump
      chronos-walbench
    EXPORT ChronosTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )
  install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  install(
    FILES "${CHRONOS_GENERATED_INCLUDE_DIR}/chronos/common/version_config.hpp"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/chronos/common"
  )

  configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/ChronosConfig.cmake.in"
    "${PROJECT_BINARY_DIR}/ChronosDBConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ChronosDB"
  )
  write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/ChronosDBConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion
  )
  install(
    FILES
      "${PROJECT_BINARY_DIR}/ChronosDBConfig.cmake"
      "${PROJECT_BINARY_DIR}/ChronosDBConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ChronosDB"
  )
  install(
    EXPORT ChronosTargets
    FILE ChronosTargets.cmake
    NAMESPACE chronos::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ChronosDB"
  )
endfunction()
