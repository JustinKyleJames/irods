set(
  IRODS_LIB_METADATA_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/metadata.cpp"
)

set(
  IRODS_LIB_METADATA_HEADERS_PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/metadata.hpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/irods/entity.hpp"
)

foreach(variant IN ITEMS server client)
  add_library(
    irods_lib_metadata_${variant}
    OBJECT
    "${IRODS_LIB_METADATA_SOURCES}"
    "${IRODS_LIB_METADATA_HEADERS_PUBLIC}"
  )
  objects_link_libraries(
    irods_lib_metadata_${variant}
    PUBLIC
    fmt::fmt
    irods_lib_api_${variant}
    irods_lib_core_${variant}
    irods_filesystem_${variant}
  )
  target_include_directories(
    irods_lib_metadata_${variant}
    PUBLIC
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<BUILD_INTERFACE:${CMAKE_IRODS_BINARY_DIR}/lib/core/include>"
    "$<BUILD_INTERFACE:${CMAKE_IRODS_SOURCE_DIR}/lib/core/include>"
    "$<BUILD_INTERFACE:${CMAKE_IRODS_SOURCE_DIR}/lib/api/include>"
    "$<BUILD_INTERFACE:${CMAKE_IRODS_SOURCE_DIR}/lib/hasher/include>"
    PRIVATE
    "${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
  )
  target_compile_definitions(
    irods_lib_metadata_${variant}
    PUBLIC
    ${IRODS_COMPILE_DEFINITIONS_PUBLIC}
    PRIVATE
    ${IRODS_COMPILE_DEFINITIONS_PRIVATE}
  )
  set_target_properties(irods_lib_metadata_${variant} PROPERTIES INTERFACE_POSITION_INDEPENDENT_CODE TRUE)
  set_target_properties(irods_lib_metadata_${variant} PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
endforeach()

target_include_directories(
  irods_lib_metadata_server
  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_IRODS_SOURCE_DIR}/server/api/include>"
  "$<BUILD_INTERFACE:${CMAKE_IRODS_SOURCE_DIR}/server/core/include>"
)

target_compile_definitions(
  irods_lib_metadata_server
  PUBLIC
  IRODS_METADATA_ENABLE_SERVER_SIDE_API
)

install(
  FILES ${IRODS_LIB_METADATA_HEADERS_PUBLIC}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/irods"
  COMPONENT ${IRODS_PACKAGE_COMPONENT_DEVELOPMENT_NAME}
)
