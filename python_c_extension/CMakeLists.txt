cmake_minimum_required(VERSION 3.16)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(GRAFC_MODULE_NAME _grafc)
set(SOURCES graf.cpp)


find_package(Python COMPONENTS Interpreter Development)
if(NOT Python_FOUND)
    message(FATAL_ERROR "Python not found")
endif()


add_library(grafc SHARED ${SOURCES})
target_link_libraries(grafc PRIVATE Python::Module)


if(WIN32)
    set(PYTHON_PLATFORM_TAG win_amd64)
else()
    set(PYTHON_PLATFORM_TAG linux_x86_64)
endif()

set(PYTHON_ABI_TAG cp${Python_VERSION_MAJOR}${Python_VERSION_MINOR})

# Set variable if its x86 or x64
if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    if(WIN32)
        set(PYTHON_PLATFORM_TAG win_amd64)
    else()
        set(PYTHON_PLATFORM_TAG linux_x86_64)
    endif()
else()
    if(WIN32)
        set(PYTHON_PLATFORM_TAG win_x86)
    else()
        set(PYTHON_PLATFORM_TAG linux_x86)
    endif()
endif()

# Set variable if its a debug target
if(CMAKE_BUILD_TYPE MATCHES Debug)
    set(PYTHON_BUILD_TYPE _d)
else()
    set(PYTHON_BUILD_TYPE "")
endif()


set_target_properties(
    grafc
    PROPERTIES
        PREFIX ""
        OUTPUT_NAME ${GRAFC_MODULE_NAME}${PYTHON_BUILD_TYPE}.${PYTHON_ABI_TAG}-${PYTHON_PLATFORM_TAG}
        LINKER_LANGUAGE C
)

if(WIN32)
    set_target_properties(
        grafc
        PROPERTIES
        SUFFIX ".pyd"
    )
endif()


