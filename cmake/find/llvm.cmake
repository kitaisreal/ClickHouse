if (APPLE OR SPLIT_SHARED_LIBRARIES OR NOT ARCH_AMD64 OR SANITIZE STREQUAL "undefined")
    set (ENABLE_EMBEDDED_COMPILER OFF CACHE INTERNAL "")
endif()

option (ENABLE_EMBEDDED_COMPILER "Enable support for 'compile_expressions' option for query execution" ON)

if (NOT ENABLE_EMBEDDED_COMPILER)
    set (USE_EMBEDDED_COMPILER 0)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/llvm/llvm/CMakeLists.txt")
    message (${RECONFIGURE_MESSAGE_LEVEL} "submodule /contrib/llvm is missing. to fix try run: \n git submodule update --init --recursive")
endif ()

set (USE_EMBEDDED_COMPILER 1)

set (LLVM_FOUND 1)
set (LLVM_VERSION "12.0.0bundled")
set (LLVM_INCLUDE_DIRS
    "${ClickHouse_SOURCE_DIR}/contrib/llvm/llvm/include"
    "${ClickHouse_BINARY_DIR}/contrib/llvm/llvm/include"
)
set (LLVM_LIBRARY_DIRS "${ClickHouse_BINARY_DIR}/contrib/llvm/llvm")

message(STATUS "LLVM include Directory: ${LLVM_INCLUDE_DIRS}")
message(STATUS "LLVM library Directory: ${LLVM_LIBRARY_DIRS}")
message(STATUS "LLVM C++ compiler flags: ${LLVM_CXXFLAGS}")

# This list was generated by listing all LLVM libraries, compiling the binary and removing all libraries while it still compiles.
set (REQUIRED_LLVM_LIBRARIES
LLVMOrcJIT
LLVMExecutionEngine
LLVMRuntimeDyld
LLVMAsmPrinter
LLVMDebugInfoDWARF
LLVMGlobalISel
LLVMSelectionDAG
LLVMMCDisassembler
LLVMPasses
LLVMCodeGen
LLVMipo
LLVMBitWriter
LLVMInstrumentation
LLVMScalarOpts
LLVMAggressiveInstCombine
LLVMInstCombine
LLVMVectorize
LLVMTransformUtils
LLVMTarget
LLVMAnalysis
LLVMProfileData
LLVMObject
LLVMBitReader
LLVMCore
LLVMRemarks
LLVMBitstreamReader
LLVMMCParser
LLVMMC
LLVMBinaryFormat
LLVMDebugInfoCodeView
LLVMSupport
LLVMDemangle
)

if (ARCH_AMD64)
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMX86Info LLVMX86Desc LLVMX86CodeGen)
elseif (ARCH_AARCH64)
    list(APPEND REQUIRED_LLVM_LIBRARIES LLVMAArch64Info LLVMAArch64Desc LLVMAArch64CodeGen)
endif ()

#function(llvm_libs_all REQUIRED_LLVM_LIBRARIES)
#    llvm_map_components_to_libnames (result all)
#    if (USE_STATIC_LIBRARIES OR NOT "LLVM" IN_LIST result)
#        list (REMOVE_ITEM result "LTO" "LLVM")
#    else()
#        set (result "LLVM")
#    endif ()
#    list (APPEND result ${CMAKE_DL_LIBS} ${ZLIB_LIBRARIES})
#    set (${REQUIRED_LLVM_LIBRARIES} ${result} PARENT_SCOPE)
#endfunction()
