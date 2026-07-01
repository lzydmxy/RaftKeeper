set(LIBRARY_DIR "${RaftKeeper_SOURCE_DIR}/contrib/zstd/lib")

set(Sources
    "${LIBRARY_DIR}/common/debug.c"
    "${LIBRARY_DIR}/common/entropy_common.c"
    "${LIBRARY_DIR}/common/error_private.c"
    "${LIBRARY_DIR}/common/fse_decompress.c"
    "${LIBRARY_DIR}/common/pool.c"
    "${LIBRARY_DIR}/common/threading.c"
    "${LIBRARY_DIR}/common/xxhash.c"
    "${LIBRARY_DIR}/common/zstd_common.c"
    "${LIBRARY_DIR}/compress/fse_compress.c"
    "${LIBRARY_DIR}/compress/hist.c"
    "${LIBRARY_DIR}/compress/huf_compress.c"
    "${LIBRARY_DIR}/compress/zstd_compress.c"
    "${LIBRARY_DIR}/compress/zstd_compress_literals.c"
    "${LIBRARY_DIR}/compress/zstd_compress_sequences.c"
    "${LIBRARY_DIR}/compress/zstd_compress_superblock.c"
    "${LIBRARY_DIR}/compress/zstd_double_fast.c"
    "${LIBRARY_DIR}/compress/zstd_fast.c"
    "${LIBRARY_DIR}/compress/zstd_lazy.c"
    "${LIBRARY_DIR}/compress/zstd_ldm.c"
    "${LIBRARY_DIR}/compress/zstdmt_compress.c"
    "${LIBRARY_DIR}/compress/zstd_opt.c"
    "${LIBRARY_DIR}/decompress/huf_decompress.c"
    "${LIBRARY_DIR}/decompress/zstd_ddict.c"
    "${LIBRARY_DIR}/decompress/zstd_decompress_block.c"
    "${LIBRARY_DIR}/decompress/zstd_decompress.c"
    "${LIBRARY_DIR}/dictBuilder/cover.c"
    "${LIBRARY_DIR}/dictBuilder/divsufsort.c"
    "${LIBRARY_DIR}/dictBuilder/fastcover.c"
    "${LIBRARY_DIR}/dictBuilder/zdict.c"
)

# ponytail: skip huf_decompress_amd64.S; needs enable_language(ASM) and the C
# fallback is already compiled. Worth ~5% huffman decode throughput.
add_library(_zstd ${Sources})
add_library(rk_contrib::zstd ALIAS _zstd)
target_include_directories(_zstd SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}")
target_include_directories(_zstd SYSTEM PRIVATE "${LIBRARY_DIR}/common")
# UBSan trips on some legitimate zstd bit tricks.
target_compile_options(_zstd PRIVATE -fno-sanitize=undefined -w)
# The .S file provides asm huffman decode; without it, tell the C code to use the pure-C path.
target_compile_definitions(_zstd PRIVATE ZSTD_DISABLE_ASM)
