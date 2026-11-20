"""
5M Trading Bot - High-Frequency Reversal Engine
Accelerated by native Rust extension (`rust_clob`) with ZERO Python fallback.
"""
try:
    import rust_clob
except ImportError as err:
    raise RuntimeError(
        f"CRITICAL: 'rust_clob' native module is mandatory but could not be imported: {err}.\n"
        "All Python fallbacks have been removed. Ensure rust_clob is built and installed."
    ) from err

