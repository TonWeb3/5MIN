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

_REQUIRED_RUST_SYMBOLS = (
    "init_client",
    "post_order",
    "compute_fair_prob",
    "compute_realized_drift_vol",
    "compute_ewma_vol",
    "fast_parse_binance_trade_flow",
    "fast_walk_asks",
    "fast_walk_bids",
    "BinanceTradeTracker",
    "OrderBookTracker",
    "PriceHistoryTracker",
)
_missing = [s for s in _REQUIRED_RUST_SYMBOLS if not hasattr(rust_clob, s)]
if _missing:
    raise RuntimeError(
        f"CRITICAL: 'rust_clob' is outdated and missing required native symbols: {_missing}.\n"
        "All Python fallbacks have been removed. Rebuild and reinstall the rust_clob crate."
    )


