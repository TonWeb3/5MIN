from typing import List, Optional, Dict, Tuple
import rust_clob

# Below this many minutes left the horizon is treated as effectively settled
MIN_HORIZON_MINUTES = 1.0 / 60.0  # 1 second


def realized_drift_vol(candles: List[Dict], lookback: int = 300):
    """(mean, std) of per-candle log returns — the per-step drift & sigma for the
    fair-prob model. Feed it 1m candles and the result is per-MINUTE.
    Returns (None, None) if there isn't enough data.
    Executed natively via Rust vector math with ZERO Python/NumPy fallback."""
    closes = [float(c["close"]) for c in candles[-lookback:] if c.get("close")]
    if len(closes) < 20:
        return None, None
    return rust_clob.compute_realized_drift_vol(closes, lookback)


def adaptive_vol(candles: List[Dict], lookback: int = 300,
                 half_life: float = 15.0) -> Tuple[Optional[float], Optional[float]]:
    """Fast EWMA 1m volatility blended with the baseline (zero-mean, drift = 0).
    Returns (sigma_blended, sigma_ewma).
    Executed natively via Rust `compute_ewma_vol` with ZERO Python fallback."""
    closes = [float(c["close"]) for c in candles[-lookback:] if c.get("close")]
    if len(closes) < 20:
        return None, None
    return rust_clob.compute_ewma_vol(closes, float(half_life), int(lookback))


def fair_prob_up(current_price: float, strike: float, minutes_left: float,
                 sigma_per_minute: Optional[float], drift_per_minute: float = 0.0) -> float:
    """Closed-form GBM probability that price closes ABOVE `strike` in `minutes_left`
    minutes — the core direction/edge model.
    Executed natively via Rust branchless fast_erfc with ZERO Python fallback."""
    if not current_price or not strike or current_price <= 0 or strike <= 0:
        return 0.5
    if sigma_per_minute is None or sigma_per_minute <= 0:
        return 0.5

    return float(rust_clob.compute_fair_prob(
        float(current_price),
        float(strike),
        float(minutes_left or 0.0),
        float(sigma_per_minute),
        float(drift_per_minute or 0.0),
    ))
