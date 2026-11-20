import math
from typing import Dict, Any, Optional

# ─────────────────────────────────────────────────────────────────────────────
#  Underdog Reversal Engine (Merged Strategies A + D + C).
#
#  Targets extreme underdog mispricings (shares priced <= MAX_UNDERDOG_PRICE,
#  typically <= 7¢ / $0.07) with asymmetric 14:1+ upside:
#
#  1. Underdog Price Filter: Only considers sides priced <= max_underdog_price.
#  2. Strategy A (Model Dislocation / EV):
#     - Fair continuous-time probability (GBM) >= min_underdog_prob (e.g. 10%).
#     - Expected Value: EV = fair_prob - ask_price >= min_underdog_ev (e.g. 2¢).
#  3. Strategy D (Volatility Cone Filter):
#     - Z-distance to strike: |spot - strike| / (spot * sigma_1m * sqrt(t)) <= max_sigma_distance.
#     - Ensures the strike is statistically reachable within normal volatility.
#  4. Strategy C (Binance Momentum Reversal Hook):
#     - UP underdog: spot must be ticking UP toward strike (momentum > 0).
#     - DOWN underdog: spot must be ticking DOWN toward strike (momentum < 0).
#     - Prevents catching falling knives before the bounce starts.
# ─────────────────────────────────────────────────────────────────────────────


def _no_trade(reason: str, side=None, prob=None, price=None, ev=None, z_dist=None, momentum=None) -> Dict[str, Any]:
    return {
        "action": "NO_TRADE", "side": side, "phase": "UNDERDOG", "strength": "NONE",
        "reason": reason, "prob": prob, "price": price, "ev": ev,
        "z_dist": z_dist, "momentum": momentum
    }


def calc_z_dist(spot: Optional[float], strike: Optional[float],
                sigma_1m: Optional[float], time_left_min: Optional[float]) -> Optional[float]:
    """Z-score distance: |spot - strike| normalized by expected dollar volatility over time t."""
    if not spot or not strike or not sigma_1m or spot <= 0 or strike <= 0 or sigma_1m <= 0:
        return None
    t = max(float(time_left_min or 0.0), 1.0 / 60.0)
    denom = spot * sigma_1m * math.sqrt(t)
    if denom <= 0:
        return None
    return abs(spot - strike) / denom


def decide_underdog_reversal(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Underdog Reversal Decision: Strategy A + Strategy D + Strategy C."""
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")       # ask (buy) price for the UP share, 0..1
    price_down = inputs.get("priceDown")   # ask (buy) price for the DOWN share, 0..1

    if p_up is None:
        return _no_trade("missing_model_data")

    p_down = 1.0 - p_up
    max_price = float(inputs.get("maxUnderdogPrice", inputs.get("maxPrice", 0.07)))
    min_prob = float(inputs.get("minUnderdogProb", inputs.get("minProb", 0.10)))
    min_ev = float(inputs.get("minUnderdogEv", inputs.get("evThreshold", 0.02)))
    max_sigma_dist = float(inputs.get("maxSigmaDistance", 2.0))
    momentum_filter = bool(inputs.get("momentumFilter", True))

    spot = inputs.get("spotPrice")
    strike = inputs.get("strikePrice")
    sigma_1m = inputs.get("sigma1m")
    time_left_min = inputs.get("timeLeftMin")
    momentum = inputs.get("momentum")  # recent spot price change (positive = up, negative = down)

    z_dist = calc_z_dist(spot, strike, sigma_1m, time_left_min)

    # Filter for candidate underdogs priced <= max_price (e.g. <= 0.07 / 7¢)
    candidates = []

    # Check UP as underdog (market heavily favors DOWN, spot is below strike)
    if price_up is not None and price_up <= max_price:
        ev_up = p_up - price_up
        # UP reversal requires spot to bounce upward toward strike
        mom_up_ok = (momentum is None) or (momentum > 0)
        candidates.append(("UP", p_up, price_up, ev_up, z_dist, mom_up_ok, momentum))

    # Check DOWN as underdog (market heavily favors UP, spot is above strike)
    if price_down is not None and price_down <= max_price:
        ev_down = p_down - price_down
        # DOWN reversal requires spot to drop downward toward strike
        mom_down_ok = (momentum is None) or (momentum < 0)
        candidates.append(("DOWN", p_down, price_down, ev_down, z_dist, mom_down_ok, momentum))

    # If neither side is <= max_price, no underdog entry exists
    if not candidates:
        px_u = f"{price_up*100:.0f}¢" if price_up is not None else "None"
        px_d = f"{price_down*100:.0f}¢" if price_down is not None else "None"
        return _no_trade(f"no_underdog_below_{max_price*100:.0f}¢ (asks: {px_u} UP / {px_d} DOWN)",
                         prob=p_up, z_dist=z_dist, momentum=momentum)

    # Pick the best underdog candidate by highest expected value (EV)
    side, p, price, ev, z, mom_ok, mom = max(candidates, key=lambda c: c[3])

    # ── GATES ──
    # 1. Strategy D: Volatility Cone Gate (Strike must be reachable within max_sigma_dist)
    if z is not None and z > max_sigma_dist:
        return _no_trade(f"{side}_dist_{z:.1f}σ_outside_{max_sigma_dist:.1f}σ_cone",
                         side, p, price, ev, z, mom)

    # 2. Strategy A: Minimum Fair Probability Gate
    if p < min_prob:
        return _no_trade(f"{side}_prob_{p*100:.1f}%_below_{min_prob*100:.1f}%",
                         side, p, price, ev, z, mom)

    # 3. Strategy A: Minimum Expected Value Gate
    if ev < min_ev:
        return _no_trade(f"{side}_ev_{ev*100:.1f}¢_below_{min_ev*100:.1f}¢",
                         side, p, price, ev, z, mom)

    # 4. Strategy C: Binance Momentum Reversal Hook
    if momentum_filter and not mom_ok:
        mom_txt = f"${mom:+.1f}" if mom is not None else "$0"
        return _no_trade(f"{side}_waiting_momentum_bounce ({mom_txt})",
                         side, p, price, ev, z, mom)

    strength = "HIGH_CONVICTION" if p >= 0.20 else "STRONG"
    return {
        "action": "ENTER",
        "side": side,
        "phase": "UNDERDOG",
        "strength": strength,
        "prob": p,
        "price": price,
        "ev": ev,
        "z_dist": z,
        "momentum": mom,
        "reason": "underdog_reversal"
    }


# Backwards compatibility alias
decide_ev = decide_underdog_reversal
