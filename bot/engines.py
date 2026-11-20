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


def _no_trade(reason: str, side=None, prob=None, price=None, ev=None, z_dist=None,
              momentum=None, flow_imbalance=None) -> Dict[str, Any]:
    return {
        "action": "NO_TRADE", "side": side, "phase": "UNDERDOG", "strength": "NONE",
        "reason": reason, "prob": prob, "price": price, "ev": ev,
        "z_dist": z_dist, "momentum": momentum, "flow_imbalance": flow_imbalance
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
    """Underdog Reversal Decision: Time-Window Gate + Strategy A + Strategy D + Strategy C (Aggressor Flow)."""
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")       # ask (buy) VWAP for the UP share, 0..1
    price_down = inputs.get("priceDown")   # ask (buy) VWAP for the DOWN share, 0..1

    if p_up is None:
        return _no_trade("missing_model_data")

    p_down = 1.0 - p_up
    max_price = float(inputs.get("maxUnderdogPrice", inputs.get("maxPrice", 0.07)))
    min_prob = float(inputs.get("minUnderdogProb", inputs.get("minProb", 0.10)))
    min_ev = float(inputs.get("minUnderdogEv", inputs.get("evThreshold", 0.02)))
    max_sigma_dist = float(inputs.get("maxSigmaDistance", 2.0))
    min_time_left = float(inputs.get("minTimeLeftMin", 0.75))
    max_time_left = float(inputs.get("maxTimeLeftMin", 3.5))
    momentum_filter = bool(inputs.get("momentumFilter", True))
    min_bounce_sigma = float(inputs.get("minBounceSigma", 0.15))
    min_flow_imbalance = float(inputs.get("minFlowImbalance", 0.10))

    spot = inputs.get("spotPrice")
    strike = inputs.get("strikePrice")
    sigma_1m = inputs.get("sigma1m")
    time_left_min = inputs.get("timeLeftMin")
    momentum = inputs.get("momentum")  # recent spot price change (positive = up, negative = down)
    flow = inputs.get("flowMetrics") or {}
    flow_imb = flow.get("flow_imbalance")

    z_dist = calc_z_dist(spot, strike, sigma_1m, time_left_min)

    # ── 0. Time-Window Gate ──
    if time_left_min is not None:
        if min_time_left > 0 and time_left_min < min_time_left:
            return _no_trade(f"too_late_{time_left_min*60:.0f}s_left (<{min_time_left*60:.0f}s)",
                             prob=p_up, z_dist=z_dist, momentum=momentum, flow_imbalance=flow_imb)
        if max_time_left > 0 and time_left_min > max_time_left:
            return _no_trade(f"too_early_{time_left_min:.1f}m_left (>{max_time_left:.1f}m)",
                             prob=p_up, z_dist=z_dist, momentum=momentum, flow_imbalance=flow_imb)

    # Filter for candidate underdogs priced <= max_price (e.g. <= 0.07 / 7¢)
    candidates = []

    if price_up is not None and price_up <= max_price:
        ev_up = p_up - price_up
        candidates.append(("UP", p_up, price_up, ev_up))

    if price_down is not None and price_down <= max_price:
        ev_down = p_down - price_down
        candidates.append(("DOWN", p_down, price_down, ev_down))

    if not candidates:
        px_u = f"{price_up*100:.0f}¢" if price_up is not None else "None"
        px_d = f"{price_down*100:.0f}¢" if price_down is not None else "None"
        return _no_trade(f"no_underdog_below_{max_price*100:.0f}¢ (asks: {px_u} UP / {px_d} DOWN)",
                         prob=p_up, z_dist=z_dist, momentum=momentum, flow_imbalance=flow_imb)

    # Pick the best underdog candidate by highest expected value (EV)
    side, p, price, ev = max(candidates, key=lambda c: c[3])

    # ── GATES ──
    # 1. Strategy D: Volatility Cone Gate (Strike must be reachable within max_sigma_dist)
    if z_dist is not None and z_dist > max_sigma_dist:
        return _no_trade(f"{side}_dist_{z_dist:.1f}σ_outside_{max_sigma_dist:.1f}σ_cone",
                         side, p, price, ev, z_dist, momentum, flow_imb)

    # 2. Strategy A: Minimum Fair Probability Gate
    if p < min_prob:
        return _no_trade(f"{side}_prob_{p*100:.1f}%_below_{min_prob*100:.1f}%",
                         side, p, price, ev, z_dist, momentum, flow_imb)

    # 3. Strategy A: Minimum Expected Value Gate
    if ev < min_ev:
        return _no_trade(f"{side}_ev_{ev*100:.1f}¢_below_{min_ev*100:.1f}¢",
                         side, p, price, ev, z_dist, momentum, flow_imb)

    # 4. Strategy C: Aggressor-Flow + Multi-Window Momentum Reversal Hook
    if momentum_filter:
        min_bounce_usd = (float(spot) * float(sigma_1m) * min_bounce_sigma) if (spot and sigma_1m and min_bounce_sigma > 0) else 0.0
        has_flow = isinstance(flow, dict) and ("mom_fast" in flow)
        if has_flow:
            mom_fast = float(flow.get("mom_fast") or 0.0)
            mom_slow = float(flow.get("mom_slow") or 0.0)
            imb = float(flow.get("flow_imbalance") or 0.0)
            total_vol = float(flow.get("buy_vol_usd") or 0.0) + float(flow.get("sell_vol_usd") or 0.0)
            bounce = float(flow.get("bounce_from_low") or 0.0) if side == "UP" else float(flow.get("drop_from_high") or 0.0)

            curl_ok = (mom_fast > 0 and mom_slow >= 0) if side == "UP" else (mom_fast < 0 and mom_slow <= 0)
            if not curl_ok:
                return _no_trade(f"{side}_waiting_momentum_curl (fast {mom_fast:+.1f} / slow {mom_slow:+.1f})",
                                 side, p, price, ev, z_dist, momentum, imb)
            if min_bounce_usd > 0 and bounce < min_bounce_usd:
                return _no_trade(f"{side}_bounce_${bounce:.1f}_below_${min_bounce_usd:.1f}",
                                 side, p, price, ev, z_dist, momentum, imb)
            if total_vol > 0 and min_flow_imbalance > 0:
                flow_ok = (imb >= min_flow_imbalance) if side == "UP" else (imb <= -min_flow_imbalance)
                if not flow_ok:
                    return _no_trade(f"{side}_waiting_aggressor_flow ({imb:+.2f})",
                                     side, p, price, ev, z_dist, momentum, imb)
        else:
            mom_ok = (momentum is None) or (momentum > 0 if side == "UP" else momentum < 0)
            if not mom_ok:
                mom_txt = f"${momentum:+.1f}" if momentum is not None else "$0"
                return _no_trade(f"{side}_waiting_momentum_bounce ({mom_txt})",
                                 side, p, price, ev, z_dist, momentum, flow_imb)

    strength = "HIGH_CONVICTION" if p >= 0.20 else "STRONG"
    return {
        "action": "ENTER",
        "side": side,
        "phase": "UNDERDOG",
        "strength": strength,
        "prob": p,
        "price": price,
        "ev": ev,
        "z_dist": z_dist,
        "momentum": momentum,
        "flow_imbalance": flow_imb,
        "reason": "underdog_reversal"
    }


# Backwards compatibility alias
decide_ev = decide_underdog_reversal
