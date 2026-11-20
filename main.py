import asyncio
import time
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
import bot.polybolt as polybolt
from bot.clob_trader import clob_trader

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load previous state
    load_state()

    # Initial seeding
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(clob_book_stream.start()),
        asyncio.create_task(clob_user_stream.start()),
        asyncio.create_task(polybolt_feed.run_forever()),
        asyncio.create_task(chainlink_refresh_loop()),
        asyncio.create_task(redeem_loop()),
        asyncio.create_task(update_loop())
    ]
    log_message(polybolt.status_line(settings.PRICE_STREAM))
    if settings.PRIVATE_KEY:
        async def _warm_key():
            try:
                await asyncio.to_thread(clob_trader.ensure_ready)
            except Exception as e:
                log_message(f"Wallet init: {e}")
        asyncio.create_task(_warm_key())

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    polybolt_feed.closed = True

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    chainlink_ws_stream.close()
    clob_book_stream.close()
    clob_user_stream.close()

app = FastAPI(title="Polymarket BTC 5m Assistant", lifespan=lifespan)
# Prefixes every downloaded file (console log / closed-trades CSV) so a user running
# several bots can tell which one a download came from and store it correctly.
STRATEGY_LABEL = "5m-base"
templates = Jinja2Templates(directory="templates")

# Global state to store the latest data
state = {
    "latest_data": {},
    "last_update_ts": 0,
    "trading_mode": settings.MODE,
    "paper_balance": settings.PAPER_BALANCE_USD,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "last_trade_side": None,
    "last_balance_refresh": 0,
    "running": False,   # trading is OFF until the user presses Start on the dashboard
    # Auto-withdrawal (capital extractor) state machine:
    #   ARMED -> (balance>=trigger) WAITING_FLAT -> (no open trades) WITHDRAWING
    #         -> WITHDRAW_SUBMITTED -> ARMED (+ resume)
    "withdraw_state": "ARMED",
    "last_withdrawal": None,   # {"amount","tx","time"} of the most recent withdrawal
    # After a take-profit / stop-loss close, lock this market id so we don't re-enter
    # until the next 5m window starts.
    "tp_sl_locked_market": None,
    # Per-window OPEN prices ("Price to Beat"), keyed by window start ms:
    #   {start_ms: {"chainlink": float|None, "close": float|None, "source": str|None}}
    # `chainlink` holds the Polymarket crypto_prices value read AT eventStartTime, `close`
    # the same feed's value at the window end. Used to settle trades the way Polymarket
    # does (close vs open, same feed on both ends).
    "market_opens": {},
    "last_window_start": None,
    # Live wins awaiting redemption into pUSD (drained by redeem_loop).
    "pending_redemptions": []
}

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"]
        }
        with open("state_data.json", "w") as f:
            json.dump(data_to_save, f, indent=2)
            
        # Only persist the PAPER bankroll. In live mode `paper_balance` mirrors the real
        # on-chain pUSD balance, and writing that into config would overwrite the user's
        # configured paper starting balance with a live account number.
        if state["trading_mode"] == "paper" and os.path.exists("config.json"):
            with open("config.json", "r") as f:
                cfg = json.load(f)
            cfg["paper_balance_usd"] = state["paper_balance"]
            with open("config.json", "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        if os.path.exists("state_data.json"):
            with open("state_data.json", "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                log_message("State loaded from state_data.json")
    except Exception as e:
        print(f"Error loading state: {e}")

def log_message(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)
    # Persist the FULL console log to a file so the whole history can be downloaded
    # (the in-memory list above is capped at 100 for the live view).
    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/console.log", "a", encoding="utf-8") as _lf:
            _lf.write(f"{datetime.now():%Y-%m-%d} {formatted}\n")
    except Exception:
        pass

clob_trader.set_log_hook(log_message)

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# Event-driven tick wakeup: Binance trades & CLOB book updates set this immediately
# so update_loop reacts in milliseconds instead of waiting for POLL_INTERVAL_MS.
_tick_wakeup: Optional[asyncio.Event] = None

def _trigger_tick() -> None:
    if _tick_wakeup is not None:
        _tick_wakeup.set()

# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_event=_trigger_tick)
# 1m is the VOLATILITY base: fast EWMA vol + baseline are measured per-minute here and the
# fair-prob horizon scales as sqrt(minutes_left) with zero drift. Must hold >= VOL_LOOKBACK_1M candles.
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=500)
# 5m is the WINDOW base: each candle's open IS the 5m market's open (same boundaries).
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
# Live CLOB order books, pushed. Replaces REST /book polling on the critical path.
clob_book_stream = ws_data.ClobBookStream(on_event=_trigger_tick)
# Authenticated user channel — pushes OUR fills so `shares` is the venue's number and not
# our estimate. Idles until live mode has a key (the provider returns None in paper).
clob_user_stream = ws_data.ClobUserStream(
    creds_provider=lambda: clob_trader.get_api_creds() if settings.MODE == "live" else None
)

# ── Price Stream (Polymarket ws-live-v2 TWAP and live reference price) ───────
def _ps_creds():
    """The CLOB API credentials derived from the key, for the price stream socket's login. None without a key."""
    if not settings.PRIVATE_KEY or not clob_trader.ensure_ready():
        return None
    c = getattr(clob_trader.clob, "creds", None)
    if not c or not getattr(c, "key", None):
        return None
    return {"apiKey": str(c.key), "secret": str(c.secret), "passphrase": str(c.passphrase)}


def _ps_use(what: str) -> bool:
    return polybolt.use(settings.PRICE_STREAM, what)


def _ps_tol() -> float:
    return float((settings.PRICE_STREAM or {}).get("pick_tolerance_s") or 2.0)


def _ps_on_live(ts: float, price: float) -> None:
    pass


polybolt_feed = polybolt.PolyBolt(
    get_creds=_ps_creds,
    enabled=lambda: polybolt.on(settings.PRICE_STREAM),
    log=lambda m: log_message(m),
    symbol=polybolt.symbol_for(settings.SYMBOL),
    proxy=ws_data.get_proxy_url_for(polybolt.URL) or None,
    on_live=_ps_on_live,
)
_ps_noted: set = set()
_ps_lag_said = [0.0]
POLY_WS_MAX_AGE_MS = 2500
MARK_CAPTURE_WINDOW_MS = 20_000


def _ps_live_as_poly_ws(poly_ws: Dict[str, Any]) -> Dict[str, Any]:
    """update_loop: with "prices" on, Polymarket's live Chainlink price from the price socket, in the shape of
    the old Polymarket feed's last price — while it is fresh: it arrived, and is stamped, within
    POLY_WS_MAX_AGE_MS. Otherwise poly_ws as it is (the bot then goes on with its other feeds, as before)."""
    if not _ps_use("prices"):
        return poly_ws
    live = polybolt_feed.live(POLY_WS_MAX_AGE_MS / 1000.0)
    if not live:
        return poly_ws
    behind_ms = (time.time() - live[0]) * 1000.0
    if behind_ms > POLY_WS_MAX_AGE_MS:
        if time.time() - _ps_lag_said[0] > 600:
            _ps_lag_said[0] = time.time()
            log_message(f"PRICE STREAM: its live price is stamped {behind_ms / 1000.0:.1f}s behind this computer's clock — "
                        "not used as the bot's price (the bot keeps its own feed)")
        return poly_ws
    age_s = time.time() - polybolt_feed.live_at
    return {"price": live[1], "updatedAt": live[0] * 1000.0, "age": age_s, "source": "polymarket_live_stream"}


def _ps_strike(start_ms: int, strike_open: Optional[float], strike_source: str):
    """update_loop: the window's strike. With "prices" on and Polymarket's 60-second average for the window's
    start published: that average, which is the price to beat. Else the bot's own, as before."""
    if not _ps_use("prices"):
        return strike_open, strike_source
    ptb = polybolt_feed.twap_at(start_ms / 1000.0, _ps_tol())
    return (ptb, "polymarket_twap") if ptb is not None else (strike_open, strike_source)


def _ps_window_note(start_ms: int, window_ms: int, own_strike: Optional[float]) -> None:
    """update_loop: one log line per window once Polymarket's average for its start is in: the window's price
    to beat next to the bot's own strike, and how the window before it closed on these numbers."""
    if start_ms in _ps_noted or not polybolt.on(settings.PRICE_STREAM):
        return
    pt = polybolt_feed.twap_point_at(start_ms / 1000.0, _ps_tol())
    if pt is None or (own_strike is None and time.time() * 1000 - start_ms < MARK_CAPTURE_WINDOW_MS):
        return
    if len(_ps_noted) > 200:
        _ps_noted.clear()
    _ps_noted.add(start_ms)
    avg, late = pt[1], pt[0] - start_ms / 1000.0
    label = datetime.fromtimestamp(start_ms / 1000).strftime('%H:%M')
    text = (f"PRICE STREAM: Polymarket's 60-second average at {label}" + (f" (stamped {late:.1f}s after it)" if late >= 0.05 else "")
            + f" is {avg:,.2f}: the {label} window's price to beat.")
    if own_strike:
        gap = own_strike - avg
        text += f" The bot's own strike: {own_strike:,.2f} (${abs(gap):,.2f} {'higher' if gap >= 0 else 'lower'})."
    before = polybolt_feed.twap_at((start_ms - window_ms) / 1000.0, _ps_tol())
    if before is not None:
        prev = datetime.fromtimestamp((start_ms - window_ms) / 1000).strftime('%H:%M')
        text += (f" On these numbers the {prev} window closed {'UP' if avg >= before else 'DOWN'}: "
                 f"${abs(avg - before):,.2f} {'above' if avg >= before else 'below'} its price to beat {before:,.2f}.")
    log_message(text)


def _ps_settle_call(trade: Dict[str, Any]):
    """update_trades: (winner, closing average, opening average) on Polymarket's own 60-second averages, with
    "settle" on and both published; None otherwise (the bot then compares its own two prices, as before)."""
    start, end = trade.get("window_start_ms"), trade.get("end_ts")
    if not _ps_use("settle") or not start or not end:
        return None
    start, end = float(start) / 1000.0, float(end)
    dur = end - start
    if not (240 <= dur <= 360 or 840 <= dur <= 960 or abs(dur - settings.CANDLE_WINDOW_MINUTES * 60) <= 2.0):
        return None
    opened = polybolt_feed.twap_at(start, _ps_tol())
    closed = polybolt_feed.twap_at(end, _ps_tol())
    if opened is None or closed is None:
        return None
    return ("UP" if closed >= opened else "DOWN"), closed, opened

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

# The ACTIVE market only changes once per 5-minute window, but the order books change
# constantly. Resolving the market via Gamma on every tick would be a wasted request at a
# 500ms poll, so cache it and re-resolve only when it expires (or every MARKET_CACHE_TTL_S
# as a safety net). The books/prices below are still fetched fresh every tick — that is
# where the latency edge lives.
MARKET_CACHE_TTL_S = 10.0
_market_cache: Dict[str, Any] = {"market": None, "fetched_at": 0.0, "end_ms": 0.0}


async def resolve_active_market() -> Optional[Dict[str, Any]]:
    now_ms = time.time() * 1000
    cached = _market_cache["market"]
    if (cached is not None and now_ms < _market_cache["end_ms"]
            and (time.time() - _market_cache["fetched_at"]) < MARKET_CACHE_TTL_S):
        return cached

    market = None
    try:
        if settings.POLYMARKET_SLUG:
            market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
        elif settings.POLYMARKET_AUTO_SELECT_LATEST:
            events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
            markets = data.flatten_event_markets(events)

            # The soonest window that hasn't ended yet — i.e. the 5m market trading right now.
            live_markets = [m for m in markets if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now_ms]
            if live_markets:
                live_markets.sort(key=lambda x: x["endDate"])
                market = live_markets[0]
    except Exception as e:
        # A transient Gamma failure must not cost the whole tick. The market IDENTITY only
        # changes once per 5 minutes, so a slightly stale-but-unexpired cached market is far
        # better than no market at all (no book, no EV, no exit marking). Only the identity
        # is reused — the books and prices are still fetched fresh every tick.
        if cached is not None and now_ms < _market_cache["end_ms"]:
            log_message(f"Gamma lookup failed ({type(e).__name__}) — reusing cached market for this window")
            return cached
        raise

    if market:
        end_ms = 0.0
        if market.get("endDate"):
            try:
                end_ms = datetime.fromisoformat(market["endDate"].replace('Z', '+00:00')).timestamp() * 1000
            except Exception:
                end_ms = 0.0
        _market_cache.update({"market": market, "fetched_at": time.time(), "end_ms": end_ms})
    return market


async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = await resolve_active_market()

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # ── The book: pushed over WebSocket, REST only as a fallback ────────────────
    # Polling /book was the tick-rate floor (~2.8s). The whole thesis is that we see a
    # move before the book reprices, so a seconds-old view of the book defeats the point.
    # The WS carries a live snapshot + level deltas; we read it with zero network cost.
    clob_book_stream.set_assets([up_token_id, down_token_id])
    up_live = clob_book_stream.is_live(up_token_id)
    down_live = clob_book_stream.is_live(down_token_id)

    empty = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None,
             "askLiquidity": None, "askLevels": [], "bidLevels": []}

    if up_live and down_live:
        up_book_summary = clob_book_stream.get_book_summary(up_token_id) or dict(empty)
        down_book_summary = clob_book_stream.get_book_summary(down_token_id) or dict(empty)
        book_source = "ws"
    else:
        # Not connected yet, or the market just rolled and we're resubscribing.
        book_source = "rest"
        try:
            up_book, down_book = await asyncio.gather(
                data.fetch_order_book(up_token_id),
                data.fetch_order_book(down_token_id)
            )
        except Exception:
            up_book = down_book = None
        up_book_summary = data.summarize_order_book(up_book) if up_book else dict(empty)
        down_book_summary = data.summarize_order_book(down_book) if down_book else dict(empty)
        if up_book is None and down_book is None:
            book_source = "none"

    # ENTRY prices = the BEST ASK, i.e. what we actually PAY to buy the share.
    #
    # These feed the EV gate (EV = fair - price), so they must be the ask. They used to
    # come from CLOB /price?side=buy, but that endpoint returns the best BID (verified
    # live: side=buy -> 0.84 while the book's best ask was 0.85) — so every edge was
    # overstated by the full spread, and we'd enter trades that were not actually
    # EV-positive once the real fill price was paid. On a 5m market, where a 1c spread is
    # a quarter of the 4c EV threshold, that is the difference between an edge and a leak.
    #
    # Reading the ask straight off the book we already fetch is both correct and two
    # fewer round trips per tick. Exits/marks still use bestBid from `orderbook` below
    # (what we'd RECEIVE selling), which was already right.
    # The window's OPEN time ("Price to Beat" is fixed at this second). Polymarket gives
    # it as eventStartTime; fall back to the aligned endDate - window if absent.
    event_start_ms = None
    est = market.get("eventStartTime")
    if est:
        try:
            event_start_ms = datetime.fromisoformat(est.replace('Z', '+00:00')).timestamp() * 1000
        except Exception:
            event_start_ms = None
    if event_start_ms is None and market.get("endDate"):
        try:
            end_ms = datetime.fromisoformat(market["endDate"].replace('Z', '+00:00')).timestamp() * 1000
            event_start_ms = end_ms - settings.CANDLE_WINDOW_MINUTES * 60_000
        except Exception:
            event_start_ms = None

    return {
        "ok": True,
        "market": market,
        "book_source": book_source,
        "event_start_ms": event_start_ms,
        "prices": {
            "up": up_book_summary["bestAsk"] if up_book_summary["bestAsk"] is not None else gamma_yes,
            "down": down_book_summary["bestAsk"] if down_book_summary["bestAsk"] is not None else gamma_no
        },
        # Midpoints = the real-time mark Polymarket's site shows (matches the % on the page).
        "mids": {
            "up": up_book_summary.get("mid"),
            "down": down_book_summary.get("mid")
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        }
    }


def compute_stake(balance: float) -> float:
    """Dollars to put at risk on one trade: RISK_VALUE% of balance, or a flat RISK_VALUE."""
    if (settings.RISK_TYPE or "percent").lower() == "fixed":
        return float(settings.RISK_VALUE)
    return (float(settings.RISK_VALUE) / 100.0) * float(balance or 0.0)


def effective_entry(orderbook: Dict[str, Any], side: str, stake_usd: float,
                    fallback_price: Optional[float]) -> Dict[str, Any]:
    """The price we'd ACTUALLY pay to put `stake_usd` into `side`, by walking the asks.

    The EV gate must run on this, not on the best ask: an "edge" that exists only for the
    first few shares at the top of the book is not an edge for the size we actually trade.
    Returns the blended VWAP plus how much of the stake the book can absorb.
    """
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    levels = ob.get("askLevels") or []
    if not levels or not stake_usd or stake_usd <= 0:
        return {"vwap": fallback_price, "shares": None, "fillable_usd": None,
                "worst": fallback_price, "walked": False}
    w = data.walk_asks(levels, stake_usd)
    if not w["vwap"]:
        return {"vwap": fallback_price, "shares": None, "fillable_usd": 0.0,
                "worst": fallback_price, "walked": False}
    # `worst` = the deepest ask the fill has to reach. This, not the VWAP or the best ask,
    # is what a live marketable limit must be priced against.
    return {"vwap": w["vwap"], "shares": w["shares"], "fillable_usd": w["filled_usd"],
            "worst": w["worst_price"], "walked": True}

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], target_open: float, token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None, strike_source: Optional[str] = None):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    # No Polymarket/Chainlink open for this window → do NOT open. We can't score the
    # trade against a real open, so we wait for the next window where we mark it.
    if target_open is None:
        return "no_open_price"

    side = decision["side"]

    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    # ── Risk per trade, priced by WALKING THE BOOK ──────────────────────────────
    # No flat price cap (EV already governs reward/risk). RISK_TYPE sizes the stake
    # ("percent" of balance, or a flat "fixed" dollar amount).
    #
    # The fill is then priced by walking the ask levels. Taking `stake / best_ask` (the
    # old behaviour) assumed the whole stake cleared at the top of the book, which on a
    # thin 5m book overstates both the share count and the edge — and does so worst on
    # exactly the trades that look most attractive. `walk_asks` returns the true blended
    # price and how much of the stake the book can actually absorb.
    balance = state["paper_balance"]
    stake = compute_stake(balance)
    if stake <= 0:
        return "stake_zero"

    eff = effective_entry(orderbook, side, stake, price)
    if not eff.get("walked"):
        return "no_book"   # no levels to price against — never guess a fill

    # The stake is EXACTLY the configured risk per trade — never trimmed to fit the book
    # and never scaled up. If the ask side cannot absorb the whole stake, we don't take a
    # smaller position: we don't take the trade at all.
    fillable = eff["fillable_usd"] or 0.0
    if fillable + 1e-6 < stake:
        log_message(f"Skip {side}: book absorbs only ${fillable:.2f} of the ${stake:.2f} stake")
        return "thin_book"
    if fillable < settings.MIN_BOOK_LIQUIDITY_USD:
        log_message(f"Skip {side}: thin book (${fillable:.2f} on the ask)")
        return "thin_book"

    amount_to_risk = stake                 # always the full risk per trade
    shares = eff["shares"] or 0.0
    fill_price = eff["vwap"]

    if not fill_price or shares <= 0:
        return "no_price"
    if balance < amount_to_risk:
        print(f"Insufficient paper balance ({balance}) for risk amount ({amount_to_risk})")
        return "insufficient_balance"

    slippage = (fill_price - price) if price else 0.0
    if settings.MAX_ENTRY_SLIPPAGE > 0 and slippage > settings.MAX_ENTRY_SLIPPAGE + 1e-9:
        log_message(f"Skip {side}: slippage {slippage*100:.2f}¢ > max {settings.MAX_ENTRY_SLIPPAGE*100:.2f}¢ "
                    f"(ask {price*100:.1f}¢ -> VWAP {fill_price*100:.1f}¢ for ${stake:.2f} stake)")
        return "slippage_too_high"

    # Snapshot the immediate liquidation bid at entry so stop-loss measures real
    # adverse movement rather than firing instantly on a wide underdog bid-ask spread.
    held_ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    entry_bid_walk = data.walk_bids(held_ob.get("bidLevels") or [], shares)
    entry_bid = entry_bid_walk.get("vwap") or held_ob.get("bestBid")
    if entry_bid_walk.get("vwap") and (entry_bid_walk.get("sold_shares") or 0.0) + 1e-6 >= shares:
        entry_liquidation_usd = entry_bid_walk.get("proceeds")
    elif entry_bid:
        entry_liquidation_usd = entry_bid * shares
    else:
        entry_liquidation_usd = None

    raw_tick = (market.get("orderPriceMinTickSize")
                or market.get("minimum_tick_size")
                or market.get("tick_size"))
    tick_size = clob_trader._normalize_tick_size(raw_tick) or "0.01"
    neg_risk = bool(market.get("negRisk") or market.get("neg_risk") or False)

    end_date_str = market.get("endDate")
    end_ts = 0
    if end_date_str:
        try:
            end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
        except: pass
    # Fallback so a trade always has a definite expiry even if endDate is missing/unparseable
    if not end_ts:
        end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

    trade = {
        "market_id": market["id"],
        "market_slug": market.get("slug"),
        # Kept at entry so a win can be REDEEMED into pUSD later without re-resolving the
        # market (which may have rolled). Redemption is what turns a live win into
        # spendable balance; see maybe_redeem_settled().
        "condition_id": market.get("conditionId") or market.get("condition_id"),
        "neg_risk": neg_risk,
        "tick_size": tick_size,
        "side": side,
        # The held side's CLOB token id, stored at entry so a manual/live close can always
        # sell it without re-deriving from the current snapshot (robust if the window rolled).
        "token_id": (token_ids or {}).get("up" if side == "UP" else "down"),
        "entry_price": fill_price,      # blended VWAP actually paid, not the best ask
        "best_ask_at_entry": price,     # top of book, for slippage attribution
        "entry_bid": entry_bid,         # bid VWAP at entry, for spread-adjusted SL
        "entry_liquidation_usd": entry_liquidation_usd,
        "peak_bid": entry_bid or 0.0,   # highest bid VWAP seen while open (for trailing stop)
        "slippage": slippage,
        "amount": amount_to_risk,
        "original_amount": amount_to_risk,
        "recovered_usd": 0.0,           # proceeds already banked via Tier 1 free-roll
        "free_rolled": False,
        "shares": shares,
        "entry_ts": time.time(),
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": target_open,
        # Which venue feed marked the Price to Beat at eventStartTime. Settlement reads the
        # close from THIS SAME feed so the two ends of the window can't straddle the ~0.13%
        # gap between the venue's USDT-quoted and Chainlink BTC/USD prices.
        "strike_source": strike_source,
        "open_reason": decision.get("reason", "entry"),  # "ev_enter" (EV signal) or "flip_entry" (flip)
        "end_ts": end_ts,
        # Window this trade belongs to (aligned 5m boundary) — lets a flip-closed
        # trade be scored against the market's true open/close after the window ends.
        "window_start_ms": int(end_ts * 1000) - settings.CANDLE_WINDOW_MINUTES * 60_000,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        open_why = "FLIP" if decision.get("reason") == "flip_entry" else "EV signal"
        log_message(f"Executed PAPER trade [{open_why}]: {side} @ {fill_price:.3f} VWAP "
                    f"(ask {price:.2f}, slip {slippage*100:+.1f}¢) {market.get('slug')} "
                    f"(${amount_to_risk:.2f}, {shares:.1f} sh)")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        # Quote the order against the WORST ask the stake has to reach, not the best ask.
        # The limit becomes worst + CLOB_MAX_SLIPPAGE, so an order sized by walking the
        # book can actually clear the levels it was sized against. Quoting off the top of
        # book got the FOK killed on exactly the thin books this sizing exists to handle.
        marketable_quote = eff.get("worst") or price
        result = await asyncio.to_thread(
            clob_trader.place_market_buy,
            token_id,
            amount_to_risk,
            marketable_quote,
            trade["neg_risk"],
            trade["tick_size"],
            settings.CLOB_ORDER_TYPE,
            settings.CLOB_POST_ONLY,
        )
        if result.get("ok"):
            resp = result.get("response") or {}
            order_id = result.get("order_id")
            if not order_id and isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            trade["order_id"] = order_id
            trade["order_response"] = resp
            trade["quote_at_entry"] = marketable_quote

            # A FOK fixes the USDC spent, not the share count. Where the venue reports the
            # real fill, use it — `shares` drives both the exit SELL size and the
            # `shares x $1` payout, and an estimate that is too high makes the exit
            # unfillable. Otherwise keep the walked estimate and let the user-channel
            # stream reconcile it (see reconcile_live_fills()).
            if result.get("filled_shares"):
                trade["shares"] = result["filled_shares"]
                trade["entry_price"] = result.get("fill_price") or trade["entry_price"]
                trade["shares_source"] = "order_response"
            else:
                trade["shares_source"] = "estimate"

            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            # Debit the mirrored balance immediately. It is refreshed from chain only every
            # ~10s, so without this the stake is counted twice (still in cash AND in the
            # position's mark) until the next refresh. Deliberately do NOT force a chain
            # re-read here: it would race the settlement of our own fill and could read the
            # pre-trade balance, undoing this debit. The normal cadence corrects it.
            state["paper_balance"] = max(0.0, state["paper_balance"] - amount_to_risk)
            state["last_balance_refresh"] = time.time()
            save_state()
            open_why = "FLIP" if decision.get("reason") == "flip_entry" else "EV signal"
            log_message(f"Executed LIVE trade [{open_why}]: {side} ${amount_to_risk:.2f} on {market.get('slug')} "
                        f"(order {order_id}, {trade['shares']:.2f} sh via {trade['shares_source']})")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(fair_up: Optional[float], poly_snapshot: Dict[str, Any], time_left_min: Optional[float],
                              strike_open: Optional[float], strike_source: Optional[str] = None):
    """Close the open position early and flip to the side favoured by FAIR PROBABILITY.

    EV is NOT considered here — the flip is driven purely by the model's fair
    probability. Opt-in (FLIP_ENABLED). Guards: the favoured side's fair prob must
    clear FLIP_MIN_CONVICTION, at least FLIP_MIN_MINUTES_LEFT must remain, we only flip
    within the same market, and we must have this window's Chainlink open. This
    function both CLOSES the old side and OPENS the new one (the normal execute_trade
    path is EV-gated and would refuse to re-enter).

    Every skip is logged (once per reason-change) so it's always visible WHY a flip
    that "should" have happened did not.
    """
    def _skip(reason: str):
        if state.get("last_flip_skip") != reason:
            state["last_flip_skip"] = reason
            log_message(f"FLIP skipped: {reason}")

    if not settings.FLIP_ENABLED:
        return
    if not state["active_trades"] or fair_up is None:
        return

    trade = state["active_trades"][0]

    # Side favoured purely by the fair-probability model (no EV).
    new_side = "UP" if fair_up >= 0.5 else "DOWN"
    new_prob = fair_up if new_side == "UP" else (1.0 - fair_up)

    # Only interesting once the model favours the OPPOSITE side of what we hold.
    if trade["side"] == new_side:
        state["last_flip_skip"] = None  # back on the held side — reset
        return

    # ONE FLIP PER MARKET: if this position is itself the result of a flip, hold it
    # to settlement — never flip again. (Analysis showed the flip *trigger* is usually
    # right, but repeated flipping/churn bleeds the edge on spread + late re-entries.)
    if trade.get("open_reason") == "flip_entry":
        _skip("already flipped once this market — holding to settlement")
        return

    if new_prob < settings.FLIP_MIN_CONVICTION:
        _skip(f"{new_side} conviction {new_prob:.2f} < {settings.FLIP_MIN_CONVICTION:.2f}")
        return
    if strike_open is None:
        _skip("no open (Price to Beat) captured for this window")
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        _skip(f"only {time_left_min:.1f}m left < {settings.FLIP_MIN_MINUTES_LEFT:.0f}m required")
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    if str(trade.get("market_id")) != str(market.get("id")):
        _skip("position is in a prior market (window rolled)")
        return

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    # Selling WALKS THE BIDS — valuing the whole position at the best bid overstates what
    # we'd receive. If the bid side can't absorb the full position we don't flip at all.
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])
    exit_price = ex["vwap"]
    if not exit_price or ex["sold_shares"] + 1e-6 < trade["shares"]:
        _skip(f"bids can't absorb the {trade['side']} position")
        return
    exit_proceeds = ex["proceeds"]

    state["last_flip_skip"] = None  # clearing — we're about to flip

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        # Quote against the LOWEST bid the sale has to reach, not the blended VWAP.
        result = await asyncio.to_thread(
            clob_trader.place_market_sell,
            token_id,
            trade["shares"],
            ex.get("worst_price") or exit_price,
            trade.get("neg_risk", False),
            trade.get("tick_size"),
        )
        if not result.get("ok"):
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')}")
            return
        # live balance is refreshed from chain elsewhere
    else:
        state["paper_balance"] += exit_proceeds  # what walking the bids actually returns

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["settlement_price_at_expiry"] = exit_price
    orig_amount = float(trade.get("original_amount") or trade["amount"])
    recovered = float(trade.get("recovered_usd") or 0.0)
    trade["profit_loss"] = (recovered + exit_proceeds) - orig_amount
    # The realized P/L above is the early-exit sell. Record the market OPEN now; the
    # CLOSE + whether this side actually won gets backfilled once the window ends
    # (see the backfill pass in update_loop). This is why a flip can book a loss even
    # though the market later resolves in this side's favour.
    trade["open_price"] = trade.get("strike_price")
    trade["exit_mark"] = exit_price  # price we sold out at (not the market close)
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side} (fair prob {new_prob:.2f})")

    # Open the new side immediately — driven by FAIR PROBABILITY, not EV. A synthetic
    # ENTER decision reuses execute_trade's sizing / liquidity / live-order logic.
    flip_decision = {"action": "ENTER", "side": new_side, "phase": "FLIP", "strength": "FLIP",
                     "prob": new_prob, "reason": "flip_entry"}
    res = await execute_trade(flip_decision, prices, market, strike_open, token_ids, orderbook, strike_source)
    # Report the flip as a flip ONLY if the new side actually opened. execute_trade can
    # refuse (thin book, no strike, stake > balance), which leaves us flat — reporting
    # "flipped_to_UP" then would claim a position that does not exist.
    if res != "entered":
        log_message(f"FLIP incomplete: closed {trade['side']} but could not open {new_side} ({res}) — now flat")
        return {"side": new_side, "opened": False, "reason": res}
    return {"side": new_side, "opened": True, "reason": res}


async def maybe_tp_sl(poly_snapshot: Dict[str, Any]):
    """Take-profit / stop-loss early exit. Closes the open position when its unrealized
    P/L (marked at the current bid) reaches +TAKE_PROFIT_PCT or -STOP_LOSS_PCT of the
    stake. After it fires, the market is locked so we don't re-enter until the next
    5m window. Returns "take_profit"/"stop_loss" if it closed, else None."""
    if not settings.TP_SL_ENABLED or not state["active_trades"]:
        return None
    if not poly_snapshot.get("ok"):
        return None

    trade = state["active_trades"][0]
    market = poly_snapshot["market"]
    if str(trade.get("market_id")) != str(market.get("id")):
        return None  # position is in a prior market — let it settle on its own

    orderbook = poly_snapshot.get("orderbook", {})
    token_ids = poly_snapshot.get("token_ids", {})
    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    # What we'd actually RECEIVE selling now: walk the BIDS for the whole position. Using
    # the best bid for every share would overstate the exit and fire TP on a sale we can't
    # actually get. If the bids can't absorb the position, there is no exit to take.
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])
    exit_price = ex["vwap"]
    if not exit_price or ex["sold_shares"] + 1e-6 < trade["shares"]:
        return None

    amount = trade["amount"]
    orig_amount = float(trade.get("original_amount") or amount)
    recovered = float(trade.get("recovered_usd") or 0.0)
    unreal = (recovered + ex["proceeds"]) - orig_amount
    pl_pct = (unreal / orig_amount) * 100.0 if orig_amount else 0.0

    # Track peak bid for Tier 2 trailing stop
    peak_bid = max(float(trade.get("peak_bid") or 0.0), float(exit_price))
    trade["peak_bid"] = peak_bid

    tp_cents = getattr(settings, "TAKE_PROFIT_CENTS", settings.TAKE_PROFIT_PCT)

    # ── Tier 1: Free-Roll Partial Take Profit ───────────────────────────────────
    # Sell FREE_ROLL_SELL_FRAC (default 50%) when bid reaches >= max(mult * entry, min_cents),
    # recovering initial stake so the remaining shares become a risk-free runner.
    if (settings.FREE_ROLL_ENABLED
            and not trade.get("free_rolled")
            and (tp_cents <= 0 or (exit_price * 100.0) < tp_cents)):
        fr_target = max(
            float(trade["entry_price"]) * float(settings.FREE_ROLL_MULT),
            float(settings.FREE_ROLL_MIN_CENTS) / 100.0,
        )
        if exit_price >= fr_target:
            sell_frac = min(0.9, max(0.1, float(settings.FREE_ROLL_SELL_FRAC)))
            sell_shares = float(trade["shares"]) * sell_frac
            fr_ex = data.walk_bids(ob.get("bidLevels") or [], sell_shares)
            if fr_ex.get("vwap") and (fr_ex.get("sold_shares") or 0.0) + 1e-6 >= sell_shares:
                fr_price = fr_ex["vwap"]
                fr_proceeds = fr_ex["proceeds"]
                if state["trading_mode"] == "live":
                    token_id = token_ids.get(held_key)
                    fr_res = await asyncio.to_thread(
                        clob_trader.place_market_sell,
                        token_id,
                        sell_shares,
                        fr_ex.get("worst_price") or fr_price,
                        trade.get("neg_risk", False),
                        trade.get("tick_size"),
                    )
                    if not fr_res.get("ok"):
                        log_message(f"FREE ROLL sell FAILED ({trade['side']}): {fr_res.get('error')}")
                        return None
                    if fr_res.get("filled_shares"):
                        sell_shares = float(fr_res["filled_shares"])
                    if fr_res.get("filled_usdc"):
                        fr_proceeds = float(fr_res["filled_usdc"])
                else:
                    state["paper_balance"] += fr_proceeds

                trade["shares"] = max(0.0, float(trade["shares"]) - sell_shares)
                trade["recovered_usd"] = recovered + fr_proceeds
                trade["free_rolled"] = True
                save_state()
                log_message(
                    f"FREE ROLL (Tier 1): sold {sell_shares:.1f} {trade['side']} sh @ {fr_price*100:.1f}¢ "
                    f"(banked ${fr_proceeds:.2f} vs ${orig_amount:.2f} stake); {trade['shares']:.1f} sh riding risk-free"
                )
                return "free_roll"

    # Spread-adjusted stop-loss basis: on thin underdog books, the bid-ask spread alone
    # can exceed STOP_LOSS_PCT on tick 0. Anchor SL drawdown to min(stake, entry_liquidation)
    # and enforce a post-entry grace window so SL only fires on real adverse price moves.
    entry_ts = trade.get("entry_ts")
    if entry_ts is None and trade.get("entry_time"):
        try:
            entry_ts = datetime.fromisoformat(trade["entry_time"]).timestamp()
        except Exception:
            entry_ts = 0.0
    held_s = (time.time() - float(entry_ts)) if entry_ts else float("inf")
    sl_grace_s = float(getattr(settings, "STOP_LOSS_GRACE_S", 15.0) or 0.0)

    entry_liq = trade.get("entry_liquidation_usd")
    if entry_liq is None and trade.get("entry_bid"):
        entry_liq = float(trade["entry_bid"]) * float(trade.get("shares") or 0.0)
    sl_base = min(amount, float(entry_liq)) if (entry_liq is not None and float(entry_liq) > 0) else amount
    sl_drawdown_pct = ((ex["proceeds"] - sl_base) / sl_base) * 100.0 if sl_base else 0.0

    hit = None
    # Strategy B (Convexity Scalp): Final TP in CENTS per share, plus Peak Trailing Bid
    if tp_cents > 0 and (exit_price * 100.0) >= tp_cents:
        hit = "take_profit"
    elif (settings.TRAILING_STOP_ENABLED
          and (peak_bid * 100.0) >= float(settings.TRAILING_ACTIVATE_CENTS)
          and ((peak_bid - exit_price) * 100.0) >= float(settings.TRAILING_DROP_CENTS) - 1e-9):
        hit = "trailing_stop"
    elif (not trade.get("free_rolled")
          and settings.STOP_LOSS_PCT > 0
          and held_s >= sl_grace_s
          and pl_pct <= -settings.STOP_LOSS_PCT + 1e-9
          and sl_drawdown_pct <= -settings.STOP_LOSS_PCT + 1e-9):
        hit = "stop_loss"
    if not hit:
        return None

    # Close the position at the bid.
    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        # Quote against the LOWEST bid the sale has to reach (see place_market_sell).
        result = await asyncio.to_thread(
            clob_trader.place_market_sell,
            token_id,
            trade["shares"],
            ex.get("worst_price") or exit_price,
            trade.get("neg_risk", False),
            trade.get("tick_size"),
        )
        if not result.get("ok"):
            log_message(f"{hit.upper()} sell FAILED ({trade['side']}): {result.get('error')}")
            return None
    else:
        state["paper_balance"] += ex["proceeds"]   # walked bids, not best-bid × shares

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = hit
    trade["settlement_price_at_expiry"] = exit_price
    trade["profit_loss"] = unreal
    trade["open_price"] = trade.get("strike_price")
    trade["exit_mark"] = exit_price
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    # Lock this market so no re-entry until the next 5m window.
    state["tp_sl_locked_market"] = str(market.get("id"))
    save_state()
    label = ("TAKE PROFIT (Strategy B Scalp)" if hit == "take_profit"
             else (f"TRAILING BID STOP (peak {peak_bid*100:.1f}¢)" if hit == "trailing_stop" else "STOP LOSS"))
    log_message(f"{label}: closed {trade['side']} @ {exit_price*100:.1f}¢ (P/L ${unreal:.2f}, {pl_pct:+.1f}%); locked market until next window")
    return hit


async def close_active_trade_now() -> Dict[str, Any]:
    """Manually close the open position RIGHT NOW at the current bid, then lock the active
    market so nothing re-enters until the next 5m window (same lock TP/SL uses).

    Unlike TP/SL this is a forced exit: it closes even when the bid side can't absorb the
    whole position. In LIVE that means a real FOK sell — if the book is too thin the venue
    kills it and we report the failure (nothing was sold, so nothing is locked). In PAPER
    we book the walked-bid proceeds for whatever the bids absorb and mark any unsellable
    remainder at the last bid we saw (conservative), so the position always ends flat.
    """
    if not state["active_trades"]:
        return {"ok": False, "error": "no_active_trade"}

    trade = state["active_trades"][0]
    held_key = "up" if trade["side"] == "UP" else "down"

    # Fresh book for the trade's own market (may differ from the active one if the window
    # just rolled). We resolve the ACTIVE market separately, to lock the right window.
    snapshot = await fetch_polymarket_snapshot()
    active_market_id = str(snapshot["market"].get("id")) if snapshot.get("ok") else None

    same_market = snapshot.get("ok") and str(trade.get("market_id")) == str(snapshot["market"].get("id"))
    ob = (snapshot.get("orderbook", {}) or {}).get(held_key) or {} if same_market else {}
    ex = data.walk_bids(ob.get("bidLevels") or [], trade["shares"])

    # Exit mark: the walked-bid VWAP if we have one, else the last marked bid, else entry.
    exit_price = ex["vwap"] or trade.get("cur_bid") or trade.get("entry_price")
    if not exit_price or exit_price <= 0:
        return {"ok": False, "error": "no_exit_price"}

    if state["trading_mode"] == "live":
        # Prefer the token id stored at entry (always correct); fall back to the snapshot.
        token_id = trade.get("token_id") or (snapshot.get("token_ids", {}) if same_market else {}).get(held_key)
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        # Quote against the LOWEST bid we'd have to reach; on a forced close the walk may
        # not cover the whole size, so fall back to the marked exit price.
        result = await asyncio.to_thread(
            clob_trader.place_market_sell,
            token_id,
            trade["shares"],
            ex.get("worst_price") or exit_price,
            trade.get("neg_risk", False),
            trade.get("tick_size"),
        )
        if not result.get("ok"):
            log_message(f"MANUAL CLOSE sell FAILED ({trade['side']}): {result.get('error')}")
            return {"ok": False, "error": result.get("error", "sell_failed")}
        proceeds = ex["proceeds"] if ex["vwap"] else trade["shares"] * exit_price
    else:
        # Paper: real proceeds for the absorbed portion + remainder marked at the exit price.
        remainder = max(0.0, trade["shares"] - ex["sold_shares"])
        proceeds = ex["proceeds"] + remainder * exit_price
        state["paper_balance"] += proceeds

    orig_amount = float(trade.get("original_amount") or trade["amount"])
    recovered = float(trade.get("recovered_usd") or 0.0)
    unreal = (recovered + proceeds) - orig_amount
    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "manual"
    trade["settlement_price_at_expiry"] = exit_price
    trade["exit_mark"] = exit_price
    trade["profit_loss"] = unreal
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = trade.get("close_price")
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    # Lock the ACTIVE window (what a new entry would open into) so nothing re-enters until
    # the window rolls. If we can't resolve the active market, fall back to the trade's.
    state["tp_sl_locked_market"] = active_market_id or str(trade.get("market_id"))
    save_state()
    log_message(f"MANUAL CLOSE: {trade['side']} @ {exit_price:.3f} (P/L ${unreal:+.2f}); "
                f"locked market until next 5m window")
    return {"ok": True, "side": trade["side"], "exit_price": exit_price, "profit_loss": unreal}


async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here so open and close come from the SAME feed (no cross-feed offset
    # can flip a near-the-money result). Binance spot is only a last-resort fallback.
    #
    # `settle_price_fresh` says whether that feed is actually alive. A dead feed keeps
    # returning its LAST value forever, and settling close-vs-strike against a frozen
    # number silently mis-resolves every near-the-money window in the same direction. When
    # it is stale we simply don't use the price-based fallback; the trade waits for the
    # venue's own resolution (or the grace-period void), which is always the safer answer.
    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    settle_price_fresh = bool(current_prices.get("chainlink_fresh"))
    # If still unresolvable this long past expiry, void it. Kept under one 5m window so a
    # stuck trade can never block the slot across two consecutive markets.
    SETTLEMENT_GRACE_SECONDS = 120

    for trade in state["active_trades"]:
        # Keep a rolling price snapshot so settlement always has a recent value,
        # even if the feed drops out exactly at expiry.
        if cur_price:
            trade["last_price"] = cur_price

        # Effective window end. If endDate was missing at entry (end_ts == 0), derive
        # it from entry_time + window so a trade can never wait forever.
        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts

        # Freeze the CLOSE the instant the window ends, from the SAME feed as the open:
        # the Polymarket crypto_prices value AT end_ts (the authoritative settlement second).
        # This mirrors how Polymarket resolves — close-vs-open on one feed — so a near-the-
        # money result can't be flipped by cross-feed offset or post-expiry drift. Falls
        # back to the last price we saw only if the feed has no sample at that second.
        # The lookup is PINNED to the feed that marked this window's open (`strike_source`).
        # The venue publishes a USDT-quoted price and a Chainlink BTC/USD price ~0.13% apart;
        # closing against a different feed than the one that set the strike would inject that
        # whole offset into the result and flip near-the-money windows on its own.
        if expired and trade.get("close_price") is None:
            at_close = polymarket_ws_stream.price_at(end_ts * 1000, tol_ms=3000,
                                                     source=trade.get("strike_source"))
            frozen_close = (at_close or {}).get("price") or cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        if expired and trade.get("expired_at") is None:
            trade["expired_at"] = now_ts
        waited = (now_ts - trade["expired_at"]) if trade.get("expired_at") else 0.0

        _pt = _ps_settle_call(trade)

        # ── Authoritative resolution: PUSHED, not polled ─────────────────────────
        # The CLOB market channel we are already subscribed to emits `market_resolved`
        # with the winning asset id. That arrives within a frame of the venue deciding,
        # instead of up to 15s later, and costs no request. The REST poll below is kept
        # only as a fallback for when the socket missed the event (e.g. it fired while we
        # were reconnecting across the window roll), and is throttled to 15s as before.
        ws_res = clob_book_stream.resolution_for(trade.get("condition_id"),
                                                 trade.get("market_slug"),
                                                 trade.get("token_id"))
        market = None
        if ws_res is None and trade.get("last_api_check", 0) < now_ts - 15:
            try:
                market = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                market = None
            trade["last_api_check"] = now_ts
            if market is not None:
                trade["_market_closed"] = bool(market.get("closed"))
        market_closed = bool(ws_res) or trade.get("_market_closed", False)

        # Still live: window running and market still open → keep waiting.
        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        # ---- Determine the winning outcome ----
        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None

        # 0) Best source: PolyBolt published TWAPs (the official Polymarket 60s averages)
        if _pt:
            winning_index = up_index if _pt[0] == "UP" else down_index
            resolution = "polymarket_twap"
            strike, settlement_price = _pt[2], _pt[1]
            trade["settlement_price_at_expiry"] = trade["close_price"] = settlement_price
            trade["strike_price"] = strike
            trade["settle_wait_s"] = round(waited, 1)

        # 1) Venue PUSHED winner over CLOB WebSocket
        elif ws_res:
            win_asset = str(ws_res.get("winning_asset_id") or "")
            win_outcome = str(ws_res.get("winning_outcome") or "").lower()
            if win_asset and str(trade.get("token_id") or "") == win_asset:
                winning_index = up_index if trade["side"] == "UP" else down_index
                resolution = "ws_market_resolved"
            elif win_outcome in (settings.POLYMARKET_UP_LABEL.lower(), settings.POLYMARKET_DOWN_LABEL.lower()):
                winning_index = up_index if win_outcome == settings.POLYMARKET_UP_LABEL.lower() else down_index
                resolution = "ws_market_resolved"
            elif win_asset and trade.get("token_id"):
                winning_index = down_index if trade["side"] == "UP" else up_index
                resolution = "ws_market_resolved"

        # If not resolved by TWAP or ws_res, and still within authoritative settle wait: wait for TWAP/venue
        if winning_index == -1 and waited < getattr(settings, "AUTHORITATIVE_SETTLE_WAIT_S", 60.0) and not market_closed:
            remaining_active.append(trade)
            continue

        # 1) Authoritative — ONLY when Polymarket says the market is CLOSED. outcomePrices is
        # a (laggy) market price, not a settlement flag: a side trading >0.9 while the market
        # is still open is just the favourite, not the resolved winner. Requiring `closed`
        # avoids mis-settling on a transient pre-settlement price.
        if winning_index == -1 and market_closed:
            for i, p in enumerate(outcome_prices):
                try:
                    if float(p) > 0.9:
                        winning_index = i
                        resolution = "polymarket_settled"
                        break
                except Exception:
                    pass

        # 2) Fallback once the window/market is over: frozen CLOSE vs STRIKE (open). Both are
        # the Polymarket crypto_prices value (open at eventStartTime, close at end), so this
        # mirrors Polymarket's rule exactly: close >= open → Up (a tie resolves UP, per the
        # market's "greater than OR EQUAL TO" wording).
        strike = trade.get("strike_price")  # the marked OPEN
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)  # the frozen CLOSE
        # `close_price` was frozen from a live feed AT expiry, so it is trustworthy even if
        # the feed died afterwards. Anything else is only a running last-seen value, and
        # using it while the feed is stale means resolving against a frozen number — so in
        # that case we decline to settle and let the grace period handle it.
        close_is_frozen = trade.get("close_price") is not None
        if winning_index == -1 and (expired or market_closed):
            if strike and settlement_price and (close_is_frozen or settle_price_fresh):
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price >= strike else down_index
                resolution = "close_vs_open"
            elif strike and settlement_price and not trade.get("_logged_stale_settle"):
                trade["_logged_stale_settle"] = True
                log_message(f"Settlement feed stale for {trade['market_slug']} — holding for "
                            f"the venue's resolution instead of scoring on a frozen price")

        # ---- Could not resolve yet ----
        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            # Grace exhausted — void so a single bad trade can't block forever.
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            trade["exit_time"] = datetime.now().isoformat()
            trade["profit_loss"] = 0.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += trade["amount"]  # refund the stake
            state["trade_history"].append(trade)
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        # Open/close context — record it and show it in the log so the direction
        # (and which side that made win) is always visible.
        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        orig_amount = float(trade.get("original_amount") or trade["amount"])
        recovered = float(trade.get("recovered_usd") or 0.0)
        if won:
            payout = trade["shares"] * 1.0
            # Paper credits the simulated balance; live balance comes from the
            # on-chain USDC refresh in the main loop, not credited here.
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += payout
            else:
                # A live win pays out in ERC-1155 OUTCOME TOKENS, not collateral. Until they
                # are redeemed they are invisible to get_pusd_balance(), so the mirrored
                # balance would only ever fall — every stake debits and no win ever credits,
                # and percent-of-balance sizing would shrink after a winning trade. Queue
                # the redemption; it runs off the trading tick.
                state["pending_redemptions"].append({
                    "condition_id": trade.get("condition_id"),
                    "neg_risk": trade.get("neg_risk", False),
                    "token_id": trade.get("token_id"),
                    "side": trade["side"],
                    "shares": trade["shares"],
                    "slug": trade.get("market_slug"),
                    "attempts": 0,
                })
            trade["profit_loss"] = (recovered + payout) - orig_amount
            result = "WIN"
        else:
            trade["profit_loss"] = recovered - orig_amount
            result = "LOSS" if trade["profit_loss"] < 0 else "WIN"

        log_message(f"{result} [{resolution or 'unknown'}] {trade['side']}: {dir_txt} -> P/L ${trade['profit_loss']:.2f} ({trade['market_slug']})")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(trade)
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def redeem_loop():
    """Convert resolved winning outcome tokens into pUSD — OFF the trading tick.

    Redemption is an on-chain transaction (seconds), so it must never sit in the 500ms
    loop. Without it a live win is stranded as ERC-1155 tokens that `get_pusd_balance()`
    cannot see: the dashboard balance would drop on every entry and never rise on a win.

    Each queued win is retried a few times (the payout is only redeemable once the venue
    has actually reported the market, which can trail our own settlement by a few seconds).
    """
    while True:
        try:
            if (state["trading_mode"] == "live" and settings.AUTO_REDEEM_ENABLED
                    and state["pending_redemptions"]):
                job = state["pending_redemptions"][0]
                job["attempts"] += 1

                # Redeem what the wallet ACTUALLY holds, not what we think we hold.
                held = await asyncio.to_thread(clob_trader.get_token_balance, job.get("token_id"))
                if held is not None and held <= 0:
                    # Nothing there: either it was already swept (auto-redeem) or the venue
                    # has not credited yet. Retry a few times, then drop it.
                    state["pending_redemptions"].pop(0)
                    if job["attempts"] <= 3:
                        state["pending_redemptions"].append(job)
                    else:
                        log_message(f"Redeem: no tokens held for {job.get('slug')} — assuming already redeemed")
                else:
                    shares = held if held is not None else job.get("shares") or 0.0
                    up_sh = shares if job.get("side") == "UP" else 0.0
                    down_sh = shares if job.get("side") == "DOWN" else 0.0
                    res = await asyncio.to_thread(
                        clob_trader.redeem_position, job.get("condition_id"),
                        up_sh, down_sh, job.get("neg_risk", False))
                    state["pending_redemptions"].pop(0)
                    if res.get("ok"):
                        state["last_balance_refresh"] = 0   # payout landed — re-read balance
                        log_message(f"Redeemed {shares:.2f} winning {job.get('side')} shares "
                                    f"({job.get('slug')}) -> pUSD (tx {res.get('tx')})")
                    elif job["attempts"] < 5:
                        state["pending_redemptions"].append(job)   # transient — try again
                        log_message(f"Redeem retry {job['attempts']}/5 for {job.get('slug')}: {res.get('error')}")
                    else:
                        log_message(f"Redeem FAILED for {job.get('slug')} after 5 attempts: {res.get('error')}. "
                                    f"Redeem manually on polymarket.com to release the funds.")
        except Exception as e:
            print(f"Redeem loop error: {e}")
        await asyncio.sleep(5)


async def reconcile_live_fills():
    """Correct an open live trade's share count from the venue's own fill report.

    The order response usually carries the fill, but when it doesn't we fall back to the
    walked-book estimate — which is only an estimate. The authenticated user channel pushes
    the real `size_matched`, so we reconcile as soon as it arrives. Getting this right
    matters twice: the exit SELL must not ask for more shares than we hold (the FOK would
    be killed), and the win payout is `shares x $1`.
    """
    if state["trading_mode"] != "live":
        return
    for t in state["active_trades"]:
        if t.get("shares_source") == "user_ws" or not t.get("order_id"):
            continue
        fill = clob_user_stream.filled(t["order_id"])
        if not fill or not fill.get("shares"):
            continue
        old = t.get("shares") or 0.0
        new = float(fill["shares"])
        t["shares"] = new
        if fill.get("price"):
            t["entry_price"] = fill["price"]
        t["shares_source"] = "user_ws"
        if abs(new - old) > 1e-6:
            log_message(f"Fill reconciled ({t['side']}): {old:.2f} -> {new:.2f} shares "
                        f"@ {t['entry_price']:.3f} (venue report)")
        save_state()


async def maybe_auto_withdraw():
    """Auto-withdrawal (capital extractor) state machine — LIVE mode only.

        ARMED --(balance >= trigger)--> WAITING_FLAT --(no open trades)--> WITHDRAWING
              --(withdrawal submitted)--> WITHDRAW_SUBMITTED --> ARMED (+ auto-resume)

    While the state is not ARMED, new entries/flips are paused (see update_loop) so
    the account can settle flat before funds are extracted. Withdraws WITHDRAW_AMOUNT
    of pUSD to your own wallet — the EOA derived from the key/seed (gasless)."""
    if state["trading_mode"] != "live" or not settings.AUTO_WITHDRAW_ENABLED:
        if state["withdraw_state"] != "ARMED":   # disabled → never keep entries paused
            state["withdraw_state"] = "ARMED"
        return

    st = state["withdraw_state"]
    bal = state["paper_balance"]  # live pUSD balance is mirrored here

    if st == "ARMED":
        if bal is not None and bal >= settings.WITHDRAW_TRIGGER_BALANCE:
            state["withdraw_state"] = "WAITING_FLAT"
            log_message(f"Auto-withdraw: balance ${bal:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f} → pausing entries, waiting to go flat")

    elif st == "WAITING_FLAT":
        # FOK market orders never rest, so "flat" == no open positions.
        if not state["active_trades"]:
            state["withdraw_state"] = "WITHDRAWING"
            log_message("Auto-withdraw: account is flat → withdrawing")

    elif st == "WITHDRAWING":
        # Destination: the user-set address, or fall back to your own wallet (the EOA
        # derived from the key/seed) when left blank.
        recipient = settings.WITHDRAW_ADDRESS or clob_trader.get_eoa_address()
        if not recipient:
            log_message("Auto-withdraw aborted: no wallet/key available. Disarming.")
            state["withdraw_state"] = "ARMED"
            return
        amount = min(float(settings.WITHDRAW_AMOUNT), float(bal or 0))
        if amount <= 0:
            state["withdraw_state"] = "ARMED"
            return
        result = await asyncio.to_thread(clob_trader.withdraw_pusd, recipient, amount)
        if result.get("ok"):
            state["last_withdrawal"] = {"amount": result.get("amount"), "tx": result.get("tx"),
                                        "to": result.get("recipient"), "time": datetime.now().isoformat()}
            state["withdraw_state"] = "WITHDRAW_SUBMITTED"
            # Remember what we expect the balance to fall to, so "confirmed" mode has
            # something concrete to verify against.
            state["withdraw_expect_below"] = max(0.0, float(bal or 0) - amount * 0.5)
            state["last_balance_refresh"] = 0
            log_message(f"Auto-withdraw: submitted ${amount:.2f} → {recipient} (tx {result.get('tx')})")
        else:
            log_message(f"Auto-withdraw FAILED: {result.get('error')}. Disarming.")
            state["withdraw_state"] = "ARMED"

    elif st == "WITHDRAW_SUBMITTED":
        # `resume_after` is honoured here (it used to be stored, shown in the UI, and then
        # ignored — the bot always resumed at 'submitted').
        #   submitted: transfer_pusd already waited for a receipt, so resume immediately.
        #   confirmed: additionally wait until the on-chain balance actually reflects the
        #              withdrawal, so we can never resume — or re-trigger — on a stale read.
        state["last_balance_refresh"] = 0  # force a fresh balance read
        if str(settings.WITHDRAW_RESUME_AFTER or "submitted").lower() == "confirmed":
            expect_below = state.get("withdraw_expect_below")
            if expect_below is not None and bal is not None and bal > expect_below:
                if not state.get("_await_confirm_logged"):
                    state["_await_confirm_logged"] = True
                    log_message(f"Auto-withdraw: awaiting confirmation (balance ${bal:.2f} "
                                f"has not dropped below ${expect_below:.2f} yet)")
                return   # stay in WITHDRAW_SUBMITTED; entries remain paused
        state["_await_confirm_logged"] = False
        state["withdraw_expect_below"] = None
        if not settings.WITHDRAW_AUTO_RESUME:
            state["running"] = False
            log_message("Auto-withdraw complete; auto-resume OFF → bot stopped.")
        else:
            log_message("Auto-withdraw complete; trading resumed.")
        state["withdraw_state"] = "ARMED"


async def chainlink_refresh_loop():
    """Keep a recent Chainlink price WITHOUT blocking the trading tick.

    The Chainlink RPC round-trip costs seconds (multi-provider failover), and its WS only
    emits on AnswerUpdated — which can be minutes apart — so at startup the loop would fall
    back to that slow RPC on EVERY tick. But the Chainlink price is not on the latency path
    at all: it marks the window open and settles trades, both of which tolerate a couple of
    seconds of age. Only the Binance spot and the CLOB book need to be fast.

    So it is refreshed here, off the critical path, and the loop just reads the last value.
    """
    while True:
        try:
            # Only pay for the RPC while neither WS feed has produced a price yet.
            if not (polymarket_ws_stream.get_last().get("price") or chainlink_ws_stream.get_last().get("price")):
                state["chainlink_rest"] = await chainlink.chainlink_fetcher.fetch_chainlink_btc_usd()
        except Exception as e:
            print(f"Chainlink refresh failed: {e}")
        await asyncio.sleep(2)


async def seed_kline_buffers():
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 500),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        log_message(f"Seeded Binance kline buffers (1m/5m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

async def update_loop():
    global _tick_wakeup
    _tick_wakeup = asyncio.Event()
    last_csv_ts = 0.0

    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        # Chosen-side inputs on EVERY tick (even no-trade) so filters can be mined later:
        "chosen_side", "chosen_prob", "chosen_price", "chosen_ev",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        tick_start = time.monotonic()
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            poly_ws = polymarket_ws_stream.get_last()
            poly_ws = _ps_live_as_poly_ws(poly_ws)
            cl_ws = chainlink_ws_stream.get_last()

            # ── SPOT FRESHNESS (safety) ──────────────────────────────────────────
            # A silently-stalled Binance socket (alive connection, no messages) freezes
            # last_price. BTC keeps moving, so our "fair" value drifts away from the book
            # and the apparent edge GROWS without bound — a dead feed is indistinguishable
            # from a huge latency edge, and the bot would fire into it at max conviction.
            # So spot must be provably recent, or it is not spot at all.
            #
            # We deliberately do NOT fall back to the Chainlink/Polymarket price here. That
            # is the SLOW feed the book is already priced off; comparing it to the book
            # measures nothing (the "edge" would be an artifact, not a lead).
            binance_ws = binance_stream.get_last()
            ws_age = (time.time() - binance_ws["ts"]) if binance_ws.get("ts") else None
            ws_fresh = (binance_ws.get("price") is not None and ws_age is not None
                        and ws_age <= settings.MAX_SPOT_AGE_S)

            # ── Tick budget ──────────────────────────────────────────────────────
            # This is a LATENCY strategy on a 300s window: the tick period is how fast we
            # can see the book move, so nothing may block it that isn't strictly needed.
            # The Binance REST price and the Chainlink RPC are FALLBACKS ONLY — the WS
            # feeds above are preferred for both (see spot_price / current_price below).
            # Once a WS feed has a price we stop calling its REST twin, which takes the
            # slow RPC (~4.5s) out of the steady-state tick entirely. The CLOB books are
            # always fetched fresh: that book IS the thing we are arbing.
            need_binance_rest = not ws_fresh   # WS silent or stale -> try REST for a fresh spot

            coros = [fetch_polymarket_snapshot()]
            if need_binance_rest:
                coros.append(data.fetch_last_price(settings.SYMBOL))

            results = await asyncio.gather(*coros, return_exceptions=True)

            def _ok(idx):
                return results[idx] if idx < len(results) and not isinstance(results[idx], Exception) else None

            poly_snapshot = _ok(0) or {"ok": False}
            last_price = _ok(1) if need_binance_rest else None
            # Refreshed off-tick by chainlink_refresh_loop — never awaited here.
            chainlink_data = state.get("chainlink_rest") or {}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            # Resolve spot + whether it is trustworthy enough to trade on.
            if ws_fresh:
                spot_price, spot_source, spot_age = binance_ws["price"], "binance_ws", ws_age
            elif last_price is not None:
                spot_price, spot_source, spot_age = last_price, "binance_rest", 0.0
            else:
                # Keep the last known value for the DISPLAY only — it is not tradeable.
                spot_price, spot_source, spot_age = binance_ws.get("price"), "stale", ws_age
            spot_fresh = spot_source in ("binance_ws", "binance_rest")

            if spot_fresh != state.get("spot_fresh_last"):
                state["spot_fresh_last"] = spot_fresh
                if not spot_fresh:
                    age_txt = f"{spot_age:.0f}s old" if spot_age else "no data"
                    log_message(f"SPOT FEED STALE ({age_txt}) — entries halted until it recovers")
                else:
                    log_message(f"Spot feed OK ({spot_source})")

            # ── Time to expiry ───────────────────────────────────────────────────
            # The market's own endDate is authoritative; the local aligned-window clock
            # is the fallback. Kept in FRACTIONAL minutes: the fair-prob horizon is
            # continuous, and on a 5m market every second of decay moves the number.
            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]
            time_left_min = max(0.0, time_left_min)
            seconds_left = time_left_min * 60.0

            # The Binance 5m candle opens on the same boundary as the 5m market, so its open
            # is a fast, same-feed anchor for measuring how far price has moved since the
            # open. The AUTHORITATIVE strike (Polymarket's Price to Beat) is marked below;
            # the model anchors to that and applies the Binance move (offset-free).
            binance_open = spot_price
            if klines_5m:
                start_ms_b = timing["startMs"]
                for c in reversed(klines_5m):
                    if c["openTime"] <= start_ms_b:
                        binance_open = c["open"]
                        break

            current_price = None
            price_source = None
            settle_fresh = False

            # Prefer Polymarket's OWN Chainlink WS feed — it's the exact price stream
            # Polymarket settles on, so marking open/close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            #
            # Each source carries a freshness verdict. A dead WS keeps handing back its last
            # value forever, and settling a window against a frozen price mis-resolves every
            # near-the-money result the same way — so `settle_fresh` travels with the price
            # into update_trades, which refuses to score close-vs-open on a stale feed.
            poly_age = poly_ws.get("age")
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
                settle_fresh = poly_age is not None and poly_age <= settings.MAX_SETTLE_PRICE_AGE_S
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
                # AnswerUpdated only fires on a deviation/heartbeat (minutes apart), so this
                # feed cannot be judged by age; it is authoritative whenever it speaks.
                settle_fresh = True
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"
                settle_fresh = True

            if settle_fresh != state.get("settle_fresh_last"):
                state["settle_fresh_last"] = settle_fresh
                if not settle_fresh and current_price:
                    age_txt = f"{poly_age:.0f}s" if poly_age is not None else "unknown age"
                    log_message(f"SETTLEMENT FEED STALE ({age_txt}) — trades will wait for "
                                f"the venue's own resolution rather than settle on a frozen price")

            # ── Mark each window's OPEN ("Price to Beat") from eventStartTime ─────
            # Polymarket fixes the strike as the Chainlink data-stream price at the market's
            # eventStartTime. We read the SAME value from Polymarket's per-second
            # `crypto_prices` feed AT THAT EXACT SECOND (price_at) — the authoritative
            # strike — rather than "the first price we happened to see near the local clock".
            #
            # If the bot started mid-window and its feed history doesn't reach back to
            # eventStartTime, the strike stays None and this window is simply NOT TRADED —
            # we wait for the next window whose boundary we actually observe. There is
            # deliberately no Binance approximation: Binance is a different feed, and
            # approximating the strike from it reintroduces exactly the cross-feed
            # settlement error the eventStartTime read exists to remove.
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = poly_snapshot.get("event_start_ms") if poly_snapshot.get("ok") else None
            start_ms = int(event_start_ms) if event_start_ms else int(timing["startMs"])
            opens = state["market_opens"]
            prev_ws = state.get("last_window_start")

            # When the window rolls, freeze the PRIOR window's CLOSE = the feed value at its
            # end second (same feed as the open), falling back to the last price we saw.
            if prev_ws is not None and prev_ws != start_ms and prev_ws in opens and opens[prev_ws].get("close") is None:
                # Pinned to the feed that marked THAT window's open — see update_trades.
                at_close = polymarket_ws_stream.price_at(prev_ws + window_ms, tol_ms=3000,
                                                         source=opens[prev_ws].get("source"))
                close_val = (at_close or {}).get("price") or state.get("last_seen_price")
                if close_val:
                    opens[prev_ws]["close"] = close_val
            if current_price:
                state["last_seen_price"] = current_price

            if start_ms not in opens:
                opens[start_ms] = {"chainlink": None, "close": None, "source": None}
                for k in list(opens.keys()):           # prune old windows
                    if k < start_ms - 4 * window_ms:
                        del opens[k]
            win = opens[start_ms]

            # Capture the strike EXACTLY at eventStartTime from Polymarket's OWN feed — the
            # authoritative Price to Beat, the same value the market settles against. No other
            # source: Binance is a different feed and would reintroduce the very settlement
            # errors this fix removes. If we weren't running at the boundary (the feed has no
            # sample at that second), the strike stays None and this window simply isn't
            # traded — we wait for the next window whose open we actually observe.
            if win["chainlink"] is None:
                strike = polymarket_ws_stream.price_at(start_ms, tol_ms=2500)
                if strike is not None:
                    win["chainlink"] = strike["price"]
                    # Record WHICH feed marked it. The close is later read from this same
                    # feed, so the two ends of the window can never straddle the ~0.13%
                    # gap between the venue's USDT-quoted and Chainlink BTC/USD prices.
                    win["source"] = strike["source"]
                    feed_label = ("Chainlink BTC/USD" if strike["source"] == polymarket_ws_stream.CHAINLINK_TOPIC
                                  else "USDT spot (Chainlink unavailable)")
                    log_message(f"Strike @ eventStartTime {datetime.utcfromtimestamp(start_ms/1000).strftime('%H:%M:%S')}Z "
                                f"= {strike['price']:.2f} (Price to Beat, {feed_label})")
            state["last_window_start"] = start_ms

            # Strike (open) for a trade entered now: ONLY the Polymarket-feed value at
            # eventStartTime. None until/unless we observed the boundary, in which case no
            # trade opens this window.
            strike_open = win["chainlink"]
            strike_source = win.get("source")

            # PolyBolt official TWAP Price to Beat
            strike_open, strike_source = _ps_strike(start_ms, strike_open, strike_source)
            if strike_open is not None and win.get("chainlink") != strike_open:
                win["chainlink"] = strike_open
                win["source"] = strike_source
            _ps_window_note(start_ms, window_ms, win.get("chainlink"))

            # ── Fair probability, anchored to the AUTHORITATIVE strike ───────────
            # The model needs P(close > strike). We anchor to the real eventStartTime strike
            # and apply the move seen on the FAST Binance feed since the Binance open:
            #   effective = strike + (binance_spot - binance_open)
            # This keeps the signal fast while measuring against the true Price to Beat, and
            # the (binance_spot - binance_open) delta is offset-free (same feed on both ends).
            # Falls back to the Binance open as the anchor until the strike is marked.
            model_open = strike_open if strike_open else binance_open
            if strike_open and binance_open and spot_price:
                effective_spot = strike_open + (spot_price - binance_open)
            else:
                effective_spot = spot_price
            sigma_1m, sigma_ewma = indicators.adaptive_vol(
                klines_1m,
                lookback=settings.VOL_LOOKBACK_1M,
                half_life=settings.VOL_EWMA_HALF_LIFE,
            )
            fair_up = indicators.fair_prob_up(effective_spot or 0, model_open or 0, time_left_min,
                                              sigma_1m, drift_per_minute=0.0)
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "minutes_left": time_left_min,
                "sigma_1m": sigma_1m,
                "sigma_ewma": sigma_ewma,
            }

            # ── The price we'd ACTUALLY pay, for the size we'd actually trade ────
            # EV is gated on the blended cost of filling the FULL stake (walking the ask
            # levels), not on the best ask. An edge that only exists for the first few
            # shares at the top of the book is not an edge for the size we trade — and on
            # a thin 5m book that is the difference between a real edge and a leak.
            ob_snap = poly_snapshot.get("orderbook", {}) if poly_snapshot["ok"] else {}
            best_ask_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            best_ask_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None
            # Midpoints = the real-time mark Polymarket's site shows. These are the DISPLAY
            # odds and the basis for the shown model edge.
            mids = poly_snapshot.get("mids", {}) if poly_snapshot["ok"] else {}
            market_up = mids.get("up")      # what the site shows for UP (mid)
            market_down = mids.get("down")  # ... and DOWN

            # Actual fill cost for the FULL stake (walk the asks). EV gates on THIS, not the
            # mid/ask, since an edge that vanishes once you walk the book was never real.
            stake = compute_stake(state["paper_balance"])
            eff_up = effective_entry(ob_snap, "UP", stake, best_ask_up)
            eff_down = effective_entry(ob_snap, "DOWN", stake, best_ask_down)
            fill_up = eff_up["vwap"]
            fill_down = eff_down["vwap"]

            # ── LATENCY EDGE ─────────────────────────────────────────────────────
            # Model fair prob vs the market's implied prob from the MID (the mark the book
            # shows). A positive edge = the book hasn't repriced the move yet.
            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            # Strategy C: Aggressor-flow + multi-window spot momentum hook
            spot_momentum = binance_stream.get_momentum(settings.MOMENTUM_LOOKBACK_S)
            flow_metrics = binance_stream.get_flow_metrics(
                fast_s=settings.MOMENTUM_FAST_S,
                slow_s=settings.MOMENTUM_SLOW_S,
                extreme_s=30.0,
            )

            decision = engines.decide_underdog_reversal({
                "mcProbUp": fair_up,
                "priceUp": fill_up,     # EV on the real fill price, not the display mid
                "priceDown": fill_down,
                "spotPrice": effective_spot,
                "strikePrice": model_open,
                "sigma1m": sigma_1m,
                "timeLeftMin": time_left_min,
                "momentum": spot_momentum,
                "flowMetrics": flow_metrics,
                "maxUnderdogPrice": settings.MAX_UNDERDOG_PRICE,
                "minUnderdogProb": settings.MIN_UNDERDOG_PROB,
                "minUnderdogEv": settings.MIN_UNDERDOG_EV,
                "maxSigmaDistance": settings.MAX_SIGMA_DISTANCE,
                "minTimeLeftMin": settings.MIN_TIME_LEFT_MIN,
                "maxTimeLeftMin": settings.MAX_TIME_LEFT_MIN,
                "momentumFilter": settings.MOMENTUM_FILTER_ENABLED,
                "minBounceSigma": settings.MIN_BOUNCE_SIGMA,
                "minFlowImbalance": settings.MIN_FLOW_IMBALANCE,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price,
                                   "chainlink_fresh": settle_fresh}

            exec_result = None
            flip_side = None
            flip_result = None
            tp_sl_hit = None

            # Clear the take-profit/stop-loss market lock once the window has rolled to
            # a new market (so the next 5m window can trade again).
            cur_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            if state.get("tp_sl_locked_market") and cur_market_id and state["tp_sl_locked_market"] != cur_market_id:
                state["tp_sl_locked_market"] = None

            # Take-profit / stop-loss runs first (even mid-withdrawal we still want to
            # honour an exit), and closing locks the market against re-entry this window.
            if poly_snapshot["ok"] and state["running"]:
                tp_sl_hit = await maybe_tp_sl(poly_snapshot)

            # Entries/flips run only when STARTED, no withdrawal pending, this market
            # isn't TP/SL-locked, and TP/SL didn't just fire this tick.
            tp_sl_locked = state.get("tp_sl_locked_market") is not None and state["tp_sl_locked_market"] == cur_market_id
            # A stale spot feed makes the edge unmeasurable (and phantom-large), so it is a
            # hard block on opening anything. Open positions still settle normally.
            entries_allowed = (state["running"] and state["withdraw_state"] == "ARMED"
                               and not tp_sl_locked and not tp_sl_hit and spot_fresh)
            if poly_snapshot["ok"] and entries_allowed:
                flip_result = await maybe_flip_position(fair_up, poly_snapshot, time_left_min, strike_open, strike_source)
                flip_side = flip_result.get("side") if flip_result else None
                # If a flip ran this tick, do NOT also run the EV entry. A flip that failed
                # to re-open leaves us flat, and the EV decision can favour the side the
                # flip just deliberately abandoned — so the bot would immediately buy back
                # what it just sold. The next tick re-evaluates cleanly either way.
                if flip_result is None:
                    exec_result = await execute_trade(decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open, poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}), strike_source)
            elif not state["running"]:
                exec_result = "stopped"
            elif not spot_fresh:
                exec_result = "stale_spot"
            elif tp_sl_hit:
                exec_result = tp_sl_hit
            elif tp_sl_locked:
                exec_result = "tp_sl_locked"

            # Correct any live position's share count from the venue's own fill report
            # before it is marked, exited or paid out.
            await reconcile_live_fills()

            await update_trades(current_prices_dict)

            # ── Mark-to-market: value each OPEN position at the current bid of its
            # held side (what you'd get selling right now) and roll it into equity so
            # the headline number doesn't just drop by the stake on entry.
            open_value = 0.0
            snap_ob = poly_snapshot.get("orderbook", {}) if poly_snapshot.get("ok") else {}
            snap_market_id = str(poly_snapshot["market"].get("id")) if poly_snapshot.get("ok") else None
            for t in state["active_trades"]:
                held_key = "up" if t["side"] == "UP" else "down"
                # Only trust the live book if this trade is in the market we just snapshotted.
                # Mark at the price we'd actually get LIQUIDATING the whole position (walk
                # the bids), not at the best bid — same reason exits do.
                if snap_market_id is not None and str(t.get("market_id")) == snap_market_id:
                    levels = (snap_ob.get(held_key) or {}).get("bidLevels") or []
                    ex = data.walk_bids(levels, t["shares"])
                    if ex["vwap"] and ex["sold_shares"] + 1e-6 >= t["shares"]:
                        t["cur_bid"] = ex["vwap"]
                mark = t.get("cur_bid") or t.get("entry_price")  # fall back to entry (neutral) until we see a bid
                orig_amt = float(t.get("original_amount") or t["amount"])
                rec_usd = float(t.get("recovered_usd") or 0.0)
                t["unrealized_pl"] = (rec_usd + t["shares"] * mark) - orig_amt
                open_value += t["shares"] * mark
            equity = state["paper_balance"] + open_value

            # ── Backfill the market OUTCOME onto flip-closed trades ──────────────
            # A flip books its P/L on the early-exit sell, so a side can show a loss
            # even though the market later resolves in its favour. Once that window's
            # CLOSE is known, score each flipped-out side against the true open→close
            # so the history shows whether the market actually went its way.
            backfilled = False
            for h in state["trade_history"]:
                if h.get("exit_reason") != "flip" or h.get("market_won") is not None:
                    continue
                ws = h.get("window_start_ms")
                winfo = opens.get(ws) if ws is not None else None
                close_px = winfo.get("close") if winfo else None
                open_px = h.get("open_price") or h.get("strike_price")
                if close_px is not None and open_px:
                    h["close_price"] = close_px
                    h["market_won"] = (("UP" if close_px >= open_px else "DOWN") == h["side"])
                    backfilled = True
            if backfilled:
                save_state()

            # In live mode, reflect the real on-chain pUSD balance in the dashboard
            # (refreshed ~10s so the header balance tracks near-realtime).
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 10:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts
                # Auto-withdrawal (capital extractor) — pause/flat/withdraw/resume.
                await maybe_auto_withdraw()

            # A flip opens a position independently of the EV signal — surface it so
            # a flip tick never reads as a bare "NO TRADE". A flip whose re-entry failed is
            # reported as such rather than as a completed flip.
            if flip_side:
                signal_label = f"FLIP {flip_side}"
                exec_result = (f"flipped_to_{flip_side}" if flip_result.get("opened")
                               else f"flip_close_only_{flip_result.get('reason')}")
            else:
                signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            now_mono = time.monotonic()
            csv_due = (now_mono - last_csv_ts) >= (settings.POLL_INTERVAL_MS / 1000.0)
            important_event = bool(flip_side or tp_sl_hit or exec_result == "entered")
            if csv_due or important_event:
                last_csv_ts = now_mono
                utils.append_csv_row("./logs/signals.csv", csv_header, [
                    datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                    signal_label, fair_up, 1 - fair_up, market_up, market_down,
                    edge["edgeUp"], edge["edgeDown"],
                    # chosen-side prob/price/ev — present on ENTER and NO_TRADE alike
                    decision.get("side"), decision.get("prob"), decision.get("price"), decision.get("ev"),
                    f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                    decision.get("reason", ""), exec_result or ""
                ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "balance": state["paper_balance"],     # cash only
                    "open_value": open_value,              # mark-to-market value of open positions
                    "equity": equity,                      # cash + open position value
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "window_minutes": settings.CANDLE_WINDOW_MINUTES,
                    "running": state["running"],
                    "has_live_creds": bool(settings.PRIVATE_KEY),
                    "withdraw": {
                        "enabled": settings.AUTO_WITHDRAW_ENABLED,
                        "state": state["withdraw_state"],
                        "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
                        "amount": settings.WITHDRAW_AMOUNT,
                        "last": state["last_withdrawal"],
                    }
                },
                "prices": {
                    "spot": spot_price,
                    "spot_source": spot_source,          # binance_ws | binance_rest | stale
                    "spot_age_s": spot_age,
                    "spot_fresh": spot_fresh,            # false => entries are halted
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "settle_fresh": settle_fresh,        # false => won't settle on this price
                    "settle_age_s": poly_age,
                    "poly_up": market_up,                # MID = the real-time mark the site shows
                    "poly_down": market_down,
                    "fill_up": fill_up,                  # VWAP you'd actually pay for the full stake
                    "fill_down": fill_down,
                    "best_ask_up": best_ask_up,          # top of book
                    "best_ask_down": best_ask_down,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else "none",
                    "book_age_s": clob_book_stream.socket_age(),   # since ANY frame on the book socket
                    "book_fresh": clob_book_stream.socket_fresh(),
                    "window_open": strike_open,          # this window's marked OPEN (Price to Beat)
                    "window_open_source": strike_source,  # which venue feed marked it (Chainlink preferred)
                    "price_stream": polybolt_feed.view() if polybolt.on(settings.PRICE_STREAM) else None
                },
                "indicators": {
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision,
                    "underdog": {
                        "max_price": settings.MAX_UNDERDOG_PRICE,
                        "z_dist": decision.get("z_dist"),
                        "momentum": decision.get("momentum")
                    }
                }
            }
            state["last_update_ts"] = time.time()

        except Exception as e:
            print(f"Error in update loop: {e}")

        # Event-driven wakeup: sleep at least EVENT_MIN_INTERVAL_MS (25ms) to coalesce bursts,
        # then wake immediately on the next Binance trade or CLOB book update (max wait = POLL_INTERVAL_MS).
        min_wait_s = max(0.005, float(getattr(settings, "EVENT_MIN_INTERVAL_MS", 25)) / 1000.0)
        elapsed_s = time.monotonic() - tick_start
        if elapsed_s < min_wait_s:
            await asyncio.sleep(min_wait_s - elapsed_s)
        max_wait_s = max(0.05, float(settings.POLL_INTERVAL_MS) / 1000.0)
        _tick_wakeup.clear()
        try:
            await asyncio.wait_for(_tick_wakeup.wait(), timeout=max_wait_s)
        except asyncio.TimeoutError:
            pass


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

@app.get("/logs.txt")
async def download_logs():
    """Download the FULL console log as a text file (the live view is capped at 100 lines)."""
    content = ""
    try:
        if os.path.exists("logs/console.log"):
            with open("logs/console.log", "r", encoding="utf-8") as f:
                content = f.read()
    except Exception:
        content = ""
    if not content:
        content = "\n".join(state["logs"])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(content=content, media_type="text/plain",
                    headers={"Content-Disposition": f'attachment; filename="console_log_{STRATEGY_LABEL}_{ts}.txt"'})

@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_5m_series()

@app.get("/api/settings")
async def get_settings():
    pk = settings.PRIVATE_KEY
    masked_pk = pk[:6] + "..." + pk[-4:] if pk and len(pk) > 10 else pk

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "relayer": {
            "api_key": "set" if settings.RELAYER_API_KEY else ""
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "withdraw_address": settings.WITHDRAW_ADDRESS,
            "auto_resume_after_withdrawal": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE,
            "max_entry_slippage": settings.MAX_ENTRY_SLIPPAGE,
            "clob_order_type": settings.CLOB_ORDER_TYPE,
            "clob_post_only": settings.CLOB_POST_ONLY
        },
        "underdog": {
            "max_price": settings.MAX_UNDERDOG_PRICE,
            "min_prob": settings.MIN_UNDERDOG_PROB,
            "min_ev": settings.MIN_UNDERDOG_EV,
            "max_sigma_distance": settings.MAX_SIGMA_DISTANCE,
            "min_time_left_min": settings.MIN_TIME_LEFT_MIN,
            "max_time_left_min": settings.MAX_TIME_LEFT_MIN,
            "momentum_filter": settings.MOMENTUM_FILTER_ENABLED,
            "min_bounce_sigma": settings.MIN_BOUNCE_SIGMA,
            "min_flow_imbalance": settings.MIN_FLOW_IMBALANCE,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD
        },
        "ev": {
            "ev_threshold": settings.MIN_UNDERDOG_EV,
            "min_prob": settings.MIN_UNDERDOG_PROB,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "tp_sl": {
            "enabled": settings.TP_SL_ENABLED,
            "take_profit_cents": getattr(settings, "TAKE_PROFIT_CENTS", settings.TAKE_PROFIT_PCT),
            "take_profit_pct": getattr(settings, "TAKE_PROFIT_CENTS", settings.TAKE_PROFIT_PCT),
            "stop_loss_pct": settings.STOP_LOSS_PCT,
            "stop_loss_grace_s": settings.STOP_LOSS_GRACE_S,
            "free_roll_enabled": settings.FREE_ROLL_ENABLED,
            "free_roll_mult": settings.FREE_ROLL_MULT,
            "free_roll_min_cents": settings.FREE_ROLL_MIN_CENTS,
            "free_roll_sell_frac": settings.FREE_ROLL_SELL_FRAC,
            "trailing_stop_enabled": settings.TRAILING_STOP_ENABLED,
            "trailing_activate_cents": settings.TRAILING_ACTIVATE_CENTS,
            "trailing_drop_cents": settings.TRAILING_DROP_CENTS
        },
        "price_stream": polybolt.merged(settings.PRICE_STREAM),
        "chainlink": {
            "alchemy_api_key": "set" if settings.ALCHEMY_API_KEY else ""
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        # masked value returned by GET — keep the stored key unchanged
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk is not None:
        from bot.config import normalize_private_key
        settings.PRIVATE_KEY = normalize_private_key(new_pk)
        new_settings["private_key"] = settings.PRIVATE_KEY  # persist the derived hex key, never the seed

    # "set" is the masked sentinel returned by GET for stored secrets — if the form
    # sends it back unchanged, don't overwrite the real key with the sentinel.
    if isinstance(new_settings.get("relayer"), dict) and new_settings["relayer"].get("api_key") == "set":
        new_settings["relayer"].pop("api_key", None)
    if isinstance(new_settings.get("chainlink"), dict) and new_settings["chainlink"].get("alchemy_api_key") == "set":
        new_settings["chainlink"].pop("alchemy_api_key", None)

    _ps_new = None
    if "price_stream" in new_settings:
        try:
            _ps_new = polybolt.normalize(new_settings["price_stream"], settings.PRICE_STREAM)
        except ValueError as e:
            return {"status": "error", "error": f"invalid_settings: {e}"}
        new_settings["price_stream"] = _ps_new

    # Deep-merge into the existing config so keys not present in the settings form
    # (chainlink, binance_base_url, poll_interval_ms, etc.) are preserved.
    existing_cfg = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open("config.json", "w") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))
        if "max_entry_slippage" in t: settings.MAX_ENTRY_SLIPPAGE = float(t["max_entry_slippage"])
        if "clob_order_type" in t: settings.CLOB_ORDER_TYPE = str(t["clob_order_type"]).upper()
        if "clob_post_only" in t: settings.CLOB_POST_ONLY = bool(t["clob_post_only"])

    if "underdog" in new_settings:
        ud = new_settings["underdog"]
        if "max_price" in ud: settings.MAX_UNDERDOG_PRICE = float(ud["max_price"])
        if "min_prob" in ud: settings.MIN_UNDERDOG_PROB = float(ud["min_prob"])
        if "min_ev" in ud: settings.MIN_UNDERDOG_EV = float(ud["min_ev"])
        if "max_sigma_distance" in ud: settings.MAX_SIGMA_DISTANCE = float(ud["max_sigma_distance"])
        if "min_time_left_min" in ud: settings.MIN_TIME_LEFT_MIN = float(ud["min_time_left_min"])
        if "max_time_left_min" in ud: settings.MAX_TIME_LEFT_MIN = float(ud["max_time_left_min"])
        if "momentum_filter" in ud: settings.MOMENTUM_FILTER_ENABLED = bool(ud["momentum_filter"])
        if "min_bounce_sigma" in ud: settings.MIN_BOUNCE_SIGMA = float(ud["min_bounce_sigma"])
        if "min_flow_imbalance" in ud: settings.MIN_FLOW_IMBALANCE = float(ud["min_flow_imbalance"])
        if "min_book_liquidity_usd" in ud: settings.MIN_BOOK_LIQUIDITY_USD = float(ud["min_book_liquidity_usd"])
    elif "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_UNDERDOG_EV = settings.EV_THRESHOLD
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_UNDERDOG_PROB = settings.MIN_PROB_EV
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "tp_sl" in new_settings:
        ts = new_settings["tp_sl"]
        if "enabled" in ts: settings.TP_SL_ENABLED = bool(ts["enabled"])
        if "take_profit_cents" in ts:
            tp_val = float(ts["take_profit_cents"])
            settings.TAKE_PROFIT_CENTS = tp_val
            settings.TAKE_PROFIT_PCT = tp_val
        elif "take_profit_pct" in ts:
            tp_val = float(ts["take_profit_pct"])
            settings.TAKE_PROFIT_CENTS = tp_val
            settings.TAKE_PROFIT_PCT = tp_val
        settings.STOP_LOSS_PCT = float(ts.get("stop_loss_pct", settings.STOP_LOSS_PCT))
        if "stop_loss_grace_s" in ts:
            settings.STOP_LOSS_GRACE_S = float(ts["stop_loss_grace_s"])
        if "free_roll_enabled" in ts: settings.FREE_ROLL_ENABLED = bool(ts["free_roll_enabled"])
        if "free_roll_mult" in ts: settings.FREE_ROLL_MULT = float(ts["free_roll_mult"])
        if "free_roll_min_cents" in ts: settings.FREE_ROLL_MIN_CENTS = float(ts["free_roll_min_cents"])
        if "free_roll_sell_frac" in ts: settings.FREE_ROLL_SELL_FRAC = float(ts["free_roll_sell_frac"])
        if "trailing_stop_enabled" in ts: settings.TRAILING_STOP_ENABLED = bool(ts["trailing_stop_enabled"])
        if "trailing_activate_cents" in ts: settings.TRAILING_ACTIVATE_CENTS = float(ts["trailing_activate_cents"])
        if "trailing_drop_cents" in ts: settings.TRAILING_DROP_CENTS = float(ts["trailing_drop_cents"])

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "relayer" in new_settings and isinstance(new_settings["relayer"], dict):
        if "api_key" in new_settings["relayer"]:
            settings.RELAYER_API_KEY = new_settings["relayer"]["api_key"]

    if "chainlink" in new_settings and isinstance(new_settings["chainlink"], dict):
        if "alchemy_api_key" in new_settings["chainlink"]:
            settings.ALCHEMY_API_KEY = new_settings["chainlink"]["alchemy_api_key"]

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "withdraw_address" in ce: settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
        if "auto_resume_after_withdrawal" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]

    # Credentials may have changed — drop the cached CLOB client so the next live
    # order re-initialises with the new key / relayer / alchemy settings.
    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    # Only reset the displayed balance in paper mode; live mode reads the on-chain
    # pUSD balance and we don't want to clobber it with the paper default on save.
    if settings.MODE == "paper":
        state["paper_balance"] = settings.PAPER_BALANCE_USD

    if _ps_new is not None:
        if _ps_new != polybolt.merged(settings.PRICE_STREAM):
            log_message(polybolt.status_line(_ps_new))
        settings.PRICE_STREAM = _ps_new

    if settings.PRIVATE_KEY:
        asyncio.create_task(asyncio.to_thread(clob_trader.ensure_ready))

    if settings.SYMBOL != old_symbol:
        polybolt_feed.set_symbol(polybolt.symbol_for(settings.SYMBOL))
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_event=_trigger_tick)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=500)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
        )
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
        asyncio.create_task(chainlink_ws_stream.start())

    return {"status": "ok"}

def _reflect_running_now():
    """Mirror the running flag into latest_data immediately so /api/latest is in sync
    on the very next poll (the update loop would otherwise lag ~1s, flickering the UI)."""
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]

@app.post("/api/start")
async def start_trading():
    """Begin trading. Data/prices stream continuously; this flips the gate so the
    engine may enter/flip trades."""
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    """Stop all trading. New entries and flips halt immediately; any open position
    keeps settling to expiry so it can't get stuck."""
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    return {"ok": True, "running": False}

@app.post("/api/close-trade")
async def close_trade():
    """Close the open position immediately at the current bid and lock the market so no
    new entry happens until the next 5m window. Manual override — independent of TP/SL."""
    result = await close_active_trade_now()
    return result

@app.post("/api/test-connection")
async def test_connection():
    """Validate the saved key/seed + relayer: derive the EOA and candidate wallets and
    report which one holds pUSD (the trading wallet) and the chosen signature type."""
    result = await asyncio.to_thread(clob_trader.test_connection)
    if result.get("ok"):
        log_message(f"Connection test OK: EOA {result.get('eoa')} → trading wallet {result.get('funder')}"
                    + ("" if result.get("relayer_key_set") else "  (relayer key MISSING)"))
    else:
        log_message(f"Connection test failed: {result.get('error')}")
    return result

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"], "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

@app.get("/history.csv")
async def get_history_csv():
    """All closed trades as a downloadable CSV (one row per trade)."""
    import csv, io
    cols = [
        ("market_slug", "market"),
        ("side", "side"),
        ("entry_time", "entry_time"),
        ("exit_time", "close_time"),
        ("entry_price", "entry_price"),
        ("exit_mark", "exit_price"),
        ("open_reason", "opened_by"),
        ("exit_reason", "exit_reason"),
        ("amount", "stake_usd"),
        ("shares", "shares"),
        ("open_price", "open_px"),
        ("close_price", "close_px"),
        ("profit_loss", "profit_loss"),
        ("status", "status"),
        ("resolution", "resolution"),
        ("winning_outcome", "winning_outcome"),
        ("mode", "mode"),
    ]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([label for _, label in cols])
    for t in state["trade_history"]:
        w.writerow([t.get(key, "") for key, _ in cols])
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="closed_trades_{STRATEGY_LABEL}_{ts}.csv"'},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
