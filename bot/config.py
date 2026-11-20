import os
import json
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict, Any, Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    MODE: str = "paper"  # "paper" or "live"
    PAPER_BALANCE_USD: float = 1000.0
    PRIVATE_KEY: str = ""  # hex private key, or derived from a 12/24-word seed phrase

    # Live trading (Polymarket CLOB V2). The wallet is derived from PRIVATE_KEY (hex
    # key or seed phrase). New wallets trade via the gasless deposit-wallet flow
    # (POLY_1271); the relayer key sponsors on-chain setup (deploy/approvals).
    CLOB_MAX_SLIPPAGE: float = 0.02  # marketable-limit buffer above the quote (probability units)
    RELAYER_API_KEY: str = ""        # Polymarket relayer API key (gasless on-chain txs)

    # ── Auto-withdrawal (capital extractor) ─────────────────────────────────────
    # When the live balance reaches TRIGGER, pause entries, wait until flat, then
    # withdraw AMOUNT of pUSD to your own wallet (the key/seed EOA) and auto-resume.
    AUTO_WITHDRAW_ENABLED: bool = False
    WITHDRAW_TRIGGER_BALANCE: float = 2000.0  # withdraw once balance reaches this
    WITHDRAW_AMOUNT: float = 1000.0           # amount of pUSD to withdraw each time
    WITHDRAW_ADDRESS: str = ""                # destination wallet (blank = your own key/seed EOA)
    WITHDRAW_AUTO_RESUME: bool = True         # resume trading after the withdrawal
    WITHDRAW_RESUME_AFTER: str = "submitted"  # "submitted" or "confirmed"

    SYMBOL: str = "BTCUSDT"
    BINANCE_BASE_URL: str = "https://api.binance.com"
    GAMMA_BASE_URL: str = "https://gamma-api.polymarket.com"
    CLOB_BASE_URL: str = "https://clob.polymarket.com"

    # A 5m window is 300s: the poll interval is how fast we SEE the book move, so it
    # bounds how much of the staleness we can actually capture.
    POLL_INTERVAL_MS: int = 500
    CANDLE_WINDOW_MINUTES: int = 5

    # Realized drift/vol are measured on 1m candles, so sigma is PER MINUTE and the
    # fair-prob horizon scales continuously as sqrt(minutes_left).
    VOL_LOOKBACK_1M: int = 300          # 1m candles used for realized drift/vol (5 hours)

    # ── Feed freshness (SAFETY) ─────────────────────────────────────────────────
    # A silently-stalled Binance socket (connection alive, messages stopped) freezes the
    # spot price. The market keeps moving, so the gap between our "fair" value and the
    # book grows without bound — a DEAD FEED LOOKS EXACTLY LIKE A HUGE LATENCY EDGE, and
    # the bot would fire into it with maximum confidence. Spot older than this is treated
    # as no spot at all, and entries stop until a fresh price arrives.
    MAX_SPOT_AGE_S: float = 5.0

    # The SAME failure mode applies to the order book — and worse, because the book IS
    # the thing we claim is stale. A frozen book is indistinguishable from an enormous
    # latency edge. We can't age out the book DATA (a quiet book is legitimately
    # unchanged), so we age out the SOCKET: an app-level PING is sent every 10s and any
    # inbound frame refreshes the clock. No frame for this long => the stream is not
    # trusted and we fall back to REST /book.
    MAX_BOOK_AGE_S: float = 15.0

    # Settlement/strike feed (Polymarket crypto_prices). Marking a window's close against
    # a frozen price silently mis-resolves near-the-money trades, so a price older than
    # this is not used to settle; we wait for the authoritative Polymarket resolution.
    MAX_SETTLE_PRICE_AGE_S: float = 30.0
    AUTHORITATIVE_SETTLE_WAIT_S: float = 90.0

    # Polymarket price stream (TWAP & Price to Beat via ws-live-v2)
    PRICE_STREAM: Dict = {}

    # Winning outcome tokens are ERC-1155 positions, not cash: they must be REDEEMED into
    # pUSD or a live win never becomes spendable balance. Redemption is gasless via the
    # relayer, same as every other on-chain action here.
    AUTO_REDEEM_ENABLED: bool = True

    # Risk per trade: "percent" = RISK_VALUE% of balance; "fixed" = RISK_VALUE dollars.
    RISK_TYPE: str = "percent"
    RISK_VALUE: float = 10.0

    # ── Underdog Reversal Engine (Strategies A + D + C) ─────────────────────────
    # Target extreme underdog mispricings (shares priced <= MAX_UNDERDOG_PRICE, e.g. 7¢).
    MAX_UNDERDOG_PRICE: float = 0.07       # target shares priced <= this (7¢)
    MIN_UNDERDOG_PROB: float = 0.10        # Strategy A: min model fair probability (10%)
    MIN_UNDERDOG_EV: float = 0.02          # Strategy A: min expected value (fair - ask >= 2¢)
    MAX_SIGMA_DISTANCE: float = 2.0        # Strategy D: Volatility cone distance threshold (Z-score)
    MOMENTUM_FILTER_ENABLED: bool = True   # Strategy C: Binance momentum hook towards strike
    MOMENTUM_LOOKBACK_S: float = 10.0      # Lookback window for momentum in seconds
    MIN_BOOK_LIQUIDITY_USD: float = 20.0   # skip if the ask side can't absorb the stake

    # Aliases for backwards compatibility
    EV_THRESHOLD: float = 0.02
    MIN_PROB_EV: float = 0.10

    # Close-and-flip the open position on a strong opposite signal
    FLIP_ENABLED: bool = False
    FLIP_MIN_CONVICTION: float = 0.80   # opposite side's adjusted prob must be >= this
    FLIP_MIN_MINUTES_LEFT: float = 1.5  # and at least this much time left in the 5m window

    # ── Strategy B: Convexity Scalper / TP-SL (Early Exit) ──────────────────────
    # Take-profit is in CENTS per share (e.g. 25 = 25¢).
    # Stop-loss is in % of the stake risked (e.g. 50 = down 50% of the stake).
    TP_SL_ENABLED: bool = True
    TAKE_PROFIT_CENTS: float = 25.0        # sell if share bid reaches this many cents (25¢)
    TAKE_PROFIT_PCT: float = 25.0          # alias for backwards compatibility
    STOP_LOSS_PCT: float = 50.0            # sell if unrealized loss >= this % of stake (and drop from entry bid)
    STOP_LOSS_GRACE_S: float = 15.0        # min seconds after entry before stop-loss can fire (prevents spread whipsaw)

    # Polymarket — BTC "Up or Down" 5-minute series.
    POLYMARKET_SLUG: str = os.getenv("POLYMARKET_SLUG", "")
    POLYMARKET_SERIES_ID: str = os.getenv("POLYMARKET_SERIES_ID", "10684")
    POLYMARKET_SERIES_SLUG: str = os.getenv("POLYMARKET_SERIES_SLUG", "btc-up-or-down-5m")
    POLYMARKET_AUTO_SELECT_LATEST: bool = os.getenv("POLYMARKET_AUTO_SELECT_LATEST", "true").lower() == "true"
    POLYMARKET_LIVE_DATA_WS_URL: str = os.getenv("POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com")
    POLYMARKET_UP_LABEL: str = os.getenv("POLYMARKET_UP_LABEL", "Up")
    POLYMARKET_DOWN_LABEL: str = os.getenv("POLYMARKET_DOWN_LABEL", "Down")

    # Chainlink
    POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon.drpc.org")
    POLYGON_RPC_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_RPC_URLS", "").split(",") if url.strip()]
    POLYGON_WSS_URL: str = os.getenv("POLYGON_WSS_URL", "wss://polygon-bor-rpc.publicnode.com")
    POLYGON_WSS_URLS: List[str] = [url.strip() for url in os.getenv("POLYGON_WSS_URLS", "").split(",") if url.strip()]
    CHAINLINK_BTC_USD_AGGREGATOR: str = os.getenv("CHAINLINK_BTC_USD_AGGREGATOR", "0xc907E116054Ad103354f2D350FD2514433D57F6f")

    # Alchemy — Polygon RPC used for the on-chain live-trading path (reads/setup).
    ALCHEMY_API_KEY: str = os.getenv("ALCHEMY_API_KEY", "")

    CHAINLINK_AGGREGATORS: Dict[str, str] = {
        "BTC": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "ETH": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "SOL": "0x39771505D18301D239916F4C88367A6010F7D2e3",
        "XRP": "0x3454796324D6469C3110996E2E10972688045F19",
        "DOGE": "0xbAf93Ba318f77363f82E8896a2E830206121D506",
        "BNB": "0x82a6C67606bdc0409f959f60608226064223A57c"
    }

    def get_aggregator(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"): s = s[:-4]
        return self.CHAINLINK_AGGREGATORS.get(s, self.CHAINLINK_BTC_USD_AGGREGATOR)

    def alchemy_rpc_url(self) -> str:
        # Polygon RPC used by the live-trading (gasless) path when a key is set.
        return f"https://polygon-mainnet.g.alchemy.com/v2/{self.ALCHEMY_API_KEY}" if self.ALCHEMY_API_KEY else ""

    # Proxy
    HTTP_PROXY: str = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))
    ALL_PROXY: str = os.getenv("ALL_PROXY", os.getenv("all_proxy", ""))

def normalize_private_key(secret: str) -> str:
    """Accept either a raw hex private key or a 12/24-word seed phrase and return a
    hex private key. EOA only — the wallet is derived from the secret, nothing else.
    Returns "" for empty input. Raises if a seed phrase can't be parsed."""
    secret = (secret or "").strip()
    if not secret:
        return ""
    # A mnemonic is several space-separated words; a private key is a single token.
    if len(secret.split()) >= 12:
        from eth_account import Account
        Account.enable_unaudited_hdwallet_features()
        return Account.from_mnemonic(secret).key.hex()
    return secret


def _is_masked_secret(v) -> bool:
    """True if the value looks like a UI mask rather than a real secret, so we never store
    a placeholder (e.g. '...', 'set', '••••') as an actual key."""
    if not isinstance(v, str):
        return False
    s = v.strip()
    return (not s) or s == "set" or "..." in s or "•" in s or "…" in s


def load_settings():
    base_settings = Settings()
    try:
        from bot import polybolt as _polybolt
        base_settings.PRICE_STREAM = _polybolt.merged({})
    except Exception:
        base_settings.PRICE_STREAM = {}
    config_path = "config.json"
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config_data = json.load(f)
        except Exception as e:
            print(f"Warning: could not read/parse config.json: {e}")
            return base_settings

        # Each section is parsed in isolation: a single bad field logs a warning and is
        # skipped, but never drops the OTHER sections back to code defaults.
        def _section(name, fn):
            try:
                fn()
            except Exception as e:
                print(f"Warning: config.json section '{name}' skipped ({e})")

        def _top():
            if "mode" in config_data: base_settings.MODE = config_data["mode"]
            if "paper_balance_usd" in config_data: base_settings.PAPER_BALANCE_USD = float(config_data["paper_balance_usd"])
            if "private_key" in config_data and not _is_masked_secret(config_data["private_key"]):
                base_settings.PRIVATE_KEY = normalize_private_key(config_data["private_key"])
        _section("top", _top)

        def _relayer():
            rl = config_data.get("relayer", {})
            if "api_key" in rl and not _is_masked_secret(rl["api_key"]):
                base_settings.RELAYER_API_KEY = rl["api_key"]
        if "relayer" in config_data: _section("relayer", _relayer)

        def _capital():
            ce = config_data["capital_extractor"]
            if "enabled" in ce: base_settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
            if "trigger_balance" in ce: base_settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
            if "withdraw_amount" in ce: base_settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
            if "withdraw_address" in ce: base_settings.WITHDRAW_ADDRESS = ce["withdraw_address"]
            if "auto_resume_after_withdrawal" in ce: base_settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume_after_withdrawal"])
            if "resume_after" in ce: base_settings.WITHDRAW_RESUME_AFTER = ce["resume_after"]
        if "capital_extractor" in config_data: _section("capital_extractor", _capital)

        def _poly():
            poly = config_data["polymarket"]
            if "gamma_base_url" in poly: base_settings.GAMMA_BASE_URL = poly["gamma_base_url"]
            if "clob_base_url" in poly: base_settings.CLOB_BASE_URL = poly["clob_base_url"]
            if "live_ws_url" in poly: base_settings.POLYMARKET_LIVE_DATA_WS_URL = poly["live_ws_url"]
            if "series_id" in poly: base_settings.POLYMARKET_SERIES_ID = str(poly["series_id"])
            if "series_slug" in poly: base_settings.POLYMARKET_SERIES_SLUG = poly["series_slug"]
            if "auto_select_latest" in poly: base_settings.POLYMARKET_AUTO_SELECT_LATEST = bool(poly["auto_select_latest"])
            if "up_label" in poly: base_settings.POLYMARKET_UP_LABEL = poly["up_label"]
            if "down_label" in poly: base_settings.POLYMARKET_DOWN_LABEL = poly["down_label"]
        if "polymarket" in config_data: _section("polymarket", _poly)

        def _trading():
            trading = config_data["trading"]
            if "symbol" in trading: base_settings.SYMBOL = trading["symbol"]
            if "binance_base_url" in trading: base_settings.BINANCE_BASE_URL = trading["binance_base_url"]
            if "candle_window_minutes" in trading: base_settings.CANDLE_WINDOW_MINUTES = int(trading["candle_window_minutes"])
            if "poll_interval_ms" in trading: base_settings.POLL_INTERVAL_MS = int(trading["poll_interval_ms"])
            if "risk_type" in trading: base_settings.RISK_TYPE = trading["risk_type"]
            if "risk_value" in trading: base_settings.RISK_VALUE = float(trading["risk_value"])
            if "max_spot_age_s" in trading: base_settings.MAX_SPOT_AGE_S = float(trading["max_spot_age_s"])
            if "max_book_age_s" in trading: base_settings.MAX_BOOK_AGE_S = float(trading["max_book_age_s"])
            if "max_settle_price_age_s" in trading: base_settings.MAX_SETTLE_PRICE_AGE_S = float(trading["max_settle_price_age_s"])
            if "auto_redeem" in trading: base_settings.AUTO_REDEEM_ENABLED = bool(trading["auto_redeem"])
        if "trading" in config_data: _section("trading", _trading)

        def _underdog():
            ud = config_data.get("underdog") or config_data.get("ev", {})
            if "max_price" in ud: base_settings.MAX_UNDERDOG_PRICE = float(ud["max_price"])
            if "max_underdog_price" in ud: base_settings.MAX_UNDERDOG_PRICE = float(ud["max_underdog_price"])
            if "min_prob" in ud:
                base_settings.MIN_UNDERDOG_PROB = float(ud["min_prob"])
                base_settings.MIN_PROB_EV = float(ud["min_prob"])
            if "min_underdog_prob" in ud:
                base_settings.MIN_UNDERDOG_PROB = float(ud["min_underdog_prob"])
                base_settings.MIN_PROB_EV = float(ud["min_underdog_prob"])
            if "min_ev" in ud:
                base_settings.MIN_UNDERDOG_EV = float(ud["min_ev"])
                base_settings.EV_THRESHOLD = float(ud["min_ev"])
            if "ev_threshold" in ud:
                base_settings.MIN_UNDERDOG_EV = float(ud["ev_threshold"])
                base_settings.EV_THRESHOLD = float(ud["ev_threshold"])
            if "max_sigma_distance" in ud: base_settings.MAX_SIGMA_DISTANCE = float(ud["max_sigma_distance"])
            if "momentum_filter" in ud: base_settings.MOMENTUM_FILTER_ENABLED = bool(ud["momentum_filter"])
            if "momentum_lookback_s" in ud: base_settings.MOMENTUM_LOOKBACK_S = float(ud["momentum_lookback_s"])
            if "min_book_liquidity_usd" in ud: base_settings.MIN_BOOK_LIQUIDITY_USD = float(ud["min_book_liquidity_usd"])
        if "underdog" in config_data or "ev" in config_data: _section("underdog", _underdog)

        def _flip():
            flip = config_data["flip"]
            if "enabled" in flip: base_settings.FLIP_ENABLED = bool(flip["enabled"])
            if "min_conviction" in flip: base_settings.FLIP_MIN_CONVICTION = float(flip["min_conviction"])
            if "min_minutes_left" in flip: base_settings.FLIP_MIN_MINUTES_LEFT = float(flip["min_minutes_left"])
        if "flip" in config_data: _section("flip", _flip)

        def _tpsl():
            ts = config_data["tp_sl"]
            if "enabled" in ts: base_settings.TP_SL_ENABLED = bool(ts["enabled"])
            # take_profit_cents / take_profit_pct in cents (e.g. 25 = 25¢)
            if "take_profit_cents" in ts:
                base_settings.TAKE_PROFIT_CENTS = float(ts["take_profit_cents"])
                base_settings.TAKE_PROFIT_PCT = float(ts["take_profit_cents"])
            elif "take_profit_pct" in ts:
                base_settings.TAKE_PROFIT_CENTS = float(ts["take_profit_pct"])
                base_settings.TAKE_PROFIT_PCT = float(ts["take_profit_pct"])
            if "stop_loss_pct" in ts: base_settings.STOP_LOSS_PCT = float(ts["stop_loss_pct"])
            if "stop_loss_grace_s" in ts: base_settings.STOP_LOSS_GRACE_S = float(ts["stop_loss_grace_s"])
        if "tp_sl" in config_data: _section("tp_sl", _tpsl)

        def _chainlink():
            cl = config_data["chainlink"]
            if "polygon_rpc_url" in cl: base_settings.POLYGON_RPC_URL = cl["polygon_rpc_url"]
            if "polygon_wss_url" in cl: base_settings.POLYGON_WSS_URL = cl["polygon_wss_url"]
            if "btc_usd_aggregator" in cl: base_settings.CHAINLINK_BTC_USD_AGGREGATOR = cl["btc_usd_aggregator"]
            if "alchemy_api_key" in cl and not _is_masked_secret(cl["alchemy_api_key"]):
                base_settings.ALCHEMY_API_KEY = cl["alchemy_api_key"]
        if "chainlink" in config_data: _section("chainlink", _chainlink)

        def _price_stream():
            try:
                from bot import polybolt as _polybolt
                base_settings.PRICE_STREAM = _polybolt.normalize(config_data["price_stream"])
            except Exception as e:
                print(f"Warning: Failed to load price_stream config: {e}")
        if "price_stream" in config_data: _section("price_stream", _price_stream)

    return base_settings

settings = load_settings()
