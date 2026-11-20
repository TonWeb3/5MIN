"""
Live trading on Polymarket CLOB V2 via `polymarket-apis` and accelerated native Rust `rust_clob`.

Polymarket migrated to CLOB V2. New wallets trade through the gasless
**deposit-wallet** flow (signature_type=3 / POLY_1271):

  - Funds live in a **deposit wallet** derived from your EOA (deposit via Polymarket).
  - Your **private key / seed (EOA)** signs every order — it holds no funds or gas.
  - A **relayer API key** sponsors on-chain setup (wallet deploy + token approvals),
    so you never pay gas.

Latency Acceleration (Python-Rust Native Pipeline):
  1. Native Rust EIP-712 / Solady TypedDataSign order signing via `k256` ECDSA (~40 microseconds).
  2. Native persistent Keep-Alive HTTP connection pooling via Rust `ureq` (`max_idle_connections: 20`).
  3. Python GIL released (`py.allow_threads`) during order signing and network I/O.
  4. Pre-warmed token fee info, tick sizes, and neg_risk metadata to eliminate preliminary GETs.
  5. Fallback crypto acceleration with C `coincurve` (libsecp256k1) and persistent `httpx.Limits`.

All clients are synchronous — call from the event loop via `asyncio.to_thread`.
"""

import math
import os
import sys
import time
import json
import shutil
import subprocess
import importlib
import threading
from typing import Optional, Dict, Any, List, Tuple
from .config import settings


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _patch_clob_models():
    """Relax over-strict fields in polymarket-apis' models. The live CLOB API omits
    some fields the library marks required (e.g. blockaid_check_enabled), which would
    otherwise crash order placement with a pydantic ValidationError."""
    try:
        import polymarket_apis.types.clob_types as ct
        changed = False
        for name in ("blockaid_check_enabled",):
            f = ct.ClobMarketInfo.model_fields.get(name)
            if f is not None and f.is_required():
                f.default = False
                changed = True
        if changed:
            ct.ClobMarketInfo.model_rebuild(force=True)
    except Exception:
        pass


def _patch_order_builder():
    """Polymarket CLOB V2 strictly enforces:
    - Market buy maker amount: max 2 decimals (collateral/USDC spent)
    - Market buy taker amount: max 4 decimals (tokens/shares received)
    The polymarket-apis library's default ROUNDING_CONFIG specifies amount=5 for 0.001
    and amount=6 for 0.0001 tick sizes, which produces taker amounts with > 4 decimals.
    Polymarket rejects these with:
      HTTP 400 - invalid amounts, the market buy orders maker amount supports a max accuracy of 2 decimals, taker amount a max of 4 decimals.
    This patch clamps ROUNDING_CONFIG to a maximum of 4 decimals and ensures exact integer divisibility:
      maker amount is snapped to 10,000 (10^(6-2) -> max 2 decimals)
      taker amount is snapped to 100 (10^(6-4) -> max 4 decimals)
    """
    try:
        from polymarket_apis.utilities.order_builder.builder import OrderBuilder, ROUNDING_CONFIG, RoundConfig
        for k, rc in list(ROUNDING_CONFIG.items()):
            if rc.amount > 4:
                ROUNDING_CONFIG[k] = RoundConfig(price=rc.price, size=rc.size, amount=min(4, rc.amount))

        orig_get_market = OrderBuilder.get_market_order_amounts

        def safe_get_market_order_amounts(self, side: str, amount: float, price: float, round_config):
            res_side, maker, taker = orig_get_market(self, side, amount, price, round_config)
            if side == "BUY":
                maker = (maker // 10_000) * 10_000
                taker = (taker // 100) * 100
            elif side == "SELL":
                maker = (maker // 100) * 100
                taker = (taker // 10_000) * 10_000
            return res_side, maker, taker

        OrderBuilder.get_market_order_amounts = safe_get_market_order_amounts
    except Exception as e:
        print(f"Warning: Failed to patch OrderBuilder: {e}")


def _patch_crypto_signing():
    """Accelerate EIP-712 cryptographic signing using compiled C coincurve (libsecp256k1).
    Bypasses pure-Python eth-account modular arithmetic, dropping signature creation to sub-0.1ms."""
    try:
        import coincurve
        from eth_account import Account
        from eth_account.datastructures import SignedMessage

        orig_unsafe_sign_hash = Account.unsafe_sign_hash

        def fast_unsafe_sign_hash(message_hash, private_key):
            try:
                if isinstance(private_key, str):
                    pk_bytes = bytes.fromhex(private_key[2:] if private_key.startswith("0x") else private_key)
                elif hasattr(private_key, "key"):
                    pk_bytes = bytes(private_key.key)
                else:
                    pk_bytes = bytes(private_key)

                msg_bytes = bytes.fromhex(message_hash[2:] if str(message_hash).startswith("0x") else message_hash) if isinstance(message_hash, str) else bytes(message_hash)

                pk = coincurve.PrivateKey(pk_bytes)
                sig65 = pk.sign_recoverable(msg_bytes, hasher=None)
                r = int.from_bytes(sig65[:32], "big")
                s = int.from_bytes(sig65[32:64], "big")
                v = sig65[64] + 27
                sig_bytes = sig65[:64] + bytes([v])
                return SignedMessage(
                    messageHash=msg_bytes,
                    r=r,
                    s=s,
                    v=v,
                    signature=sig_bytes,
                )
            except Exception:
                return orig_unsafe_sign_hash(message_hash, private_key)

        Account.unsafe_sign_hash = staticmethod(fast_unsafe_sign_hash)
    except Exception as e:
        print(f"Note: coincurve C-signing patch fallback: {e}")


_patch_clob_models()
_patch_order_builder()
_patch_crypto_signing()


def _ensure_rust_clob():
    """Ensure the native Rust `rust_clob` extension is available and imported.
    Tries site-packages and the local workspace crate `rust_clob`. If not yet installed,
    attempts to compile it via cargo/pip, with graceful fallback to existing compiled binary."""
    proj_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    crate_dir = os.path.join(proj_root, "rust_clob")

    # 1. Try importing immediately
    try:
        import rust_clob
        if hasattr(rust_clob, "post_order"):
            return rust_clob
    except ImportError:
        pass

    # 2. Add crate_dir to sys.path if not present (in case compiled .pyd is in local rust_clob)
    if crate_dir not in sys.path:
        sys.path.insert(0, crate_dir)
        try:
            import rust_clob
            if hasattr(rust_clob, "post_order"):
                return rust_clob
        except ImportError:
            pass

    # 3. If missing, attempt compile
    if not os.path.isdir(crate_dir):
        raise RuntimeError(f"Missing rust_clob crate directory at {crate_dir}")

    env = os.environ.copy()
    cargo_bin = os.path.expanduser("~/.cargo/bin")
    if os.path.isdir(cargo_bin) and cargo_bin not in env.get("PATH", ""):
        env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"

    if not shutil.which("cargo", path=env.get("PATH")):
        if os.name != "nt":
            print("[rust_clob] Rust toolchain not found — installing rustup automatically...")
            try:
                subprocess.run(
                    "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal",
                    shell=True,
                    check=True,
                    env=env,
                )
                env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"
            except Exception as e:
                raise RuntimeError(f"Failed to install rustup: {e}")
        else:
            raise RuntimeError("Rust toolchain (cargo) not found in PATH.")

    print(f"[rust_clob] Compiling/reinstalling native Rust extension from {crate_dir} ...")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-deps", "--force-reinstall", crate_dir],
            check=True,
            env=env,
        )
    except Exception as e:
        print(f"[rust_clob] Compilation warning: {e}. Checking if pre-compiled extension is importable...")

    if "rust_clob" in sys.modules:
        import rust_clob
        return importlib.reload(rust_clob)
    import rust_clob
    return rust_clob


def derive_beacon_deposit_wallet(eoa_address: str) -> str:
    """Derive deterministic ERC-1967 Beacon Proxy deposit wallet address
    introduced in Polymarket's June 29, 2026 upgrade.
    Uses CREATE2 with Solady LibClone initCodeHashERC1967BeaconProxy."""
    from eth_utils import keccak, to_checksum_address, to_bytes
    from eth_abi import encode

    owner = to_checksum_address(eoa_address)
    factory = to_checksum_address("0x00000000000Fb5C9ADea0298D729A0CB3823Cc07")
    beacon = to_checksum_address("0x7A18EDfe055488A3128f01F563e5B479D92ffc3a")

    owner_b32 = to_bytes(hexstr=owner).rjust(32, b"\x00")
    args = encode(["address", "bytes32"], [factory, owner_b32])
    salt = keccak(args)

    prefix = bytes.fromhex("60523d8160223d3973")
    beacon_b20 = to_bytes(hexstr=beacon)
    suffix = bytes.fromhex("60195155f3363d3d373d3d363d602036600436635c60da1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3")
    bytecode_hash = keccak(prefix + beacon_b20 + suffix)

    create2_input = b"\xff" + to_bytes(hexstr=factory) + salt + bytecode_hash
    derived_bytes = keccak(create2_input)[12:]
    return to_checksum_address(derived_bytes)


class ClobTrader:
    def __init__(self):
        self.clob = None          # PolymarketClobClient — order placement
        self.gasless = None       # PolymarketGaslessWeb3Client — relayer on-chain ops
        self.funder: Optional[str] = None         # funded wallet (deposit/proxy/safe)
        self.signature_type: Optional[int] = None
        self.ready = False
        self.last_error: Optional[str] = None
        self._approvals_done = False
        self._api_creds = None
        self._rust_ready = False
        self._lock = threading.Lock()
        self.wallet_source: str = ""
        self._log_hook = None

    def set_log_hook(self, fn):
        self._log_hook = fn

    def _log(self, msg: str):
        fn = self._log_hook
        if fn is not None:
            try:
                fn(msg)
                return
            except Exception:
                pass
        print(msg)

    def reset(self):
        """Drop cached clients so the next call re-initialises with fresh
        credentials (call after the key / relayer settings change)."""
        with self._lock:
            self.clob = None
            self.gasless = None
            self.funder = None
            self.signature_type = None
            self.ready = False
            self.last_error = None
            self._approvals_done = False
            self._api_creds = None
            self._rust_ready = False

    # ── wallet derivation / detection ───────────────────────────────────────────
    def _derive_creds(self):
        """CLOB API creds derived from the key (L1 auth). Cached. Doubles as the
        gasless client's builder_creds when no relayer key is set."""
        if self._api_creds is not None:
            return self._api_creds
        from polymarket_apis.clients.clob_client import PolymarketClobClient
        from eth_account import Account
        eoa = Account.from_key(settings.PRIVATE_KEY).address
        c = PolymarketClobClient(private_key=settings.PRIVATE_KEY, address=eoa,
                                 chain_id=137, signature_type=3)
        self._api_creds = c.create_or_derive_api_creds()
        return self._api_creds

    def _bind_gasless_funder(self, gasless, signature_type: int, funder: Optional[str]):
        if not funder:
            return gasless
        try:
            from web3 import Web3
            chk = Web3.to_checksum_address(funder)
            gasless.address = chk
            if signature_type == 3:
                gasless.get_expected_deposit_wallet = lambda: chk
                gasless.get_deposit_wallet_address = lambda: chk
            elif signature_type == 2:
                gasless.get_safe_proxy_wallet_address = lambda: chk
                if hasattr(gasless, "safe_abi"):
                    gasless.safe = gasless._contract(chk, gasless.safe_abi)
            elif signature_type == 1:
                gasless.get_poly_proxy_wallet_address = lambda: chk
        except Exception:
            pass
        return gasless

    def _new_gasless(self, signature_type: int, funder: Optional[str] = None):
        from polymarket_apis.clients.web3_client import PolymarketGaslessWeb3Client
        kwargs = {"private_key": settings.PRIVATE_KEY, "signature_type": signature_type}
        if settings.RELAYER_API_KEY:
            kwargs["relayer_api_key"] = settings.RELAYER_API_KEY
        else:
            kwargs["builder_creds"] = self._derive_creds()
        if settings.alchemy_rpc_url():
            kwargs["rpc_url"] = settings.alchemy_rpc_url()
        client = PolymarketGaslessWeb3Client(**kwargs)
        if funder:
            self._bind_gasless_funder(client, signature_type, funder)
        return client

    def _candidate_wallets(self, gasless) -> List[Tuple[int, str]]:
        """(signature_type, address) candidates derived from the EOA:
        Modern Beacon deposit wallet first, then legacy UUPS deposit, proxy, safe."""
        out: List[Tuple[int, str]] = []

        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
            beacon = derive_beacon_deposit_wallet(eoa)
            if beacon:
                out.append((3, beacon))
        except Exception:
            pass

        for st, getter in (
            (3, gasless.get_expected_deposit_wallet),
            (1, gasless.get_poly_proxy_wallet_address),
            (2, gasless.get_safe_proxy_wallet_address),
        ):
            try:
                addr = getter()
                if addr and not any(a.lower() == addr.lower() for _, a in out):
                    out.append((st, addr))
            except Exception:
                pass
        return out

    def _pick_funded_wallet(self, gasless) -> Tuple[int, str]:
        """Pick the trading wallet by PRIORITY ORDER, not by largest balance. Candidates are
        probed deposit(3) → proxy(1) → safe(2), and we take the FIRST one holding pUSD.
        Falls back to the deposit wallet only when every candidate reads 0."""
        best = None  # (sig_type, addr, balance)
        for st, addr in self._candidate_wallets(gasless):
            try:
                bal = float(gasless.get_pusd_balance(address=addr))
            except Exception:
                bal = 0.0
            if bal > 0:
                return st, addr
            if best is None or bal > best[2]:
                best = (st, addr, bal)
        if best:
            return best[0], best[1]
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
            return 3, derive_beacon_deposit_wallet(eoa)
        except Exception:
            return 3, gasless.get_expected_deposit_wallet()

    def _init_clients(self):
        from polymarket_apis.clients.clob_client import PolymarketClobClient

        # Any gasless client can derive every candidate address; probe with deposit.
        probe = self._new_gasless(3)
        sig_type, funder = self._pick_funded_wallet(probe)

        gasless = probe if sig_type == 3 else self._new_gasless(sig_type, funder=funder)
        self._bind_gasless_funder(gasless, sig_type, funder)

        clob = PolymarketClobClient(
            private_key=settings.PRIVATE_KEY,
            address=funder,
            chain_id=137,
            signature_type=sig_type,
        )
        clob.set_api_creds(clob.create_or_derive_api_creds())

        # Optimize HTTP client transport: persistent Keep-Alive pool with HTTP/1.1 (bypasses HTTP/2 framing)
        try:
            import httpx
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=30, keepalive_expiry=300.0)
            transport = httpx.HTTPTransport(http1=True, http2=False, limits=limits)
            clob.client = httpx.Client(base_url=clob.base_url, transport=transport, timeout=10.0)
        except Exception:
            pass

        # Initialize native Rust order signer & persistent HTTP/1.1 Keep-Alive sender (sub-millisecond execution)
        rust_mod = _ensure_rust_clob()
        pk_hex = clob.signer.account.key.hex() if hasattr(clob.signer, "account") else clob.signer.private_key
        creds = clob.creds
        rust_mod.init_client(
            str(pk_hex),
            str(funder),
            int(sig_type),
            str(creds.key),
            str(creds.secret),
            str(creds.passphrase),
            str(clob.base_url),
            137,
        )
        self._rust_ready = True

        self.gasless = gasless
        self.clob = clob
        self.funder = funder
        self.signature_type = sig_type
        self.ready = True
        self.last_error = None

        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            eoa = "unknown"

        bal_str = ""
        try:
            bal = float(gasless.get_pusd_balance(address=funder))
            bal_str = f" holds ${bal:.2f}"
        except Exception:
            pass

        self._log(f"LIVE WALLET: EOA {eoa} (from your key) | trading wallet {funder} "
                  f"(sig type {sig_type}, {self.wallet_source or 'auto: Deposit Wallet'}{bal_str}) | "
                  f"API key {str(creds.key)[:7]} made by the EOA")

    def is_ready(self) -> bool:
        return bool(self.ready and self.clob is not None)

    def ensure_ready(self) -> bool:
        if self.ready and self.clob is not None:
            return True
        with self._lock:
            if self.ready and self.clob is not None:
                return True
            if not settings.PRIVATE_KEY:
                self.last_error = "missing_private_key"
                return False
            try:
                self._init_clients()
                if self._rust_ready:
                    # Pre-warm native Rust crypto signing context
                    rust_mod = _ensure_rust_clob()
                    _ = rust_mod.build_signed_order("1", 1000000, 1000000, 0, "FOK", "0", False, False, False)
                return True
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                self.ready = False
                self.clob = None
                return False

    def ensure_setup(self) -> Dict[str, Any]:
        """One-time gasless on-chain setup: deploy the deposit wallet (if needed) and
        set token approvals, sponsored by the relayer key. Required once before the
        first live order on a fresh deposit wallet."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        if self._approvals_done:
            return {"ok": True, "skipped": "already_done"}
        if not settings.RELAYER_API_KEY:
            return {"ok": False, "error": "missing_relayer_api_key"}
        try:
            receipts = self.gasless.set_all_approvals()
            self._approvals_done = True
            return {"ok": True, "approvals": len(receipts or [])}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def warm_token(self, token_id: str, tick_size: str = "0.01", neg_risk: bool = False):
        """Pre-warm token fee, tick size, and neg_risk metadata in the CLOB client
        so order creation and EIP-712 signing execute in sub-millisecond with zero network round trips."""
        if not self.clob or not token_id:
            return
        tid = str(token_id)
        try:
            from polymarket_apis.types.clob_types import FeeInfo
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__fee_infos"):
                if tid not in self.clob._PolymarketReadOnlyClobClient__fee_infos:
                    self.clob._PolymarketReadOnlyClobClient__fee_infos[tid] = FeeInfo(rate=0, exponent=0)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__tick_sizes"):
                self.clob._PolymarketReadOnlyClobClient__tick_sizes[tid] = (tick_size, time.monotonic() + 86400)
            if hasattr(self.clob, "_PolymarketReadOnlyClobClient__neg_risk"):
                self.clob._PolymarketReadOnlyClobClient__neg_risk[tid] = neg_risk
        except Exception:
            pass

    # ── orders ──────────────────────────────────────────────────────────────────
    def _post_order_fast(
        self,
        token_id: str,
        maker_amount: int,
        taker_amount: int,
        side_int: int,
        order_type_str: str,
        expiration_str: str = "0",
        neg_risk: bool = False,
    ) -> Dict[str, Any]:
        """Sign EIP-712 / Solady ERC-1271 order and POST via native Rust (`rust_clob`),
        releasing the GIL during network I/O."""
        rust_mod = _ensure_rust_clob()
        res = rust_mod.post_order(
            str(token_id),
            int(maker_amount),
            int(taker_amount),
            int(side_int),
            str(order_type_str),
            str(expiration_str),
            bool(neg_risk),
            False,
            False,
        )
        http_status = int(res.get("http_status") or 0)
        resp_text = res.get("response_text") or ""
        if not res.get("ok_http"):
            err_detail = resp_text
            try:
                err_json = json.loads(resp_text) if resp_text else {}
                if isinstance(err_json, dict):
                    err_detail = (
                        err_json.get("error")
                        or err_json.get("errorMsg")
                        or err_json.get("message")
                        or resp_text
                    )
            except Exception:
                pass
            if http_status == 0:
                raise RuntimeError(res.get("transport_error") or "rust_clob transport error")
            raise RuntimeError(f"OrderPlacementError: Order placement failed: HTTP {http_status} - {err_detail}")
        data = json.loads(resp_text) if resp_text else {}
        if isinstance(data, dict):
            data["rust_sign_us"] = res.get("sign_time_us")
            data["rust_total_us"] = res.get("total_time_us")
        return data

    @staticmethod
    def _normalize_tick_size(tick_size: Any) -> Optional[str]:
        """Normalize a tick size (e.g. 0.01, '0.001', 0.0001) to a decimal string key
        matching polymarket-apis' ROUNDING_CONFIG ('0.1', '0.01', '0.001', '0.0001')."""
        if tick_size in (None, ""):
            return None
        try:
            v = float(tick_size)
            if v <= 0:
                return None
            return f"{v:.6f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return None

    def _resolve_market_meta(
        self,
        token_id: str,
        tick_size: Optional[str] = None,
        neg_risk: Optional[bool] = None,
    ) -> Tuple[str, bool]:
        """Resolve (tick_size, neg_risk) from explicit args -> cached CLOB metadata -> defaults."""
        tid = str(token_id)
        ts = self._normalize_tick_size(tick_size)
        if ts is None and self.clob is not None:
            cached_ts = getattr(self.clob, "_PolymarketReadOnlyClobClient__tick_sizes", {}).get(tid)
            if isinstance(cached_ts, tuple) and cached_ts:
                ts = self._normalize_tick_size(cached_ts[0])
            elif cached_ts is not None:
                ts = self._normalize_tick_size(cached_ts)
        if ts is None:
            ts = "0.01"

        nr = neg_risk
        if nr is None and self.clob is not None:
            cached_nr = getattr(self.clob, "_PolymarketReadOnlyClobClient__neg_risk", {}).get(tid)
            if cached_nr is not None:
                nr = bool(cached_nr)
        if nr is None:
            nr = False
        return ts, bool(nr)

    def _market_order(
        self,
        token_id,
        amount,
        side: str,
        price,
        order_type=None,
        neg_risk: Optional[bool] = None,
        tick_size: Optional[str] = None,
    ) -> Dict[str, Any]:
        from polymarket_apis.types.clob_types import OrderType
        from polymarket_apis.utilities.order_builder.builder import ROUNDING_CONFIG

        ot = order_type if order_type is not None else OrderType.FOK
        ot_str = str(ot.value if hasattr(ot, "value") else ot)

        resolved_tick, resolved_neg_risk = self._resolve_market_meta(token_id, tick_size=tick_size, neg_risk=neg_risk)
        round_cfg = ROUNDING_CONFIG.get(resolved_tick) or ROUNDING_CONFIG["0.01"]

        # Amount semantics: BUY = USDC to spend (2dp), SELL = share quantity.
        # SELL must be floored to avoid exceeding position size.
        if side == "SELL":
            order_amount = math.floor(float(amount) * 1_000_000) / 1_000_000
        else:
            order_amount = max(1.00, round(float(amount), 2))
        order_price = round(float(price), round_cfg.price) if price else 0

        self.warm_token(token_id, tick_size=resolved_tick, neg_risk=resolved_neg_risk)

        # Polymarket rule: taker amount for BUY must be at least 5 shares
        if side == "BUY" and order_price > 0:
            min_usd = round(max(1.00, math.ceil(5.0 * order_price * 100.0) / 100.0), 2)
            if order_amount < min_usd:
                order_amount = min_usd

        side_int, maker_amt, taker_amt = self.clob.builder.get_market_order_amounts(
            side, order_amount, order_price, round_cfg
        )

        # Fast Rust native order signing & HTTP persistent post (zero Python fallback)
        data = self._post_order_fast(
            token_id=str(token_id),
            maker_amount=maker_amt,
            taker_amount=taker_amt,
            side_int=side_int,
            order_type_str=ot_str,
            expiration_str="0",
            neg_risk=resolved_neg_risk,
        )

        order_id = None
        status = ""
        explicit_fail = (data is None)
        if isinstance(data, dict):
            order_id = data.get("order_id") or data.get("orderID") or data.get("orderId") or data.get("id")
            status = str(data.get("status", "")).lower()
            if data.get("success") is False:
                explicit_fail = True

        FILLED = ("matched", "delayed")
        KILLED = ("unmatched", "cancelled", "canceled", "rejected", "killed", "expired", "invalid")
        if explicit_fail or status in KILLED:
            success = False
        elif status in FILLED:
            success = True
        else:
            success = bool(order_id)

        out = {"ok": success, "response": data, "order_id": order_id, "status": status}
        if not success:
            err = data.get("error_msg") or data.get("errorMsg") or data.get("error")
            out["error"] = err or f"order_not_filled(status={status or 'unknown'})"
        if success:
            out.update(self._parse_fill(data, side))
        return out

    @staticmethod
    def _parse_fill(data: Any, side: str) -> Dict[str, Any]:
        """Actual filled size from the order response.
        OrderPostResponse carries makingAmount/takingAmount:
            BUY  -> making = USDC paid,   taking = shares received
            SELL -> making = shares sold, taking = USDC received
        """
        if not isinstance(data, dict):
            return {}
        def _num(*keys):
            for k in keys:
                v = data.get(k)
                if v not in (None, ""):
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
            return None
        making = _num("making_amount", "makingAmount")
        taking = _num("taking_amount", "takingAmount")
        if making is None or taking is None or making <= 0 or taking <= 0:
            return {}
        shares, usdc = (taking, making) if side == "BUY" else (making, taking)
        price = usdc / shares
        if not (0.0 < price <= 1.0):
            return {}
        return {"filled_shares": shares, "filled_usdc": usdc, "fill_price": price}

    def _ensure_setup_ok(self) -> Optional[str]:
        setup = self.ensure_setup()
        if setup.get("ok"):
            return None
        if setup.get("error") == "missing_relayer_api_key":
            return None  # no relayer -> assume a legacy/pre-approved wallet
        return f"setup_failed: {setup.get('error')}"

    def place_market_buy(
        self,
        token_id: str,
        usdc_amount: float,
        price: Optional[float] = None,
        neg_risk: Optional[bool] = None,
        tick_size: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fill-Or-Kill marketable BUY for `usdc_amount` USDC of `token_id`."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not price or price <= 0:
            return {"ok": False, "error": "missing_quote_price"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            resolved_tick, _ = self._resolve_market_meta(token_id, tick_size=tick_size, neg_risk=neg_risk)
            tick_f = float(resolved_tick)
            limit = min(1.0 - tick_f, max(float(price), float(price) + settings.CLOB_MAX_SLIPPAGE))
            return self._market_order(token_id, usdc_amount, "BUY", limit,
                                      neg_risk=neg_risk, tick_size=resolved_tick)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def place_market_sell(
        self,
        token_id: str,
        size: float,
        price: Optional[float] = None,
        neg_risk: Optional[bool] = None,
        tick_size: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fill-Or-Kill marketable SELL of `size` shares (used to exit / flip / tp / sl)."""
        if not token_id:
            return {"ok": False, "error": "missing_token_id"}
        if not price or price <= 0:
            return {"ok": False, "error": "missing_quote_price"}
        if not size or float(size) <= 0:
            return {"ok": False, "error": "nothing_to_sell"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            resolved_tick, _ = self._resolve_market_meta(token_id, tick_size=tick_size, neg_risk=neg_risk)
            tick_f = float(resolved_tick)
            limit = max(tick_f, min(float(price), float(price) - settings.CLOB_MAX_SLIPPAGE))
            return self._market_order(token_id, size, "SELL", limit,
                                      neg_risk=neg_risk, tick_size=resolved_tick)
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── redemption (winning positions -> spendable pUSD) ────────────────────────
    def get_api_creds(self) -> Optional[Dict[str, str]]:
        if not settings.PRIVATE_KEY:
            return None
        try:
            creds = self._derive_creds()
            return {"apiKey": creds.key, "secret": creds.secret, "passphrase": creds.passphrase}
        except Exception as e:
            self.last_error = f"creds: {type(e).__name__}: {e}"
            return None

    def get_token_balance(self, token_id: str) -> Optional[float]:
        """Outcome-token (ERC-1155) balance held by the trading wallet."""
        if not token_id or not self.ensure_ready():
            return None
        try:
            return float(self.gasless.get_token_balance(str(token_id), address=self.funder))
        except Exception:
            return None

    def enable_auto_redeem(self) -> Dict[str, Any]:
        """Authorise the CTF auto-redeem operator once, so resolved winners are swept into
        pUSD without us sending a redeem per market. Gasless."""
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            self.gasless.auto_redeem_enable()
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def redeem_position(self, condition_id: str, up_shares: float = 0.0,
                        down_shares: float = 0.0, neg_risk: bool = False) -> Dict[str, Any]:
        """Convert resolved winning outcome tokens into pUSD."""
        if not condition_id:
            return {"ok": False, "error": "missing_condition_id"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        err = self._ensure_setup_ok()
        if err:
            return {"ok": False, "error": err}
        try:
            receipt = self.gasless.redeem_position(
                condition_id, [float(up_shares or 0.0), float(down_shares or 0.0)], bool(neg_risk)
            )
            tx = None
            try:
                d = receipt.model_dump() if hasattr(receipt, "model_dump") else dict(receipt)
                tx = d.get("transaction_hash") or d.get("transactionHash") or d.get("hash")
            except Exception:
                tx = str(receipt) if receipt is not None else None
            return {"ok": True, "tx": tx}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── withdrawal (auto capital extractor) ─────────────────────────────────────
    def get_eoa_address(self) -> Optional[str]:
        """The EOA address derived from PRIVATE_KEY — the wallet you control (the
        key/seed owner). Default destination for auto-withdrawals."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            from eth_account import Account
            return Account.from_key(settings.PRIVATE_KEY).address
        except Exception:
            return None

    def withdraw_pusd(self, recipient: str, amount: float) -> Dict[str, Any]:
        """Transfer `amount` pUSD from the funded (deposit) wallet to `recipient`.
        Gasless via the relayer. Used by the auto-withdrawal state machine."""
        if not recipient:
            return {"ok": False, "error": "missing_withdraw_address"}
        if amount is None or float(amount) <= 0:
            return {"ok": False, "error": "invalid_amount"}
        if not self.ensure_ready():
            return {"ok": False, "error": self.last_error or "client_not_ready"}
        try:
            from eth_utils import to_checksum_address, is_address
            if not is_address(recipient):
                return {"ok": False, "error": "invalid_recipient_address"}
            addr = to_checksum_address(recipient)
        except Exception:
            return {"ok": False, "error": "invalid_recipient_address"}
        amt = math.floor(float(amount) * 100) / 100
        if amt <= 0:
            return {"ok": False, "error": "invalid_amount"}
        try:
            receipt = self.gasless.transfer_pusd(addr, amt)
            tx = None
            try:
                data = receipt.model_dump() if hasattr(receipt, "model_dump") else dict(receipt)
                tx = data.get("transaction_hash") or data.get("transactionHash") or data.get("hash")
            except Exception:
                tx = str(receipt) if receipt is not None else None
            return {"ok": True, "tx": tx, "amount": amt, "recipient": addr}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # ── diagnostics / balance ───────────────────────────────────────────────────
    def test_connection(self) -> Dict[str, Any]:
        """Derive the EOA + its candidate wallets and report pUSD balances — shows
        which wallet holds the funds and which signature type will be used. Read-only
        (no relayer key required)."""
        if not settings.PRIVATE_KEY:
            return {"ok": False, "error": "missing_private_key"}
        try:
            from eth_account import Account
            eoa = Account.from_key(settings.PRIVATE_KEY).address
        except Exception as e:
            return {"ok": False, "error": f"invalid_key: {type(e).__name__}: {e}"}
        try:
            probe = self._new_gasless(3)
            wallets = []
            for st, addr in self._candidate_wallets(probe):
                try:
                    bal = float(probe.get_pusd_balance(address=addr))
                except Exception:
                    bal = None
                wallets.append({"signature_type": st, "address": addr, "pusd_balance": bal})
            sig_type, funder = self._pick_funded_wallet(probe)
            return {
                "ok": True,
                "eoa": eoa,
                "chosen_signature_type": sig_type,
                "funder": funder,
                "relayer_key_set": bool(settings.RELAYER_API_KEY),
                "wallets": wallets,
                "rust_clob_available": bool(self._rust_ready or _ensure_rust_clob()),
            }
        except Exception as e:
            return {"ok": False, "eoa": eoa, "error": f"{type(e).__name__}: {e}"}

    def get_usdc_balance(self) -> Optional[float]:
        """pUSD balance of the funded wallet (dollars), or None. pUSD is Polymarket's
        V2 collateral; this is the deposit wallet's tradeable balance."""
        if not settings.PRIVATE_KEY:
            return None
        try:
            if self.ready and self.gasless is not None and self.funder:
                return float(self.gasless.get_pusd_balance(address=self.funder))
            probe = self._new_gasless(3)
            _, funder = self._pick_funded_wallet(probe)
            return float(probe.get_pusd_balance(address=funder))
        except Exception:
            return None


clob_trader = ClobTrader()
