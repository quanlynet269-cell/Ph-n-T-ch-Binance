#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""HarsiBot ALL-IN-ONE — gop HarsiBot_V6_2.py (thu vien chi bao/API) + V6.3 FIXED (engine chay chinh)
vao 1 file duy nhat, khong con phu thuoc import cheo giua 2 file.
PAPER ONLY — chi dung du lieu cong khai Binance, khong dat lenh thuc.
Chay script nay se tu dong khoi dong engine V6.3 FIXED (Phase 1): sua loi + timeframe moi.
"""
from __future__ import annotations
import argparse, json, logging, math, os, shutil, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional
import requests

BASE_DIR = Path(__file__).resolve().parent

# =============================================================================
# PHAN 1: V6.2 — thu vien chi bao + API cong khai Binance (giu nguyen, khong sua)
# =============================================================================
def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError as exc:
        print(f"WARNING: Could not read .env file: {exc}", file=sys.stderr)
load_env_file(BASE_DIR / ".env")

def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}

@dataclass(frozen=True)
class Config:
    base_url: str = os.getenv("BINANCE_FUTURES_BASE_URL", "https://fapi.binance.com")
    quote_asset: str = "USDT"
    min_quote_volume_24h: float = float(os.getenv("MIN_QUOTE_VOLUME_24H", "20000000"))
    max_symbols: int = int(os.getenv("MAX_SYMBOLS", "120"))
    max_open_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
    daily_loss_limit_pct: float = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.03"))
    max_total_open_risk_pct: float = float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "0.025"))
    confirmation_max_age_1h: int = int(os.getenv("CONFIRMATION_MAX_AGE_1H", "6"))
    max_workers: int = int(os.getenv("MAX_WORKERS", "6"))
    scan_interval_sec: int = int(os.getenv("SCAN_INTERVAL_SEC", "180"))
    symbol_refresh_sec: int = int(os.getenv("SYMBOL_REFRESH_SEC", "3600"))
    kline_limit: int = int(os.getenv("KLINE_LIMIT", "300"))
    pivot_lookback: int = 5
    bos_max_age: int = 12
    ob_search_bars: int = 12
    atr_period: int = 14
    rsi_period: int = 14
    volume_lookback: int = 20
    volume_spike_mult: float = 1.30
    harsi_band: float = 15.0
    taker_buy_min: float = 0.60
    taker_sell_max: float = 0.40
    fvg_min_atr: float = 0.05
    displacement_min_atr: float = 0.60
    bos_buffer_atr: float = 0.05
    require_sweep_context: bool = False
    min_rr: float = 2.0
    tp1_r: float = 1.0
    tp2_r: float = 2.0
    final_tp_r: float = 2.5
    stop_atr_buffer: float = 0.15
    risk_per_trade: float = 0.005
    max_leverage: float = 2.0
    starting_equity: float = float(os.getenv("PAPER_STARTING_EQUITY", "10000"))
    taker_fee_rate: float = 0.0005
    slippage_rate: float = 0.0002
    max_spread_pct: float = 0.0015
    compare_mode: bool = True
    execute_balanced_filter: bool = True
    adx_period: int = 14
    adx_min_4h: float = 18.0
    harsi_extension_cap: float = 75.0
    atr_pct_min: float = 0.001
    atr_pct_max: float = 0.06
    vwap_min_bars: int = 3
    state_file: str = os.getenv("HARSI_STATE_FILE", "harsibot_v6_state.json")
    log_file: str = os.getenv("HARSI_LOG_FILE", "harsibot_v6_1.log")
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    telegram_notify_waiting: bool = False
    telegram_notify_cancelled: bool = False
    telegram_retries: int = int(os.getenv("TELEGRAM_RETRIES", "3"))
    telegram_timeout_sec: float = 5.0
    telegram_max_per_cycle: int = 2

CFG = Config()
STATE_PATH = BASE_DIR / CFG.state_file
SERVER_TIME_OFFSET_MS = 0.0
SERVER_TIME_SYNCED = False

def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def vn_now_text() -> str:
    return datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S (Gio VN, GMT+7)")
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default
def is_finite_positive(value: Any) -> bool:
    v = safe_float(value, float("nan"))
    return math.isfinite(v) and v > 0
def direction_sign(direction: str) -> int:
    if direction == "BUY": return 1
    if direction == "SELL": return -1
    raise ValueError(f"Unsupported direction: {direction}")
def align_trend(trend: str, direction: str) -> bool:
    return (direction == "BUY" and trend == "UPTREND") or (direction == "SELL" and trend == "DOWNTREND")

def empty_state(cfg: Config = CFG) -> dict[str, Any]:
    return {
        "version": 6, "equity": cfg.starting_equity, "equity_mark": cfg.starting_equity,
        "unrealized_pnl": 0.0, "peak_equity": cfg.starting_equity, "max_drawdown_pct": 0.0,
        "pending": {}, "positions": {}, "trades": [], "telegram_outbox": [], "telegram_sent_ids": [],
        "last_scan": None, "day_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "day_start_equity": cfg.starting_equity, "daily_halted": False, "last_mark_prices": {},
        "missed_entries": [], "missed_setup_ids": [],
        "v62_comparison": {"baseline_qualified": 0, "balanced_pass": 0, "balanced_reject": 0,
                            "reasons": {}, "evaluated_ids": [], "events": []},
    }

def load_state(path: Path = STATE_PATH, cfg: Config = CFG) -> dict[str, Any]:
    if not path.exists(): return empty_state(cfg)
    try:
        with path.open("r", encoding="utf-8") as f: data = json.load(f)
        if not isinstance(data, dict) or data.get("version") != 6:
            logging.warning("State file is not compatible V6 state; starting a clean state.")
            return empty_state(cfg)
        state = empty_state(cfg); state.update(data)
        for key in ("pending", "positions"):
            if not isinstance(state.get(key), dict): state[key] = {}
        for key in ("trades", "telegram_outbox", "telegram_sent_ids", "missed_entries", "missed_setup_ids"):
            if not isinstance(state.get(key), list): state[key] = []
        if not isinstance(state.get("v62_comparison"), dict):
            state["v62_comparison"] = empty_state(cfg)["v62_comparison"]
        else:
            for key, value in empty_state(cfg)["v62_comparison"].items():
                state["v62_comparison"].setdefault(key, value)
        if not is_finite_positive(state.get("equity")): state["equity"] = cfg.starting_equity
        defaults = empty_state(cfg)
        for key in ("equity_mark", "unrealized_pnl", "max_drawdown_pct", "last_mark_prices"):
            if key not in state: state[key] = defaults[key]
        if not isinstance(state.get("last_mark_prices"), dict): state["last_mark_prices"] = {}
        for position in state["positions"].values():
            if isinstance(position, dict):
                position.setdefault("last_managed_bar", position.get("entry_bar_time"))
                position.setdefault("initial_risk_usdt", safe_float(position.get("risk_usdt")))
        return state
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logging.error("Primary state file unreadable (%s); trying backup.", exc)
        backup_path = path.with_suffix(path.suffix + ".bak")
        if backup_path.exists():
            try:
                with backup_path.open("r", encoding="utf-8") as f: backup = json.load(f)
                if isinstance(backup, dict) and backup.get("version") == 6:
                    state = empty_state(cfg); state.update(backup)
                    for key in ("pending", "positions"):
                        if not isinstance(state.get(key), dict): state[key] = {}
                    for key in ("trades", "telegram_outbox", "telegram_sent_ids", "missed_entries", "missed_setup_ids"):
                        if not isinstance(state.get(key), list): state[key] = []
                    for key in ("equity_mark", "unrealized_pnl", "max_drawdown_pct", "last_mark_prices"):
                        state.setdefault(key, empty_state(cfg)[key])
                    for position in state["positions"].values():
                        if isinstance(position, dict):
                            position.setdefault("last_managed_bar", position.get("entry_bar_time"))
                            position.setdefault("initial_risk_usdt", safe_float(position.get("risk_usdt")))
                    try:
                        corrupt_path = path.with_suffix(path.suffix + f".corrupt.{int(time.time())}")
                        os.replace(path, corrupt_path)
                    except OSError as move_exc:
                        raise RuntimeError(f"Cannot quarantine corrupt primary {path}: {move_exc}") from move_exc
                    save_state(state, path); return state
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                logging.error("Backup state is also unreadable.")
        try:
            corrupt_path = path.with_suffix(path.suffix + f".corrupt.{int(time.time())}")
            os.replace(path, corrupt_path)
        except OSError: pass
        return empty_state(cfg)

def save_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        backup_path = path.with_suffix(path.suffix + ".bak")
        backup_fd, backup_tmp = tempfile.mkstemp(prefix=backup_path.name + ".", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(backup_fd, "wb") as bf, path.open("rb") as cf:
                shutil.copyfileobj(cf, bf); bf.flush(); os.fsync(bf.fileno())
            os.replace(backup_tmp, backup_path)
        except Exception:
            try: os.unlink(backup_tmp)
            except OSError: pass
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, allow_nan=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try: os.unlink(tmp_name)
        except OSError: pass
        raise

class BinanceAPIError(RuntimeError): pass

def api_get(path: str, params: Optional[dict[str, Any]] = None, cfg: Config = CFG,
            retries: int = 3, timeout: int = 15) -> Any:
    url = cfg.base_url.rstrip("/") + path
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code in (418, 429):
                wait = max(safe_float(response.headers.get("Retry-After"), 2.0), min(30.0, 2.0 ** attempt))
                time.sleep(wait); last_error = BinanceAPIError(f"HTTP {response.status_code}: rate limited"); continue
            if response.status_code >= 500:
                last_error = BinanceAPIError(f"HTTP {response.status_code}: server error")
                time.sleep(min(10.0, 1.5 ** attempt)); continue
            if response.status_code != 200:
                raise BinanceAPIError(f"HTTP {response.status_code}: {response.text[:300]}")
            return response.json()
        except (requests.RequestException, ValueError, BinanceAPIError) as exc:
            last_error = exc
            if attempt < retries: time.sleep(min(8.0, 1.5 ** attempt))
    raise BinanceAPIError(f"Request failed for {path}: {last_error}")

def get_futures_universe(cfg: Config = CFG) -> list[str]:
    exchange_info = api_get("/fapi/v1/exchangeInfo", cfg=cfg)
    tickers = api_get("/fapi/v1/ticker/24hr", cfg=cfg)
    if not isinstance(exchange_info, dict) or not isinstance(tickers, list):
        raise BinanceAPIError("Unexpected exchangeInfo/ticker response shape")
    tradable: set[str] = set()
    for item in exchange_info.get("symbols", []):
        if (item.get("status") == "TRADING" and item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == cfg.quote_asset and item.get("symbol", "").endswith(cfg.quote_asset)):
            base = str(item.get("baseAsset", ""))
            if base.endswith(("UP", "DOWN", "BULL", "BEAR")): continue
            tradable.add(item["symbol"])
    liquid: list[tuple[str, float]] = []
    for ticker in tickers:
        symbol = ticker.get("symbol")
        if symbol in tradable:
            qv = safe_float(ticker.get("quoteVolume"))
            if qv >= cfg.min_quote_volume_24h: liquid.append((symbol, qv))
    liquid.sort(key=lambda x: x[1], reverse=True)
    selected = [s for s, _ in liquid[:cfg.max_symbols]]
    if "BTCUSDT" in tradable and "BTCUSDT" not in selected: selected.append("BTCUSDT")
    return selected

def get_book_spreads(cfg: Config = CFG) -> dict[str, float]:
    data = api_get("/fapi/v1/ticker/bookTicker", cfg=cfg)
    if not isinstance(data, list): return {}
    spreads: dict[str, float] = {}
    for item in data:
        bid = safe_float(item.get("bidPrice")); ask = safe_float(item.get("askPrice"))
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
        if mid > 0 and ask >= bid: spreads[item.get("symbol", "")] = (ask - bid) / mid
    return spreads

def get_mark_prices(cfg: Config = CFG) -> dict[str, float]:
    data = api_get("/fapi/v1/premiumIndex", cfg=cfg)
    if isinstance(data, dict): data = [data]
    if not isinstance(data, list): return {}
    prices: dict[str, float] = {}
    for item in data:
        symbol = str(item.get("symbol", "")); price = safe_float(item.get("markPrice"))
        if symbol and price > 0: prices[symbol] = price
    return prices

def sync_binance_server_clock(cfg: Config = CFG) -> bool:
    global SERVER_TIME_OFFSET_MS, SERVER_TIME_SYNCED
    payload = api_get("/fapi/v1/time", cfg=cfg)
    server_ms = int(payload["serverTime"])
    SERVER_TIME_OFFSET_MS = server_ms - (time.time() * 1000.0)
    SERVER_TIME_SYNCED = True
    return True

def get_closed_klines(symbol: str, interval: str, limit: int = 300, cfg: Config = CFG) -> Optional[list[dict[str, Any]]]:
    try:
        raw = api_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}, cfg)
        if not isinstance(raw, list): return None
        now_ms = int(time.time() * 1000 + SERVER_TIME_OFFSET_MS)
        candles: list[dict[str, Any]] = []
        for idx, row in enumerate(raw):
            if not isinstance(row, list) or len(row) < 12: continue
            close_time = int(row[6])
            if close_time >= now_ms: continue
            quote_volume = safe_float(row[7]); buy_quote = safe_float(row[10])
            candles.append({
                "idx": idx, "open_time": int(row[0]), "close_time": close_time,
                "open": safe_float(row[1]), "high": safe_float(row[2]), "low": safe_float(row[3]),
                "close": safe_float(row[4]), "volume": safe_float(row[5]), "quote_volume": quote_volume,
                "trade_count": int(row[8]), "taker_buy_base_volume": safe_float(row[9]),
                "taker_buy_quote_volume": buy_quote,
                "taker_buy_ratio": buy_quote / quote_volume if quote_volume > 0 else 0.5,
            })
        if len(candles) < max(60, cfg.rsi_period + 2 * cfg.pivot_lookback + 10): return None
        return candles
    except (BinanceAPIError, ValueError, TypeError, IndexError) as exc:
        logging.warning("Kline fetch failed %s %s: %s", symbol, interval, exc); return None

def calculate_rsi_series(prices: list[float], period: int = 14) -> list[Optional[float]]:
    n = len(prices); output: list[Optional[float]] = [None] * n
    if period <= 0 or n <= period: return output
    deltas = [prices[i] - prices[i - 1] for i in range(1, n)]
    avg_gain = sum(max(x, 0.0) for x in deltas[:period]) / period
    avg_loss = sum(max(-x, 0.0) for x in deltas[:period]) / period
    def rsi_value(gain, loss):
        if loss <= 0: return 100.0 if gain > 0 else 50.0
        if gain <= 0: return 0.0
        return 100.0 - (100.0 / (1.0 + gain / loss))
    output[period] = rsi_value(avg_gain, avg_loss)
    for i in range(period + 1, n):
        delta = deltas[i - 1]; gain = max(delta, 0.0); loss = max(-delta, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        output[i] = rsi_value(avg_gain, avg_loss)
    return output
    
def calculate_harsi_series(klines: list[dict], period: int = 14) -> list[Optional[dict]]:
    """Heikin-Ashi candles dung tren RSI(Open/High/Low/Close), dong bo theo tung nen."""
    n = len(klines)
    r_open = calculate_rsi_series([k["open"] for k in klines], period)
    r_high = calculate_rsi_series([k["high"] for k in klines], period)
    r_low = calculate_rsi_series([k["low"] for k in klines], period)
    r_close = calculate_rsi_series([k["close"] for k in klines], period)
    result: list[Optional[dict]] = [None] * n
    prev_ha_open = None; prev_ha_close = None
    for i in range(n):
        values = (r_open[i], r_high[i], r_low[i], r_close[i])
        if any(v is None for v in values):
            continue
        ro, rh, rl, rc = (float(v) - 50.0 for v in values)
        ha_close = (ro + rh + rl + rc) / 4.0
        ha_open = (ro + rc) / 2.0 if prev_ha_open is None else (prev_ha_open + prev_ha_close) / 2.0
        ha_high = max(rh, rl, ha_open, ha_close)
        ha_low = min(rh, rl, ha_open, ha_close)
        result[i] = {"open": ha_open, "high": ha_high, "low": ha_low, "close": ha_close}
        prev_ha_open, prev_ha_close = ha_open, ha_close
    return result

def last_harsi_value(klines: list[dict], period: int = 14):
    series = calculate_harsi_series(klines, period)
    for item in reversed(series):
        if item is not None:
            return item["close"], item
    return None, None

def calculate_atr_series(klines: list[dict[str, Any]], period: int = 14) -> list[Optional[float]]:
    n = len(klines); output: list[Optional[float]] = [None] * n
    if n <= period or period <= 0: return output
    true_ranges: list[float] = []
    for i, candle in enumerate(klines):
        if i == 0: true_ranges.append(candle["high"] - candle["low"])
        else:
            prev_close = klines[i - 1]["close"]
            true_ranges.append(max(candle["high"] - candle["low"], abs(candle["high"] - prev_close), abs(candle["low"] - prev_close)))
    atr = sum(true_ranges[1:period + 1]) / period; output[period] = atr
    for i in range(period + 1, n):
        atr = ((atr * (period - 1)) + true_ranges[i]) / period; output[i] = atr
    return output

def find_pivots(klines: list[dict[str, Any]], lookback: int = 5) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    highs: list[tuple[int, float]] = []; lows: list[tuple[int, float]] = []
    if lookback < 1 or len(klines) < lookback * 2 + 1: return highs, lows
    for i in range(lookback, len(klines) - lookback):
        high = klines[i]["high"]; low = klines[i]["low"]
        if all(high > klines[i - j]["high"] and high >= klines[i + j]["high"] for j in range(1, lookback + 1)):
            highs.append((i, high))
        if all(low < klines[i - j]["low"] and low <= klines[i + j]["low"] for j in range(1, lookback + 1)):
            lows.append((i, low))
    return highs, lows

def determine_trend(klines: list[dict[str, Any]], lookback: int = 5):
    highs, lows = find_pivots(klines, lookback)
    if len(highs) < 2 or len(lows) < 2: return "NEUTRAL", highs, lows
    h_prev, h_last = highs[-2][1], highs[-1][1]; l_prev, l_last = lows[-2][1], lows[-1][1]
    if h_last > h_prev and l_last > l_prev: return "UPTREND", highs, lows
    if h_last < h_prev and l_last < l_prev: return "DOWNTREND", highs, lows
    return "RANGE", highs, lows

def check_volume_spike(klines, idx, cfg: Config = CFG) -> bool:
    start = idx - cfg.volume_lookback
    if start < 0 or idx >= len(klines): return False
    previous = [safe_float(klines[j].get("quote_volume")) for j in range(start, idx)]
    avg_volume = sum(previous) / len(previous) if previous else 0.0
    current = safe_float(klines[idx].get("quote_volume"))
    if avg_volume <= 0 or current <= 0: return False
    return current >= avg_volume * cfg.volume_spike_mult

def candle_body_ratio(candle) -> float:
    full_range = candle["high"] - candle["low"]
    return abs(candle["close"] - candle["open"]) / full_range if full_range > 0 else 0.0

def detect_bos_diagnostic(klines, cfg: Config = CFG):
    if len(klines) < max(60, cfg.volume_lookback + cfg.pivot_lookback * 2 + 5):
        return None, "INSUFFICIENT_BOS_DATA"
    atrs = calculate_atr_series(klines, cfg.atr_period)
    lookback = cfg.pivot_lookback
    start_idx = max(lookback * 2, len(klines) - cfg.bos_max_age)
    events: list[dict[str, Any]] = []; rejected_candidates: list[tuple[int, str]] = []
    for i in range(start_idx, len(klines)):
        atr = atrs[i]
        if atr is None or atr <= 0: continue
        candle = klines[i]; prev_close = klines[i - 1]["close"]
        confirmed_highs, confirmed_lows = find_pivots(klines[:i], lookback)
        candidates: list[tuple[str, float]] = []
        if confirmed_highs:
            level = confirmed_highs[-1][1]
            if prev_close <= level and candle["close"] > level + atr * cfg.bos_buffer_atr:
                candidates.append(("BUY", level))
        if confirmed_lows:
            level = confirmed_lows[-1][1]
            if prev_close >= level and candle["close"] < level - atr * cfg.bos_buffer_atr:
                candidates.append(("SELL", level))
        for direction, level in candidates:
            proper_color = (candle["close"] > candle["open"] if direction == "BUY" else candle["close"] < candle["open"])
            body_ratio = candle_body_ratio(candle)
            if not proper_color or body_ratio < 0.55:
                rejected_candidates.append((i, "BOS_CANDLE_QUALITY")); continue
            if not check_volume_spike(klines, i, cfg):
                rejected_candidates.append((i, "VOLUME_SPIKE")); continue
            events.append({"direction": direction, "sub_type": "BOS_UP" if direction == "BUY" else "BOS_DOWN",
                           "level": level, "idx": i, "open_time": candle["open_time"], "close_time": candle["close_time"],
                           "atr": atr, "volume_ok": True, "body_ratio": body_ratio,
                           "taker_buy_ratio": safe_float(candle.get("taker_buy_ratio"), 0.5)})
    if events: return max(events, key=lambda x: x["idx"]), ""
    if rejected_candidates: return None, max(rejected_candidates, key=lambda x: x[0])[1]
    return None, "NO_BOS"

def detect_liquidity_sweep(klines, direction, cfg: Config = CFG, window: int = 10):
    highs, lows = find_pivots(klines, cfg.pivot_lookback)
    atrs = calculate_atr_series(klines, cfg.atr_period)
    start = max(cfg.pivot_lookback * 2, len(klines) - window)
    for i in range(len(klines) - 1, start - 1, -1):
        atr = atrs[i]
        if atr is None: continue
        if direction == "BUY":
            prior = [(idx, value) for idx, value in lows if idx < i - cfg.pivot_lookback]
            if not prior: continue
            p_idx, level = prior[-1]; candle = klines[i]
            if candle["low"] < level - atr * cfg.bos_buffer_atr and candle["close"] > level:
                return {"idx": i, "pivot_idx": p_idx, "level": level, "extreme": candle["low"]}
        else:
            prior = [(idx, value) for idx, value in highs if idx < i - cfg.pivot_lookback]
            if not prior: continue
            p_idx, level = prior[-1]; candle = klines[i]
            if candle["high"] > level + atr * cfg.bos_buffer_atr and candle["close"] < level:
                return {"idx": i, "pivot_idx": p_idx, "level": level, "extreme": candle["high"]}
    return None

def find_valid_fvg(klines, ob_idx, bos_idx, direction, cfg: Config = CFG):
    atrs = calculate_atr_series(klines, cfg.atr_period)
    for i in range(ob_idx, bos_idx - 1):
        atr = atrs[i + 2] if i + 2 < len(atrs) else None
        if atr is None or atr <= 0: continue
        c1, c3 = klines[i], klines[i + 2]
        if direction == "BUY":
            gap_low, gap_high = c1["high"], c3["low"]
            if gap_high <= gap_low or (gap_high - gap_low) < atr * cfg.fvg_min_atr: continue
            filled = any(klines[j]["low"] <= gap_low for j in range(i + 3, len(klines)))
        else:
            gap_low, gap_high = c3["high"], c1["low"]
            if gap_high <= gap_low or (gap_high - gap_low) < atr * cfg.fvg_min_atr: continue
            filled = any(klines[j]["high"] >= gap_high for j in range(i + 3, len(klines)))
        if not filled: return {"idx": i + 1, "low": gap_low, "high": gap_high, "size": gap_high - gap_low, "filled": False}
    return None

def find_order_block_diagnostic(klines, bos, cfg: Config = CFG):
    direction = bos["direction"]; bos_idx = int(bos["idx"]); atr = safe_float(bos.get("atr"))
    start = max(0, bos_idx - cfg.ob_search_bars); ob_idx: Optional[int] = None
    for i in range(bos_idx - 1, start - 1, -1):
        candle = klines[i]
        if direction == "BUY" and candle["close"] < candle["open"]: ob_idx = i; break
        if direction == "SELL" and candle["close"] > candle["open"]: ob_idx = i; break
    if ob_idx is None: return None, "NO_OB_CANDLE"
    ob_candle = klines[ob_idx]; post = klines[ob_idx + 1:bos_idx + 1]
    if not post or atr <= 0: return None, "DISPLACEMENT"
    if direction == "BUY":
        displacement = max(x["close"] for x in post) - ob_candle["high"]
        ob_low, ob_high = ob_candle["low"], ob_candle["high"]
        entry_low, entry_high = ob_low, ob_candle["open"]
        invalidated = any(x["close"] < ob_low for x in klines[ob_idx + 1:])
    else:
        displacement = ob_candle["low"] - min(x["close"] for x in post)
        ob_low, ob_high = ob_candle["low"], ob_candle["high"]
        entry_low, entry_high = ob_candle["open"], ob_high
        invalidated = any(x["close"] > ob_high for x in klines[ob_idx + 1:])
    if invalidated: return None, "OB_INVALIDATED"
    if displacement < atr * cfg.displacement_min_atr: return None, "DISPLACEMENT"
    fvg = find_valid_fvg(klines, ob_idx, bos_idx, direction, cfg)
    if fvg is None: return None, "FVG"
    sweep = detect_liquidity_sweep(klines[:bos_idx + 1], direction, cfg)
    if cfg.require_sweep_context and sweep is None: return None, "SWEEP_REQUIRED"
    return ({"idx": ob_idx, "open_time": ob_candle["open_time"], "low": ob_low, "high": ob_high,
             "entry_low": min(entry_low, entry_high), "entry_high": max(entry_low, entry_high),
             "fvg": fvg, "sweep": sweep, "displacement": displacement}, "")

def calculate_adx_series(klines, period: int = 14) -> list[Optional[float]]:
    n = len(klines); out: list[Optional[float]] = [None] * n
    if period < 2 or n < (period * 2 + 1): return out
    tr = [0.0] * n; plus_dm = [0.0] * n; minus_dm = [0.0] * n
    for i in range(1, n):
        cur, prev = klines[i], klines[i - 1]
        up = safe_float(cur.get("high")) - safe_float(prev.get("high"))
        down = safe_float(prev.get("low")) - safe_float(cur.get("low"))
        plus_dm[i] = up if up > down and up > 0 else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr[i] = max(safe_float(cur.get("high")) - safe_float(cur.get("low")),
                    abs(safe_float(cur.get("high")) - safe_float(prev.get("close"))),
                    abs(safe_float(cur.get("low")) - safe_float(prev.get("close"))))
    sm_tr = sum(tr[1:period + 1]); sm_plus = sum(plus_dm[1:period + 1]); sm_minus = sum(minus_dm[1:period + 1])
    dx_values: list[tuple[int, float]] = []
    for i in range(period, n):
        if i > period:
            sm_tr = sm_tr - sm_tr / period + tr[i]
            sm_plus = sm_plus - sm_plus / period + plus_dm[i]
            sm_minus = sm_minus - sm_minus / period + minus_dm[i]
        if sm_tr <= 0: continue
        pdi = 100.0 * sm_plus / sm_tr; mdi = 100.0 * sm_minus / sm_tr; denom = pdi + mdi
        dx_values.append((i, 100.0 * abs(pdi - mdi) / denom if denom > 0 else 0.0))
    if len(dx_values) < period: return out
    seed = sum(v for _, v in dx_values[:period]) / period
    out[dx_values[period - 1][0]] = seed; adx = seed
    for idx, dx in dx_values[period:]:
        adx = ((adx * (period - 1)) + dx) / period; out[idx] = adx
    return out

def telegram_send_attempt(message: str, cfg: Config = CFG) -> tuple[str, float, str]:
    if not (cfg.telegram_token.strip() and cfg.telegram_chat_id.strip()):
        return "PERMANENT", 0.0, "Telegram is not configured"
    url = f"https://api.telegram.org/bot{cfg.telegram_token}/sendMessage"
    try:
        response = requests.post(url, data={"chat_id": cfg.telegram_chat_id, "text": message,
                                             "disable_web_page_preview": True},
                                  timeout=(3.0, max(1.0, float(cfg.telegram_timeout_sec))))
        try: payload = response.json()
        except ValueError: payload = {}
        if response.status_code == 200 and payload.get("ok") is True: return "SENT", 0.0, ""
        if response.status_code == 429:
            return "RETRY", max(1.0, safe_float((payload.get("parameters") or {}).get("retry_after"), 15.0)), "Telegram rate limited"
        if response.status_code in (408, 425) or response.status_code >= 500:
            return "RETRY", 15.0, f"Telegram temporary HTTP {response.status_code}"
        return "PERMANENT", 0.0, f"Telegram rejected message: {str(payload.get('description', response.text[:200]))[:200]}"
    except requests.RequestException as exc:
        return "RETRY", 15.0, f"Telegram network error: {exc}"
    except Exception as exc:
        return "RETRY", 15.0, f"Unexpected Telegram delivery error: {exc}"

def send_telegram(message: str, cfg: Config = CFG) -> bool:
    status, _, detail = telegram_send_attempt(message, cfg)
    if status == "SENT": return True
    logging.error("Telegram test/send failed: %s", detail); return False

# =============================================================================
# PHAN 2: V6.3 FIXED — engine chinh duoc chay (Phase 1: sua loi + timeframe moi)
# =============================================================================
STATE_DIR = BASE_DIR / "State"; LOG_DIR = BASE_DIR / "Logs"
STATE_DIR.mkdir(exist_ok=True); LOG_DIR.mkdir(exist_ok=True)

SCAN_INTERVAL_SEC = 300
STARTING_EQUITY = 5000.0
RISK_PER_TRADE = 0.005
MAX_OPEN_POSITIONS = 5
MAX_TOTAL_OPEN_RISK_PCT = 0.025
DAILY_LOSS_LIMIT_PCT = 0.03
MAX_LEVERAGE = 2.0
TAKER_FEE_RATE = 0.0005
SLIPPAGE_RATE = 0.0002
MAX_SPREAD_PCT = 0.0015
COOLDOWN_AFTER_STOP_MIN = 60

ADX_MIN = {"ST": 19.0, "LT": 18.0}
STOP_ATR_MULT = {"ST": 1.6, "LT": 1.8}
HARSI_BAND = 15.0  # Tinh tren trend_bars: 1H voi bo ST (1H/30m/15m), 4H voi bo LT (4H/2H/1H).
                    # BUY can HARSI < -15 (qua ban, bat dip); SELL can HARSI > +15 (qua mua, ban bounce).
VOLUME_RATIO_MIN = 1.0
TAKER_BUY_MIN = 0.55
TAKER_SELL_MAX = 0.45
REQUIRE_STRUCTURE = True
STRUCTURE_WINDOW_BARS = {"ST": 96, "LT": 36}
OB_SEARCH_BARS = {"ST": 20, "LT": 16}

TELEGRAM_TOKEN = os.getenv("TELEGRAM_V63FIXED_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_V63FIXED_CHAT_ID", "")

TIMEFRAMES = {"ST": {"trend": "1h", "setup": "30m", "entry": "15m"},
              "LT": {"trend": "4h", "setup": "2h", "entry": "1h"}}

# Cac ma nay LUON duoc phan tich va hien thi dau danh sach uu tien o ca ST/LT,
# du co du dieu kien vao lenh hay khong. Khong vao lenh THAT qua engine nay voi cac ma trong danh sach.
ALWAYS_PRIORITY_SYMBOLS = ["BTCUSDT", "ETHUSDT"]  # them ma vang vao day sau khi ban xac nhan symbol
PRICE_ALERT_PCT = 0.05  # canh bao Telegram khi gia bat ky ma da theo doi di +/-5% so voi lan canh bao truoc

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_DIR / "v63_fixed.log", encoding="utf-8")],
        force=True)

def structure_cfg(variant: str):
    return replace(CFG, bos_max_age=STRUCTURE_WINDOW_BARS[variant], ob_search_bars=OB_SEARCH_BARS[variant])

def ema(values, period):
    out = [None] * len(values)
    if len(values) < period: return out
    alpha = 2 / (period + 1); cur = sum(values[:period]) / period; out[period - 1] = cur
    for i in range(period, len(values)):
        cur = values[i] * alpha + cur * (1 - alpha); out[i] = cur
    return out

def session_vwap(bars):
    if not bars: return None
    day = datetime.fromtimestamp(bars[-1]["open_time"] / 1000, timezone.utc).strftime("%Y-%m-%d")
    chosen = [b for b in bars if datetime.fromtimestamp(b["open_time"] / 1000, timezone.utc).strftime("%Y-%m-%d") == day]
    pv = vol = 0.0
    for b in chosen:
        v = float(b["volume"]); tp = (float(b["high"]) + float(b["low"]) + float(b["close"])) / 3
        pv += tp * v; vol += v
    return pv / vol if vol > 0 else None

def v63_signal(symbol, trend_bars, setup_bars, entry_bars, spread, variant):
    if not trend_bars or not setup_bars or not entry_bars: return None, "DATA"
    c_trend = [x["close"] for x in trend_bars]; c_setup = [x["close"] for x in setup_bars]
    e_trend50 = ema(c_trend, 50); e_setup9 = ema(c_setup, 9); e_setup21 = ema(c_setup, 21)
    e_entry9 = ema([x["close"] for x in entry_bars], 9); e_entry21 = ema([x["close"] for x in entry_bars], 21)
    adx = calculate_adx_series(trend_bars, 14)[-1]
    rsi = calculate_rsi_series(c_setup, 7)[-1]
    atr = calculate_atr_series(entry_bars, 14)[-1]
    harsi, _ = last_harsi_value(trend_bars, 14)
    if None in (e_trend50[-1], e_setup9[-1], e_setup21[-1], e_entry9[-1], e_entry21[-1], adx, rsi, atr, harsi):
        return None, "INDICATOR_WARMUP"
    if spread is None or spread > MAX_SPREAD_PCT: return None, "SPREAD"
    if adx < ADX_MIN[variant]: return None, "ADX_RANGE"
    vwap = session_vwap(setup_bars)
    if not vwap or atr <= 0: return None, "VWAP_ATR"
    last = entry_bars[-1]
    avgvol = sum(x["volume"] for x in setup_bars[-21:-1]) / 20 if len(setup_bars) >= 21 else 0
    volratio = setup_bars[-1]["volume"] / avgvol if avgvol > 0 else 0
    taker = float(setup_bars[-1].get("taker_buy_ratio", 0.5))
    long_trend = c_trend[-1] > e_trend50[-1] and e_setup9[-1] > e_setup21[-1]
    short_trend = c_trend[-1] < e_trend50[-1] and e_setup9[-1] < e_setup21[-1]
    retest_long = last["low"] <= e_entry21[-1] * 1.0008 and last["close"] > e_entry9[-1] and last["close"] > last["open"]
    retest_short = last["high"] >= e_entry21[-1] * 0.9992 and last["close"] < e_entry9[-1] and last["close"] < last["open"]
    if volratio < VOLUME_RATIO_MIN: return None, "VOLUME"
    atr_mult = STOP_ATR_MULT[variant]
    cap_extra = atr * 0.3 * atr_mult
    if long_trend and last["close"] > vwap and 50 <= rsi <= 78 and taker >= TAKER_BUY_MIN and retest_long:
        direction = "BUY"
        # BUY = bat dip trong xu huong tang: can HARSI (tren khung trend) xuong duoi -15 (qua ban).
        if harsi >= -HARSI_BAND: return None, "HARSI_WEAK"
        wick_floor = min(x["low"] for x in entry_bars[-4:])
        atr_floor = last["close"] - atr_mult * atr
        stop = max(min(wick_floor, atr_floor), atr_floor - cap_extra)
    elif short_trend and last["close"] < vwap and 22 <= rsi <= 50 and taker <= TAKER_SELL_MAX and retest_short:
        direction = "SELL"
        # SELL = ban bounce trong xu huong giam: can HARSI (tren khung trend) vuot +15 (qua mua).
        if harsi <= HARSI_BAND: return None, "HARSI_WEAK"
        wick_ceiling = max(x["high"] for x in entry_bars[-4:])
        atr_ceiling = last["close"] + atr_mult * atr
        stop = min(max(wick_ceiling, atr_ceiling), atr_ceiling + cap_extra)
    else:
        return None, "NO_ALIGNMENT"
    structure = None
    if REQUIRE_STRUCTURE:
        scfg = structure_cfg(variant)
        bos, bos_reason = detect_bos_diagnostic(setup_bars, scfg)
        if not bos or bos.get("direction") != direction: return None, "STRUCTURE_BOS"
        ob, ob_reason = find_order_block_diagnostic(setup_bars, bos, scfg)
        if not ob: return None, "STRUCTURE_" + str(ob_reason)
        structure = {"bos_level": float(bos["level"]), "ob_low": float(ob["low"]), "ob_high": float(ob["high"])}
    entry = float(last["close"]); risk = abs(entry - stop)
    if risk <= 0 or risk / entry > 0.025: return None, "INVALID_STOP"
    sign = 1 if direction == "BUY" else -1
    return {"symbol": symbol, "direction": direction, "entry": entry, "stop": float(stop),
            "tp1": entry + sign * risk, "tp2": entry + sign * 2 * risk, "final_tp": entry + sign * 2.5 * risk,
            "bar_time": int(last["open_time"]), "variant": variant, "structure": structure}, "OK"

def pnl_for(direction, entry, exit_price, qty):
    return (exit_price - entry) * qty * (1 if direction == "BUY" else -1)

def send_alert(state, event_id, message):
    sent = state.setdefault("events_sent", [])
    if event_id in sent: return
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        logging.info("ALERT (Telegram chua cau hinh): %s", message); sent.append(event_id); return
    tc = replace(CFG, telegram_token=TELEGRAM_TOKEN, telegram_chat_id=TELEGRAM_CHAT_ID)
    if send_telegram(message, tc):
        sent.append(event_id); state["events_sent"] = sent[-3000:]
    else:
        logging.warning("Gui Telegram that bai cho %s", event_id)

def close_piece(state, sym, p, price, qty, reason):
    direction = p["direction"]
    if reason == "TP1":
        state.setdefault("ui_events", []).append({"type": "TP1", "symbol": sym, "direction": direction,
            "time": datetime.now(timezone.utc).isoformat(), "id": f"TP1:{sym}:{p['entry_time']}"})
        state["ui_events"] = state["ui_events"][-100:]
    elif reason in ("STOP", "BREAKEVEN_AFTER_TP1_SAME_BAR"):
        state.setdefault("ui_events", []).append({"type": "STOP", "symbol": sym, "direction": direction,
            "time": datetime.now(timezone.utc).isoformat(), "id": f"STOP:{sym}:{p['entry_time']}"})
        state["ui_events"] = state["ui_events"][-100:]
    exit_px = price * (1 - SLIPPAGE_RATE if direction == "BUY" else 1 + SLIPPAGE_RATE)
    gross = pnl_for(direction, p["entry"], exit_px, qty); fee = exit_px * qty * TAKER_FEE_RATE
    state["equity"] += gross - fee
    p["qty"] -= qty; p["realized_gross"] += gross; p["fees"] += fee
    if p["qty"] <= max(1e-12, p["initial_qty"] * 1e-9):
        state["trades"].append({"symbol": sym, "direction": direction, "entry_time": p["entry_time"],
            "exit_time": datetime.now(timezone.utc).isoformat(), "entry": p["entry"], "exit": exit_px,
            "net_pnl": p["realized_gross"] - p["fees"], "reason": reason})
        state["positions"].pop(sym, None)
        if reason == "STOP":
            state.setdefault("cooldowns", {})[sym] = time.time() + COOLDOWN_AFTER_STOP_MIN * 60
        send_alert(state, f"CLOSE:{sym}:{p['entry_time']}",
                   f"[V63-FIXED-{p['variant']}] CLOSE {sym} {direction} | {reason} | net {p['realized_gross']-p['fees']:.2f} USDT")

def manage_positions(state, bars_map):
    for sym in list(state["positions"]):
        p = state["positions"].get(sym); bars = bars_map.get(sym) or []
        if not p or not bars: continue
        for bar in bars:
            if int(bar["open_time"]) <= int(p.get("last_bar_time", p["bar_time"])): continue
            p["last_bar_time"] = int(bar["open_time"]); buy = p["direction"] == "BUY"
            def stop_hit_now():
                return bar["low"] <= p["stop"] if buy else bar["high"] >= p["stop"]
            if stop_hit_now():
                close_piece(state, sym, p, p["stop"], p["qty"], "STOP"); break
            if not p.get("tp1_done") and (bar["high"] >= p["tp1"] if buy else bar["low"] <= p["tp1"]):
                q = min(p["qty"], p["initial_qty"] * 0.5)
                close_piece(state, sym, p, p["tp1"], q, "TP1")
                if sym not in state["positions"]: break
                p["tp1_done"] = True; p["stop"] = p["entry"]
                send_alert(state, f"TP1:{sym}:{p['entry_time']}",
                           f"[V63-FIXED-{p['variant']}] TP1 {sym} {p['direction']} | 50% da chot | stop -> breakeven")
                if stop_hit_now():
                    close_piece(state, sym, p, p["stop"], p["qty"], "BREAKEVEN_AFTER_TP1_SAME_BAR"); break
            if p.get("tp1_done") and not p.get("tp2_done") and (bar["high"] >= p["tp2"] if buy else bar["low"] <= p["tp2"]):
                q = min(p["qty"], p["initial_qty"] * 0.25)
                close_piece(state, sym, p, p["tp2"], q, "TP2")
                if sym not in state["positions"]: break
                p["tp2_done"] = True
                send_alert(state, f"TP2:{sym}:{p['entry_time']}",
                           f"[V63-FIXED-{p['variant']}] TP2 {sym} {p['direction']} | 25% ban dau da chot")
            if p.get("tp2_done") and (bar["high"] >= p["final_tp"] if buy else bar["low"] <= p["final_tp"]):
                close_piece(state, sym, p, p["final_tp"], p["qty"], "FINAL_TP"); break

def open_position(state, setup, spread):
    sym = setup["symbol"]; equity = state["equity"]
    if sym in state["positions"] or state.get("daily_halted") or len(state["positions"]) >= MAX_OPEN_POSITIONS:
        return False
    if float(state.get("cooldowns", {}).get(sym, 0)) > time.time():
        return False
    if spread is None or spread > MAX_SPREAD_PCT:
        return False
    entry = setup["entry"] * (1 + SLIPPAGE_RATE if setup["direction"] == "BUY" else 1 - SLIPPAGE_RATE)
    stop = setup["stop"]
    risk_distance = abs(entry - stop)
    if risk_distance <= 0:
        return False
    # Rui ro toan phan: cong them phi round-trip (entry+exit) va slippage khi dong lenh
    # tai stop, giong cach V6.2 goc tinh all_in_risk_per_unit. Thieu buoc nay khien
    # lo thuc te moi lan Stop vuot qua muc risk_per_trade danh nghia.
    all_in_risk_per_unit = (risk_distance + entry * TAKER_FEE_RATE
                            + stop * (TAKER_FEE_RATE + SLIPPAGE_RATE))
    risk_budget = equity * RISK_PER_TRADE
    open_risk = sum(float(p["risk_usdt"]) for p in state["positions"].values())
    if open_risk + risk_budget > equity * MAX_TOTAL_OPEN_RISK_PCT:
        return False
    day_loss = (state["day_start_equity"] - state["equity"]) / max(1, state["day_start_equity"])
    if day_loss >= DAILY_LOSS_LIMIT_PCT:
        state["daily_halted"] = True; return False
    qty = risk_budget / all_in_risk_per_unit
    qty = min(qty, equity * MAX_LEVERAGE / entry)
    if qty <= 0:
        return False
    fee = entry * qty * TAKER_FEE_RATE; state["equity"] -= fee
    p = {**setup, "entry": entry, "qty": qty, "initial_qty": qty, "risk_usdt": qty * all_in_risk_per_unit,
         "fees": fee, "realized_gross": 0.0, "entry_time": datetime.now(timezone.utc).isoformat(),
         "last_bar_time": setup["bar_time"], "tp1_done": False, "tp2_done": False}
    state["positions"][sym] = p
    state.setdefault("ui_events", []).append({
        "type": "ENTRY", "symbol": sym, "direction": p["direction"],
        "time": datetime.now(timezone.utc).isoformat(), "id": f"ENTRY:{sym}:{p['entry_time']}"})
    state["ui_events"] = state["ui_events"][-100:]
    send_alert(state, f"ENTRY:{sym}:{setup['bar_time']}",
               f"[V63-FIXED-{setup['variant']}] ENTRY {sym} {setup['direction']} | entry {entry:.8g} | "
               f"SL {p['stop']:.8g} | TP1 {p['tp1']:.8g} | TP2 {p['tp2']:.8g} | Final {p['final_tp']:.8g}")
    return True
# =============================================================================
# PHAN 3: PHASE 2 — phan loai uu tien, heuristic mo hinh gia, dashboard, canh bao gia
# Canh bao: nhan dien mau hinh va "uu tien" la heuristic TU VIET, khong phai
# thuat toan chuan cong nghiep, co the sai hoac bo lot.
# =============================================================================
MILESTONE_FRACTIONS = [0.5, 0.8, 1.0]

def detect_head_shoulders(bars, lookback=3):
    highs, lows = find_pivots(bars, lookback)
    patterns = []
    for i in range(len(highs) - 2):
        l_idx, l_val = highs[i]; h_idx, h_val = highs[i + 1]; r_idx, r_val = highs[i + 2]
        if h_val > l_val and h_val > r_val:
            shoulder_diff = abs(l_val - r_val) / h_val if h_val > 0 else 1
            if shoulder_diff < 0.03 and (h_val - max(l_val, r_val)) / h_val > 0.01:
                patterns.append({"type": "VAI_DAU_VAI_GIAM", "head_idx": h_idx})
    for i in range(len(lows) - 2):
        l_idx, l_val = lows[i]; h_idx, h_val = lows[i + 1]; r_idx, r_val = lows[i + 2]
        if h_val < l_val and h_val < r_val:
            shoulder_diff = abs(l_val - r_val) / h_val if h_val > 0 else 1
            if shoulder_diff < 0.03 and (min(l_val, r_val) - h_val) / h_val > 0.01:
                patterns.append({"type": "VAI_DAU_VAI_NGUOC_TANG", "head_idx": h_idx})
    # Loc trung: chi giu 1 mau hinh MOI NHAT (head_idx lon nhat) cho moi loai,
    # tranh hien thi nhieu bo ba pivot chong lap cung mot dang mau hinh.
    latest_by_type = {}
    for p in patterns:
        existing = latest_by_type.get(p["type"])
        if existing is None or p["head_idx"] > existing["head_idx"]:
            latest_by_type[p["type"]] = p
    return list(latest_by_type.values())

def v63_condition_report(symbol, trend_bars, setup_bars, entry_bars, spread, variant):
    missing = []
    if not trend_bars or not setup_bars or not entry_bars:
        return {"missing": ["DATA"], "direction": None, "patterns": [], "last_price": 0.0}
    c_trend = [x["close"] for x in trend_bars]; c_setup = [x["close"] for x in setup_bars]
    e_trend50 = ema(c_trend, 50); e_setup9 = ema(c_setup, 9); e_setup21 = ema(c_setup, 21)
    e_entry9 = ema([x["close"] for x in entry_bars], 9); e_entry21 = ema([x["close"] for x in entry_bars], 21)
    adx = calculate_adx_series(trend_bars, 14)[-1]
    rsi = calculate_rsi_series(c_setup, 7)[-1]
    atr = calculate_atr_series(entry_bars, 14)[-1]
    harsi, _ = last_harsi_value(trend_bars, 14)
    if None in (e_trend50[-1], e_setup9[-1], e_setup21[-1], e_entry9[-1], e_entry21[-1], adx, rsi, atr, harsi):
        return {"missing": ["INDICATOR_WARMUP"], "direction": None, "patterns": [], "last_price": entry_bars[-1]["close"]}
    if spread is None or spread > MAX_SPREAD_PCT: missing.append("SPREAD")
    if adx < ADX_MIN[variant]: missing.append("ADX_RANGE")
    vwap = session_vwap(setup_bars)
    last = entry_bars[-1]
    if not vwap or atr <= 0:
        return {"missing": ["VWAP_ATR"], "direction": None, "patterns": [], "last_price": last["close"]}
    avgvol = sum(x["volume"] for x in setup_bars[-21:-1]) / 20 if len(setup_bars) >= 21 else 0
    volratio = setup_bars[-1]["volume"] / avgvol if avgvol > 0 else 0
    taker = float(setup_bars[-1].get("taker_buy_ratio", 0.5))
    long_trend = c_trend[-1] > e_trend50[-1] and e_setup9[-1] > e_setup21[-1]
    short_trend = c_trend[-1] < e_trend50[-1] and e_setup9[-1] < e_setup21[-1]
    retest_long = last["low"] <= e_entry21[-1] * 1.002 and last["close"] > e_entry9[-1] and last["close"] > last["open"]
    retest_short = last["high"] >= e_entry21[-1] * 0.998 and last["close"] < e_entry9[-1] and last["close"] < last["open"]
    if volratio < VOLUME_RATIO_MIN: missing.append("VOLUME")
    direction = "BUY" if long_trend else ("SELL" if short_trend else None)
    if direction is None:
        missing.append("TREND_ALIGNMENT")
    elif direction == "BUY":
        if not (last["close"] > vwap): missing.append("VWAP_SIDE")
        if not (50 <= rsi <= 78): missing.append("RSI_RANGE")
        if not (taker >= TAKER_BUY_MIN): missing.append("TAKER_RATIO")
        if not retest_long: missing.append("RETEST")
        if harsi >= -HARSI_BAND: missing.append("HARSI_WEAK")
    else:
        if not (last["close"] < vwap): missing.append("VWAP_SIDE")
        if not (22 <= rsi <= 50): missing.append("RSI_RANGE")
        if not (taker <= TAKER_SELL_MAX): missing.append("TAKER_RATIO")
        if not retest_short: missing.append("RETEST")
        if harsi <= HARSI_BAND: missing.append("HARSI_WEAK")
    if REQUIRE_STRUCTURE and direction is not None:
        scfg = structure_cfg(variant)
        bos, _ = detect_bos_diagnostic(setup_bars, scfg)
        if not bos or bos.get("direction") != direction:
            missing.append("STRUCTURE_BOS")
        else:
            ob, _ = find_order_block_diagnostic(setup_bars, bos, scfg)
            if not ob: missing.append("STRUCTURE_OB")
    patterns = detect_head_shoulders(setup_bars)
    return {"missing": missing, "direction": direction, "patterns": patterns, "last_price": float(last["close"])}

def classify_priority(report):
    if report["direction"] is None: return False
    return len(report["missing"]) <= 1 or len(report.get("patterns", [])) > 0

def milestone_key(sym, kind, frac):
    return f"MS:{sym}:{kind}:{frac}"

def check_price_milestones(state, sym, p, price):
    direction = p["direction"]; entry = p["entry"]; stop = p["stop"]; tp1 = p["tp1"]
    risk = abs(entry - stop) or 1e-9
    reward = abs(tp1 - entry) or 1e-9
    if direction == "BUY":
        progress_to_tp1 = (price - entry) / reward
        progress_to_stop = (entry - price) / risk
    else:
        progress_to_tp1 = (entry - price) / reward
        progress_to_stop = (price - entry) / risk
    for frac in MILESTONE_FRACTIONS:
        if progress_to_tp1 >= frac:
            send_alert(state, milestone_key(sym, "TP1_PROGRESS", frac),
                       f"[GIA] {sym} {direction} da di {int(frac*100)}% quang duong toi TP1 (gia hien tai {price:.6g})")
        if progress_to_stop >= frac:
            send_alert(state, milestone_key(sym, "STOP_PROGRESS", frac),
                       f"[CANH BAO] {sym} {direction} da di {int(frac*100)}% quang duong ve STOP (gia hien tai {price:.6g})")
                       
PRICE_WATCH_PATH = STATE_DIR / "price_watch_state.json"

def load_price_watch_state():
    if PRICE_WATCH_PATH.exists():
        try:
            return json.loads(PRICE_WATCH_PATH.read_text(encoding="utf-8"))
        except Exception:
            logging.exception("Loi doc price_watch_state.json; tao moi")
    return {}

def save_price_watch_state(d):
    tmp = PRICE_WATCH_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PRICE_WATCH_PATH)

def send_price_watch_alert(message):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        logging.info("ALERT GIA (Telegram chua cau hinh): %s", message)
        return
    tc = replace(CFG, telegram_token=TELEGRAM_TOKEN, telegram_chat_id=TELEGRAM_CHAT_ID)
    if not send_telegram(message, tc):
        logging.warning("Gui Telegram canh bao gia that bai: %s", message)

def check_reference_price_alert(watch_state, sym, price):
    if not price or price <= 0:
        return
    last = watch_state.get(sym)
    if last is None or last <= 0:
        watch_state[sym] = price
        return
    pct_change = (price - last) / last
    if abs(pct_change) >= PRICE_ALERT_PCT:
        direction_txt = "TANG" if pct_change > 0 else "GIAM"
        send_price_watch_alert(
            f"[THEO DOI GIA] {sym} da {direction_txt} {abs(pct_change) * 100:.2f}% so voi lan canh bao truoc "
            f"(tu {last:.6g} -> {price:.6g})")
        watch_state[sym] = price
        
WATCH_SYMBOLS_PATH = STATE_DIR / "watch_symbols.json"

def load_watch_symbols():
    if WATCH_SYMBOLS_PATH.exists():
        try:
            data = json.loads(WATCH_SYMBOLS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [str(s).upper() for s in data]
        except Exception:
            logging.exception("Loi doc watch_symbols.json; tao moi")
    return []

def save_watch_symbols(symbols):
    tmp = WATCH_SYMBOLS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(symbols, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, WATCH_SYMBOLS_PATH)

import uuid
import threading

MANUAL_POSITIONS_FILE = os.path.join("State", "manual_positions.json")
MANUAL_LOCK = threading.Lock()
MANUAL_REPORT = {}        # pos_id -> bao cao hien thi (gia, pnl, sr, canh bao)
MANUAL_SR_CACHE = {}      # pos_id -> {khung: {"support":.., "resistance":..}}
MANUAL_ALERTS_SENT = {}   # pos_id -> set canh bao da gui Telegram (dedup)
MANUAL_SR_TIMEFRAMES = ["15m", "30m", "1h", "2h", "4h"]
MANUAL_SR_ALERT_PCT = 0.003  # canh bao khi gia cach ho tro/khang cu duoi 0.3%


def load_manual_positions():
    if not os.path.exists(MANUAL_POSITIONS_FILE):
        return []
    try:
        with open(MANUAL_POSITIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_manual_positions(positions):
    os.makedirs(os.path.dirname(MANUAL_POSITIONS_FILE), exist_ok=True)
    with open(MANUAL_POSITIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(positions, f, ensure_ascii=False, indent=2)


def add_manual_position(symbol, direction, entry_price, capital, leverage):
    positions = load_manual_positions()
    positions.append({
        "id": uuid.uuid4().hex[:8],
        "symbol": symbol.upper().strip(),
        "direction": direction.upper().strip(),
        "entry": float(entry_price),
        "capital": float(capital),
        "leverage": float(leverage),
        "opened_at": datetime.now(timezone.utc).isoformat(),
    })
    save_manual_positions(positions)


def remove_manual_position(pos_id):
    positions = [p for p in load_manual_positions() if p["id"] != pos_id]
    save_manual_positions(positions)


def calc_manual_pnl(position, current_price):
    sign = 1.0 if position["direction"] == "BUY" else -1.0
    price_pct = (current_price - position["entry"]) / position["entry"] * sign
    roi_pct = price_pct * position["leverage"] * 100.0
    pnl_usdt = position["capital"] * position["leverage"] * price_pct
    return pnl_usdt, roi_pct


def find_nearest_support_resistance(bars, current_price, lookback=50, pivot_window=2):
    """Tim ho tro/khang cu gan nhat dua tren swing high/low (pivot) trong N nen gan nhat."""
    if not bars or len(bars) < pivot_window * 2 + 1:
        return None, None
    recent = bars[-lookback:] if len(bars) > lookback else bars
    n = len(recent)
    highs, lows = [], []
    for i in range(pivot_window, n - pivot_window):
        hi = recent[i]["high"]; lo = recent[i]["low"]
        window = recent[i - pivot_window:i] + recent[i + 1:i + pivot_window + 1]
        if all(hi >= w["high"] for w in window):
            highs.append(hi)
        if all(lo <= w["low"] for w in window):
            lows.append(lo)
    resistances = sorted(h for h in highs if h > current_price)
    supports = sorted((l for l in lows if l < current_price), reverse=True)
    return (supports[0] if supports else None), (resistances[0] if resistances else None)


def fetch_manual_tf_bars(symbols):
    """Lay nen cho CA 5 khung co dinh (15m/30m/1h/2h/4h), doc lap voi bo ST/LT dang chay."""
    result = {sym: {} for sym in symbols}
    def fetch(sym, tf):
        return sym, tf, get_closed_klines(sym, tf, 300)
    jobs = [(sym, tf) for sym in symbols for tf in MANUAL_SR_TIMEFRAMES]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(fetch, sym, tf) for sym, tf in jobs]
        for fu in as_completed(futs):
            try:
                sym, tf, bars = fu.result()
                result[sym][tf] = bars
            except Exception:
                logging.exception("Loi fetch du lieu khung co dinh cho vi the nhap tay")
    return result


def _manual_check_alerts(price, sr):
    alerts = []
    for tf, levels in sr.items():
        res = levels.get("resistance"); sup = levels.get("support")
        if res is not None and abs(price - res) / price <= MANUAL_SR_ALERT_PCT:
            alerts.append(f"{tf}: gia dang GAN KHANG CU {res:.6g}")
        if sup is not None and abs(price - sup) / price <= MANUAL_SR_ALERT_PCT:
            alerts.append(f"{tf}: gia dang GAN HO TRO {sup:.6g}")
    return alerts


def _manual_send_new_alerts(pos, alerts):
    already_sent = MANUAL_ALERTS_SENT.get(pos["id"], set())
    for alert_text in alerts:
        if alert_text not in already_sent:
            send_price_watch_alert(
                f"[VI THE NHAP TAY] {pos['symbol']} {pos['direction']} "
                f"(entry {pos['entry']:.6g}, von {pos['capital']:.2f} x{pos['leverage']:.0f}): {alert_text}")
    MANUAL_ALERTS_SENT[pos["id"]] = set(alerts)


def update_manual_positions_full():
    """Chay 1 lan moi vong quet day du (SCAN_INTERVAL_SEC): lay nen 5 khung,
    tinh lai ho tro/khang cu + canh bao + gui Telegram."""
    positions = load_manual_positions()
    live_ids = {p["id"] for p in positions}
    if not positions:
        with MANUAL_LOCK:
            MANUAL_REPORT.clear(); MANUAL_SR_CACHE.clear(); MANUAL_ALERTS_SENT.clear()
        return
    symbols = {p["symbol"] for p in positions}
    bars_by_symbol = fetch_manual_tf_bars(symbols)
    report, sr_cache = {}, {}
    for pos in positions:
        tf_bars = bars_by_symbol.get(pos["symbol"], {})
        base_bars = tf_bars.get("15m") or next((b for b in tf_bars.values() if b), None)
        if not base_bars:
            continue  # giu nguyen du lieu cu neu fetch lan nay loi, khong xoa
        last_price = float(base_bars[-1]["close"])
        pnl_usdt, roi_pct = calc_manual_pnl(pos, last_price)
        sr = {}
        for tf in MANUAL_SR_TIMEFRAMES:
            bars = tf_bars.get(tf)
            sup, res = find_nearest_support_resistance(bars, last_price) if bars else (None, None)
            sr[tf] = {"support": sup, "resistance": res}
        alerts = _manual_check_alerts(last_price, sr)
        _manual_send_new_alerts(pos, alerts)
        sr_cache[pos["id"]] = sr
        report[pos["id"]] = {"position": pos, "last_price": last_price,
                              "pnl_usdt": pnl_usdt, "roi_pct": roi_pct, "sr": sr, "alerts": alerts}
    with MANUAL_LOCK:
        MANUAL_REPORT.update(report)
        MANUAL_SR_CACHE.update(sr_cache)
        for pid in list(MANUAL_REPORT.keys()):
            if pid not in live_ids: MANUAL_REPORT.pop(pid, None)
        for pid in list(MANUAL_SR_CACHE.keys()):
            if pid not in live_ids: MANUAL_SR_CACHE.pop(pid, None)
        for pid in list(MANUAL_ALERTS_SENT.keys()):
            if pid not in live_ids: MANUAL_ALERTS_SENT.pop(pid, None)


def update_manual_positions_fast(marks):
    """Chay moi 15s (cung fast_price_loop): cap nhat gia/lai-lo ngay lap tuc,
    kiem tra canh bao dua tren SR da cache (khong fetch lai klines moi 15s)."""
    positions = load_manual_positions()
    if not positions:
        return
    with MANUAL_LOCK:
        sr_snapshot = dict(MANUAL_SR_CACHE)
    updated = {}
    for pos in positions:
        price = marks.get(pos["symbol"])
        if not price:
            continue
        pnl_usdt, roi_pct = calc_manual_pnl(pos, price)
        sr = sr_snapshot.get(pos["id"], {})
        alerts = _manual_check_alerts(price, sr)
        _manual_send_new_alerts(pos, alerts)
        updated[pos["id"]] = {"position": pos, "last_price": price,
                               "pnl_usdt": pnl_usdt, "roi_pct": roi_pct, "sr": sr, "alerts": alerts}
    with MANUAL_LOCK:
        MANUAL_REPORT.update(updated)


def build_sr_report(trend_bars, setup_bars, entry_bars, current_price, alert_pct=0.003):
    tf_map = {"Trend": trend_bars, "Setup": setup_bars, "Entry": entry_bars}
    sr = {}
    alerts = []
    for label, bars in tf_map.items():
        sup, res = find_nearest_support_resistance(bars, current_price)
        sr[label] = {"support": sup, "resistance": res}
        if res is not None and abs(current_price - res) / current_price <= alert_pct:
            alerts.append(f"{label}: gia dang GAN KHANG CU {res:.6g}")
        if sup is not None and abs(current_price - sup) / current_price <= alert_pct:
            alerts.append(f"{label}: gia dang GAN HO TRO {sup:.6g}")
    return sr, alerts

def render_dashboard(state_st, state_lt, priority_st, priority_lt, end_ts, marks=None):
    marks = marks or {}
    os.system("cls" if os.name == "nt" else "clear")
    lines = ["=" * 110, f"HARSIBOT V6.3 DASHBOARD (PAPER ONLY) | Cap nhat: {utc_now_text()}", "=" * 110]
    for label, state, priority in (("ST (1H/30m/15m)", state_st, priority_st), ("LT (4H/2H/1H)", state_lt, priority_lt)):
        lines.append(f"\n--- {label} | Equity: {state['equity']:.2f} USDT | Vi the mo: {len(state['positions'])} ---")
        if state["positions"]:
            lines.append(f"{'Symbol':<12}{'Huong':<6}{'GiaHT':<14}{'Entry':<14}{'Stop':<14}{'TP1':<14}{'TP2':<14}{'Final':<14}{'TP1':<6}{'TP2':<6}")
            for sym, p in state["positions"].items():
                cur = marks.get(sym, 0.0)
                lines.append(f"{sym:<12}{p['direction']:<6}{cur:<14.6g}{p['entry']:<14.6g}{p['stop']:<14.6g}"
                             f"{p['tp1']:<14.6g}{p['tp2']:<14.6g}{p['final_tp']:<14.6g}"
                             f"{'OK' if p.get('tp1_done') else '-':<6}{'OK' if p.get('tp2_done') else '-':<6}")
        else:
            lines.append("  (chua co vi the mo)")
        if priority:
            lines.append(f"\n  Uu tien phan tich ({len(priority)} ma, da an cac ma khong du dieu kien):")
            for sym, rep in priority.items():
                cur = marks.get(sym, rep.get("last_price", 0.0))
                missing_txt = ",".join(rep["missing"]) or "KHONG CON THIEU"
                pattern_txt = ",".join(p["type"] for p in rep.get("patterns", [])) or "-"
                lines.append(f"  {sym:<10} {str(rep['direction']):<5} gia={cur:<12.6g} thieu=[{missing_txt}] mau_hinh=[{pattern_txt}]")
        else:
            lines.append("  (khong co ma uu tien trong vong quet nay)")
    remaining = max(0, int(end_ts - time.time()))
    lines.append(f"\nLan quet day du tiep theo trong {remaining}s... (Ctrl+C de dung)")
    print("\n".join(lines))
    # =============================================================================
# PHAN 4: WEB DASHBOARD NHE (Flask) — chi hien thi, khong dieu khien bot
# =============================================================================
import threading
try:
    from flask import Flask, Response, jsonify, request, redirect
except ImportError:
    Flask = None

DASHBOARD_DATA = {"state_st": None, "state_lt": None, "priority_st": {}, "priority_lt": {},
                  "marks": {}, "end_ts": 0.0, "updated_at": ""}
DASHBOARD_LOCK = threading.Lock()

def update_dashboard_data(state_st, state_lt, priority_st, priority_lt, marks, end_ts, watch_symbols=None):
    with DASHBOARD_LOCK:
        prev_marks = DASHBOARD_DATA.get("marks") or {}
        DASHBOARD_DATA.update(state_st=state_st, state_lt=state_lt, priority_st=priority_st,
                               priority_lt=priority_lt, marks=marks, prev_marks=prev_marks,
                               end_ts=end_ts, updated_at=vn_now_text(),
                               watch_symbols=list(watch_symbols or []))

def _html_escape(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    
MISSING_LABEL_VN = {
    "DATA": "Thieu du lieu",
    "INDICATOR_WARMUP": "Chi bao chua du du lieu khoi dong",
    "SPREAD": "Spread qua cao",
    "ADX_RANGE": "ADX yeu (thi truong di ngang)",
    "VWAP_ATR": "Thieu VWAP/ATR hop le",
    "VOLUME": "Khoi luong giao dich chua dat",
    "TREND_ALIGNMENT": "Xu huong chua ro rang",
    "VWAP_SIDE": "Gia chua dung phia VWAP",
    "RSI_RANGE": "RSI ngoai khoang cho phep",
    "TAKER_RATIO": "Ty le Taker Buy/Sell chua dat",
    "RETEST": "Chua retest vung EMA",
    "STRUCTURE_BOS": "Chua co pha vo cau truc (BOS) dung huong",
    "STRUCTURE_OB": "Chua co Order Block hop le",
    "HARSI_WEAK": "HARSI khung lon chua vuot nguong xac nhan (+15/-15)",
}

def _translate_missing(codes):
    return [MISSING_LABEL_VN.get(code, code) for code in codes]

NEW_POSITION_HIGHLIGHT_SEC = 180  # highlight mau cho vi the moi mo trong 180s gan nhat

def _position_pnl_pct(p, cur):
    entry = p.get("entry", 0.0)
    if not cur or not entry:
        return None
    if p["direction"] == "BUY":
        return (cur - entry) / entry * 100.0
    return (entry - cur) / entry * 100.0

def _is_new_position(p):
    entry_time = p.get("entry_time")
    if not entry_time:
        return False
    try:
        opened = datetime.fromisoformat(entry_time)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - opened).total_seconds() <= NEW_POSITION_HIGHLIGHT_SEC
    
CLOSED_HIGHLIGHT_SEC = 180  # highlight mau cho lenh vua dong trong 180s gan nhat

def _is_recent_close(trade):
    exit_time = trade.get("exit_time")
    if not exit_time:
        return False
    try:
        closed = datetime.fromisoformat(exit_time)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - closed).total_seconds() <= CLOSED_HIGHLIGHT_SEC

def _render_recent_closes_table(state):
    recent = [t for t in reversed(state.get("trades", [])) if _is_recent_close(t)]
    if not recent:
        return ""
    rows = ""
    for t in recent:
        net = t.get("net_pnl", 0.0)
        color = "#4CAF50" if net >= 0 else "#F44336"
        rows += (f"<tr class='closed-position'><td>{_html_escape(t.get('symbol',''))}</td>"
                 f"<td>{t.get('direction','')}</td><td>{_html_escape(t.get('reason',''))}</td>"
                 f"<td style='color:{color}; font-weight:bold;'>{net:+.2f} USDT</td>"
                 f"<td>{_html_escape(t.get('exit_time',''))}</td></tr>")
    return ("<h4>Vua dong lenh gan day</h4>"
            "<table border='1' cellpadding='4' cellspacing='0'><tr><th>Symbol</th><th>Huong</th>"
            "<th>Ly do</th><th>Net P&amp;L</th><th>Thoi gian dong</th></tr>" + rows + "</table>")

def _unrealized_total(state, marks):
    total = 0.0
    for sym, p in state.get("positions", {}).items():
        cur = marks.get(sym, 0.0)
        if not cur:
            continue
        total += pnl_for(p["direction"], p["entry"], cur, p["qty"])
    return total
    
def _priority_sort_key(sym, watch_symbols):
    if sym in ALWAYS_PRIORITY_SYMBOLS:
        return (0, ALWAYS_PRIORITY_SYMBOLS.index(sym))
    if sym in watch_symbols:
        return (1, sym)
    return (2, sym)
    
def v63_stats(state):
    trades = state.get("trades", [])
    wins = [t for t in trades if safe_float(t.get("net_pnl")) > 0]
    losses = [t for t in trades if safe_float(t.get("net_pnl")) < 0]
    gross_profit = sum(safe_float(t.get("net_pnl")) for t in wins)
    gross_loss = abs(sum(safe_float(t.get("net_pnl")) for t in losses))
    closed = len(trades)
    win_rate = (len(wins) / closed * 100.0) if closed else 0.0
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = float("inf") if gross_profit > 0 else 0.0
    net_pnl = sum(safe_float(t.get("net_pnl")) for t in trades)
    return {"closed_trades": closed, "wins": len(wins), "losses": len(losses),
            "win_rate_pct": win_rate, "profit_factor": profit_factor, "net_pnl": net_pnl}

def _render_stats_box(label, stats):
    pf = stats["profit_factor"]
    pf_txt = "∞" if math.isinf(pf) else f"{pf:.2f}"
    color = "#4CAF50" if stats["net_pnl"] >= 0 else "#F44336"
    return (f"<div class='stats-box'><b>Thong ke {_html_escape(label)}:</b> "
            f"Lenh da dong: {stats['closed_trades']} (Thang {stats['wins']} / Thua {stats['losses']}) &nbsp;|&nbsp; "
            f"Win rate: {stats['win_rate_pct']:.1f}% &nbsp;|&nbsp; Profit Factor: {pf_txt} &nbsp;|&nbsp; "
            f"Net P&amp;L da thuc hien: <span style='color:{color}; font-weight:bold;'>{stats['net_pnl']:+.2f} USDT</span></div>")
            
def _render_positions_table(state, marks, prev_marks):
    if not state["positions"]:
        return "<p><i>(chua co vi the mo)</i></p>"
    items = sorted(state["positions"].items(),
                   key=lambda kv: kv[1].get("entry_time", ""), reverse=True)
    rows = ""
    for sym, p in items:
        cur = marks.get(sym, 0.0)
        prev = prev_marks.get(sym)
        price_color = ("#4CAF50" if (prev is not None and cur > prev)
                       else "#F44336" if (prev is not None and cur < prev) else "#eee")
        dir_color = "#4CAF50" if p["direction"] == "BUY" else "#F44336"
        pnl_pct = _position_pnl_pct(p, cur)
        if pnl_pct is None:
            pnl_cell = "<td>-</td>"
        else:
            color = "#4CAF50" if pnl_pct >= 0 else "#F44336"
            pnl_cell = f"<td style='color:{color}; font-weight:bold;'>{pnl_pct:+.2f}%</td>"
        row_class = " class='new-position'" if _is_new_position(p) else ""
        rows += (f"<tr{row_class}>"
                 f"<td style='color:#FFD700; font-weight:bold;'>{_html_escape(sym)}</td>"
                 f"<td style='color:{dir_color}; font-weight:bold;'>{p['direction']}</td>"
                 f"<td style='color:{price_color};'>{cur:.6g}</td>"
                 f"<td>{p['entry']:.6g}</td><td>{p['stop']:.6g}</td><td>{p['tp1']:.6g}</td>"
                 f"<td>{p['tp2']:.6g}</td><td>{p['final_tp']:.6g}</td>"
                 f"<td>{'OK' if p.get('tp1_done') else '-'}</td><td>{'OK' if p.get('tp2_done') else '-'}</td>"
                 f"{pnl_cell}</tr>")
    return ("<table border='1' cellpadding='4' cellspacing='0'><tr><th>Symbol</th><th>Huong</th>"
            "<th>Gia HT</th><th>Entry</th><th>Stop</th><th>TP1</th><th>TP2</th><th>Final</th>"
            f"<th>TP1 done</th><th>TP2 done</th><th>P&amp;L %</th></tr>{rows}</table>")
            
def _render_priority_table(priority, marks, prev_marks, watch_symbols):
    if not priority:
        return "<p><i>(khong co ma uu tien trong vong quet nay)</i></p>"
    items = sorted(priority.items(), key=lambda kv: _priority_sort_key(kv[0], watch_symbols))
    rows = ""
    for sym, rep in items:
        cur = marks.get(sym, rep.get("last_price", 0.0))
        prev = prev_marks.get(sym)
        price_color = ("#4CAF50" if (prev is not None and cur > prev)
                       else "#F44336" if (prev is not None and cur < prev) else "#eee")
        missing_txt = ", ".join(_translate_missing(rep["missing"])) or "KHONG CON THIEU"
        pattern_txt = ", ".join(p["type"] for p in rep.get("patterns", [])) or "-"
        direction = rep["direction"]
        if direction == "BUY":
            dir_html = "<span style='color:#4CAF50; font-weight:bold;'>BUY</span>"
        elif direction == "SELL":
            dir_html = "<span style='color:#F44336; font-weight:bold;'>SELL</span>"
        else:
            dir_html = "-"
        if sym in ALWAYS_PRIORITY_SYMBOLS:
            style = " style='background-color:#1a3a5c;'"
            label = f"{_html_escape(sym)} (THAM CHIEU THI TRUONG)"
        elif sym in watch_symbols:
            style = " style='background-color:#3a2f1a;'"
            label = (f"{_html_escape(sym)} (DANG THEO DOI) "
                     f"<a href='#' onclick=\"removeWatchSymbol('{sym}'); return false;\" "
                     f"style='color:#F44336; text-decoration:none;'>[Bo theo doi]</a>")
        else:
            style = ""
            label = _html_escape(sym)
        rows += (f"<tr{style}><td style='color:#FFD700; font-weight:bold;'>{label}</td>"
                 f"<td>{dir_html}</td><td style='color:{price_color};'>{cur:.6g}</td>"
                 f"<td>{_html_escape(missing_txt)}</td><td>{_html_escape(pattern_txt)}</td></tr>")
    return ("<table border='1' cellpadding='4' cellspacing='0'><tr><th>Symbol</th><th>Huong</th>"
            f"<th>Gia</th><th>Thieu dieu kien</th><th>Mau hinh</th></tr>{rows}</table>")         

def render_dashboard_html():
    with DASHBOARD_LOCK:
        d = dict(DASHBOARD_DATA)
    if d["state_st"] is None:
        return "<html><body><h2>Dang khoi dong vong quet dau tien, doi vai phut...</h2></body></html>"
    remaining = max(0, int(d["end_ts"] - time.time()))
    marks = d["marks"]
    prev_marks = d.get("prev_marks") or {}
    watch_list = d.get("watch_symbols") or []
    watch_set = set(watch_list)
    watch_list_html = ", ".join(
        f"{_html_escape(s)} <a href='#' onclick=\"removeWatchSymbol('{s}'); return false;\" "
        f"style='color:#F44336; text-decoration:none;'>[x]</a>" for s in watch_list
    ) or "(chua co)"
    unreal_st = _unrealized_total(d['state_st'], marks)
    unreal_lt = _unrealized_total(d['state_lt'], marks)
    total_equity = d['state_st']['equity'] + d['state_lt']['equity'] + unreal_st + unreal_lt
    total_start = STARTING_EQUITY * 2
    total_pnl = total_equity - total_start
    total_pnl_pct = (total_pnl / total_start * 100.0) if total_start else 0.0
    total_color = "#4CAF50" if total_pnl >= 0 else "#F44336"
    stats_st = v63_stats(d['state_st'])
    stats_lt = v63_stats(d['state_lt'])
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Phan Tich</title>
<style>
body {{ font-family: Consolas, monospace; background:#111; color:#eee; padding:16px; }}
h2 {{ color:#4CAF50; }} table {{ border-color:#444; margin-bottom:20px; }}
th, td {{ padding:6px 10px; }} th {{ background:#222; }}
.new-position {{ background-color:#2e7d32 !important; animation: pulse-new 1.4s ease-in-out infinite alternate; }}
@keyframes pulse-new {{ from {{ background-color:#2e7d32; }} to {{ background-color:#55c15b; }} }}
.closed-position {{ background-color:#5d4037 !important; animation: pulse-closed 1.4s ease-in-out infinite alternate; }}
@keyframes pulse-closed {{ from {{ background-color:#5d4037; }} to {{ background-color:#8d6e63; }} }}
.total-pnl-box {{ border:1px solid #444; padding:10px 16px; margin-bottom:16px; display:inline-block; }}
.stats-box {{ border:1px dashed #555; padding:8px 14px; margin-bottom:12px; background:#1a1a1a; }}
#soundToggleBtn {{ background:#222; color:#eee; border:1px solid #555; padding:8px 14px; cursor:pointer; margin-left:12px; font-size:14px; }}
#watchSymbolInput {{ background:#222; color:#eee; border:1px solid #555; padding:4px 8px; }}
.watch-btn {{ background:#222; color:#eee; border:1px solid #555; padding:4px 10px; cursor:pointer; }}
</style>
<script>
const lastUpdated = "{d['updated_at']}";
async function pollStatus() {{
    try {{
        const res = await fetch('/api/status');
        const data = await res.json();
        if (data.updated_at && data.updated_at !== lastUpdated) {{ location.reload(); }}
    }} catch (e) {{ /* bo qua loi mang tam thoi, se thu lai lan poll sau */ }}
}}
setInterval(pollStatus, 3000);
const endTs = {d['end_ts']};
function tickCountdown() {{
    const remain = Math.max(0, Math.floor(endTs - Date.now() / 1000));
    const el = document.getElementById('countdown');
    if (el) el.textContent = remain + 's';
}}
setInterval(tickCountdown, 1000);

function isSoundEnabled() {{ return localStorage.getItem('harsibot_sound') !== 'off'; }}
function updateSoundBtn() {{
    const btn = document.getElementById('soundToggleBtn');
    if (btn) btn.textContent = isSoundEnabled() ? 'Am thanh: DANG BAT (bam de TAT)' : 'Am thanh: DANG TAT (bam de BAT)';
}}
function toggleSound() {{
    localStorage.setItem('harsibot_sound', isSoundEnabled() ? 'off' : 'on');
    updateSoundBtn();
    if (isSoundEnabled()) speak('Da bat am thanh canh bao');
}}
function speak(text) {{
    if (!isSoundEnabled() || !('speechSynthesis' in window)) return;
    const u = new SpeechSynthesisUtterance(text);
    u.lang = 'vi-VN';
    window.speechSynthesis.speak(u);
}}
async function checkEvents() {{
    try {{
        const res = await fetch('/api/events');
        const data = await res.json();
        const spoken = JSON.parse(localStorage.getItem('harsibot_spoken_ids') || '[]');
        const spokenSet = new Set(spoken);
        const newSpoken = spoken.slice();
        for (const ev of (data.events || [])) {{
            if (spokenSet.has(ev.id)) continue;
            let text = '';
            if (ev.type === 'ENTRY') text = `Da co ma ${{ev.symbol}} co the vao lenh`;
            else if (ev.type === 'STOP') text = `Ma giao dich ${{ev.symbol}} da Stop Loss`;
            else if (ev.type === 'TP1') text = `Ma giao dich ${{ev.symbol}} da den Target 1`;
            if (text) speak(text);
            newSpoken.push(ev.id);
        }}
        localStorage.setItem('harsibot_spoken_ids', JSON.stringify(newSpoken.slice(-200)));
    }} catch (e) {{ /* bo qua loi mang tam thoi */ }}
}}
async function addWatchSymbol() {{
    const el = document.getElementById('watchSymbolInput');
    const sym = el.value.trim().toUpperCase();
    if (!sym) return;
    await fetch('/api/watch/add', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{symbol: sym}})}});
    location.reload();
}}
async function removeWatchSymbol(sym) {{
    await fetch('/api/watch/remove', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify({{symbol: sym}})}});
    location.reload();
}}
window.addEventListener('DOMContentLoaded', () => {{ updateSoundBtn(); checkEvents(); tickCountdown(); }});
</script>
</head><body>
<h2>PHAN TICH
<button id="soundToggleBtn" onclick="toggleSound()">Am thanh</button>
<a href="/manual" style="color:#FFD700; margin-left:12px; font-size:16px;">[Vi the nhap tay]</a>
</h2>
<h3 style="color:#FFD700; margin-top:-10px;">Coin Binance (PAPER ONLY — khong dat lenh thuc)</h3>
<div class="total-pnl-box">
  <b>Tong P&amp;L (ST+LT, da tinh ca lai/lo tam tinh):</b>
  <span style="color:{total_color}; font-weight:bold;">{total_pnl:+.2f} USDT ({total_pnl_pct:+.2f}%)</span>
  &nbsp;|&nbsp; Tong equity hien tai: {total_equity:,.2f} USDT / Von khoi diem: {total_start:,.2f} USDT
</div>
<div class="total-pnl-box">
  <b>Theo doi them ma (ghim len dau, chi theo doi, khong tu vao lenh):</b>
  <input id="watchSymbolInput" type="text" placeholder="VD: SOLUSDT">
  <button class="watch-btn" onclick="addWatchSymbol()">Theo doi</button>
  &nbsp;|&nbsp; Dang theo doi: {watch_list_html}
</div>
<p>Cap nhat (gio Viet Nam): {d['updated_at']} | Lan quet day du tiep theo trong: <b id="countdown">{remaining}s</b></p>
<h3>ST (1H / 30m / 15m) | Equity: {d['state_st']['equity']:.2f} USDT | Vi the mo: {len(d['state_st']['positions'])}</h3>
{_render_stats_box("ST", stats_st)}
{_render_positions_table(d['state_st'], marks, prev_marks)}
{_render_recent_closes_table(d['state_st'])}
<h4>Uu tien phan tich ST ({len(d['priority_st'])} ma)</h4>
{_render_priority_table(d['priority_st'], marks, prev_marks, watch_set)}
<h3>LT (4H / 2H / 1H) | Equity: {d['state_lt']['equity']:.2f} USDT | Vi the mo: {len(d['state_lt']['positions'])}</h3>
{_render_stats_box("LT", stats_lt)}
{_render_positions_table(d['state_lt'], marks, prev_marks)}
{_render_recent_closes_table(d['state_lt'])}
<h4>Uu tien phan tich LT ({len(d['priority_lt'])} ma)</h4>
{_render_priority_table(d['priority_lt'], marks, prev_marks, watch_set)}
<p style="color:#888">PAPER ONLY — khong dung API key, khong dat lenh thuc. Trang tu tai lai ngay khi co du lieu moi (sau moi lan quet/cap nhat gia).</p>
</body></html>"""
MANUAL_PAGE_STYLE = """
<style>
body { font-family: Consolas, monospace; background:#111; color:#eee; padding:16px; }
h2 { color:#4CAF50; } table { border-color:#444; margin-bottom:20px; }
th, td { padding:6px 10px; } th { background:#222; }
a { color:#FFD700; }
form input, form select, form button { background:#222; color:#eee; border:1px solid #555; padding:4px 8px; }
</style>
"""


def _render_manual_positions_page():
    positions = load_manual_positions()
    with MANUAL_LOCK:
        report_snapshot = dict(MANUAL_REPORT)
    rows = []
    for pos in positions:
        rep = report_snapshot.get(pos["id"])
        if rep:
            price_txt = f"{rep['last_price']:.6g}"
            pnl_color = "#4CAF50" if rep["pnl_usdt"] >= 0 else "#F44336"
            pnl_txt = f"<span style='color:{pnl_color}; font-weight:bold;'>{rep['pnl_usdt']:+.2f} USDT ({rep['roi_pct']:+.1f}%)</span>"
            sr_txt = "<br>".join(
                f"{tf}: HT={d['support'] if d['support'] is not None else '--'} / "
                f"KC={d['resistance'] if d['resistance'] is not None else '--'}"
                for tf, d in rep["sr"].items()
            )
            alert_txt = " | ".join(rep["alerts"]) if rep["alerts"] else "Khong co canh bao"
        else:
            price_txt = pnl_txt = sr_txt = alert_txt = "Dang cho du lieu..."
        rows.append(f"""
        <tr>
            <td style="color:#FFD700; font-weight:bold;">{_html_escape(pos['symbol'])}</td>
            <td>{_html_escape(pos['direction'])}</td>
            <td>{pos['entry']:.6g}</td>
            <td>{pos['capital']:.2f}</td>
            <td>{pos['leverage']:.0f}x</td>
            <td>{price_txt}</td>
            <td>{pnl_txt}</td>
            <td>{sr_txt}</td>
            <td>{alert_txt}</td>
            <td><form method="post" action="/manual/close" style="margin:0">
                <input type="hidden" name="pos_id" value="{_html_escape(pos['id'])}">
                <button type="submit">Dong theo doi</button>
            </form></td>
        </tr>""")
    rows_html = "".join(rows) if rows else "<tr><td colspan='10'>Chua co vi the nhap tay nao.</td></tr>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Vi the nhap tay</title>
{MANUAL_PAGE_STYLE}
<script>setInterval(() => location.reload(), 15000);</script>
</head><body>
<a href="/">&larr; Ve trang chu</a>
<h2>Vi the nhap tay (theo doi lien tuc, gia + lai/lo cap nhat moi 15s)</h2>
<form method="post" action="/manual/add">
    Ma: <input type="text" name="symbol" placeholder="BTCUSDT" required>
    Chieu: <select name="direction"><option value="BUY">BUY</option><option value="SELL">SELL</option></select>
    Entry: <input type="number" step="any" name="entry" required>
    Von (USDT): <input type="number" step="any" name="capital" required>
    Don bay: <input type="number" step="any" name="leverage" value="1" required>
    <button type="submit">Them vi the theo doi</button>
</form>
<table border="1" cellpadding="4">
    <tr>
        <th>Ma</th><th>Chieu</th><th>Entry</th><th>Von</th><th>Don bay</th>
        <th>Gia hien tai</th><th>Lai/Lo</th><th>Ho tro / Khang cu (15'/30'/1h/2h/4h)</th>
        <th>Canh bao</th><th></th>
    </tr>
    {rows_html}
</table>
</body></html>
"""
if Flask is not None:
    flask_app = Flask(__name__)

    @flask_app.route("/")
    def _dashboard_route():
        return Response(render_dashboard_html(), mimetype="text/html")

    @flask_app.route("/api/status")
    def _status_route():
        with DASHBOARD_LOCK:
            updated_at = DASHBOARD_DATA.get("updated_at", "")
        return jsonify({"updated_at": updated_at})

    @flask_app.route("/api/events")
    def _events_route():
        with DASHBOARD_LOCK:
            st = DASHBOARD_DATA.get("state_st") or {}
            lt = DASHBOARD_DATA.get("state_lt") or {}
        events = []
        for tag, state in (("ST", st), ("LT", lt)):
            for e in state.get("ui_events", []):
                ev = dict(e); ev["variant"] = tag; events.append(ev)
        events.sort(key=lambda e: e.get("time", ""))
        return jsonify({"events": events[-100:]})

    @flask_app.route("/api/watch/add", methods=["POST"])
    def _watch_add_route():
        data = request.get_json(silent=True) or {}
        sym = str(data.get("symbol", "")).strip().upper()
        if not sym:
            return jsonify({"ok": False, "error": "Thieu symbol"}), 400
        symbols = load_watch_symbols()
        if sym not in symbols:
            symbols.append(sym)
            save_watch_symbols(symbols)
        return jsonify({"ok": True, "symbols": symbols})

    @flask_app.route("/api/watch/remove", methods=["POST"])
    def _watch_remove_route():
        data = request.get_json(silent=True) or {}
        sym = str(data.get("symbol", "")).strip().upper()
        symbols = [s for s in load_watch_symbols() if s != sym]
        save_watch_symbols(symbols)
        return jsonify({"ok": True, "symbols": symbols})

    @flask_app.route("/manual", methods=["GET"])
    def manual_positions_page():
        return _render_manual_positions_page()

    @flask_app.route("/manual/add", methods=["POST"])
    def manual_positions_add():
        symbol = request.form.get("symbol", "").strip()
        direction = request.form.get("direction", "BUY").strip()
        entry = request.form.get("entry", "0")
        capital = request.form.get("capital", "0")
        leverage = request.form.get("leverage", "1")
        if symbol and direction in ("BUY", "SELL"):
            try:
                add_manual_position(symbol, direction, entry, capital, leverage)
            except (ValueError, TypeError):
                pass
        return redirect("/manual")

    @flask_app.route("/manual/close", methods=["POST"])
    def manual_positions_close():
        pos_id = request.form.get("pos_id", "")
        if pos_id:
            remove_manual_position(pos_id)
            with MANUAL_LOCK:
                MANUAL_REPORT.pop(pos_id, None)
        return redirect("/manual")

    def run_flask_background(port=5000):
        flask_app.run(host="127.0.0.1", port=port, debug=False, use_reloader=False)
else:
    def run_flask_background(port=5000):
        logging.error("Flask chua duoc cai; khong chay duoc web dashboard. Chay: pip install flask")

def fast_price_loop(state_st, state_lt, priority_st, priority_lt, path_st, path_lt, duration_sec, poll_interval=15):
    end_ts = time.time() + duration_sec
    price_watch_state = load_price_watch_state()
    while True:
        user_watch = load_watch_symbols()
        manual_symbols = {p["symbol"] for p in load_manual_positions()}
        all_watched = set(ALWAYS_PRIORITY_SYMBOLS) | set(user_watch)
        watch = (set(state_st["positions"]) | set(state_lt["positions"]) |
                 set(priority_st) | set(priority_lt) | all_watched | manual_symbols)
        marks = {}
        if watch:
            try: marks = get_mark_prices()
            except Exception: logging.exception("Khong lay duoc gia real-time")
        for sym, p in list(state_st["positions"].items()):
            if sym in marks: check_price_milestones(state_st, sym, p, marks[sym])
        for sym, p in list(state_lt["positions"].items()):
            if sym in marks: check_price_milestones(state_lt, sym, p, marks[sym])
        for sym in all_watched:
            if sym in marks:
                check_reference_price_alert(price_watch_state, sym, marks[sym])
        try: update_manual_positions_fast(marks)
        except Exception: logging.exception("Loi cap nhat nhanh vi the nhap tay")
        save_price_watch_state(price_watch_state)
        save_v63_state(state_st, path_st); save_v63_state(state_lt, path_lt)
        update_dashboard_data(state_st, state_lt, priority_st, priority_lt, marks, end_ts, user_watch)
        if time.time() >= end_ts: break
        time.sleep(min(poll_interval, max(1, end_ts - time.time())))
        
def load_v63_state(path, starting):
    if path.exists():
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(d.get("positions"), dict) and isinstance(d.get("trades"), list): return d
        except Exception:
            logging.exception("State file loi; tao moi")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return {"equity": starting, "day_utc": today, "day_start_equity": starting, "daily_halted": False,
            "positions": {}, "trades": [], "events_sent": [], "cooldowns": {}, "signals_rejected": {}}

def save_v63_state(state, path):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)

def fetch_variant_bars(symbols, variant):
    tfs = TIMEFRAMES[variant]; result = {}
    def fetch(sym):
        return sym, get_closed_klines(sym, tfs["trend"], 300), get_closed_klines(sym, tfs["setup"], 300), get_closed_klines(sym, tfs["entry"], 300)
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(fetch, s) for s in symbols]
        for fu in as_completed(futs):
            try:
                sym, trend, setup, entry = fu.result(); result[sym] = (trend, setup, entry)
            except Exception:
                logging.exception("Loi fetch du lieu")
    return result

def run_cycle(state, symbols, variant, path):
    try: sync_binance_server_clock()
    except Exception:
        logging.exception("Khong dong bo duoc gio Binance; bo qua vong quet nay"); return {}
    try: spreads = get_book_spreads()
    except Exception: spreads = {}
    user_watch = set(load_watch_symbols())
    watch = set(symbols) | set(state["positions"]) | set(ALWAYS_PRIORITY_SYMBOLS) | user_watch
    fetched = fetch_variant_bars(watch, variant)
    manage_positions(state, {sym: d[2] for sym, d in fetched.items() if d[2]})
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("day_utc") != today:
        state["day_utc"] = today; state["day_start_equity"] = state["equity"]; state["daily_halted"] = False
    pinned_symbols = set(ALWAYS_PRIORITY_SYMBOLS) | user_watch
    priority_map = {}
    for sym, (trend, setup, entry) in fetched.items():
        if sym in state["positions"]:
            continue
        if not (trend and setup and entry):
            if sym in pinned_symbols:
                priority_map[sym] = {"missing": ["DATA"], "direction": None, "patterns": [], "last_price": 0.0}
            continue
        sig, reason = v63_signal(sym, trend, setup, entry, spreads.get(sym), variant)
        if sig:
            open_position(state, sig, spreads.get(sym))
        else:
            state["signals_rejected"][reason] = state["signals_rejected"].get(reason, 0) + 1
            report = v63_condition_report(sym, trend, setup, entry, spreads.get(sym), variant)
            if sym in pinned_symbols or classify_priority(report):
                priority_map[sym] = report
    save_state(state, path)
    logging.info("[%s] Xong vong quet: positions=%d trades=%d equity=%.2f uu_tien=%d",
                 variant, len(state["positions"]), len(state["trades"]), state["equity"], len(priority_map))
    return priority_map

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong quet roi thoat (de test)")
    parser.add_argument("--web-port", type=int, default=5000, help="Cong web dashboard (mac dinh 5000)")
    args = parser.parse_args()
    setup_logging()
    logging.info("HarsiBot ALL-IN-ONE | V6.3 Phase 2 + Web Dashboard | PAPER ONLY | khong API key | khong lenh thuc")
    if not args.once:
        threading.Thread(target=run_flask_background, args=(args.web_port,), daemon=True).start()
        logging.info("Web dashboard: http://127.0.0.1:%d/", args.web_port)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        tc = replace(CFG, telegram_token=TELEGRAM_TOKEN, telegram_chat_id=TELEGRAM_CHAT_ID)
        ok = send_telegram("HarsiBot V6.3 da khoi dong (PAPER ONLY). Day la tin nhan kiem tra ket noi Telegram.", tc)
        logging.info("Test Telegram luc khoi dong: %s", "THANH CONG" if ok else "THAT BAI")
    else:
        logging.info("Telegram chua cau hinh (thieu TELEGRAM_V63FIXED_BOT_TOKEN/CHAT_ID); bo qua test ket noi Telegram.")
    path_st = STATE_DIR / "V63_FIXED_ST_state.json"; path_lt = STATE_DIR / "V63_FIXED_LT_state.json"
    state_st = load_v63_state(path_st, STARTING_EQUITY); state_lt = load_v63_state(path_lt, STARTING_EQUITY)
    while True:
        try: symbols = get_futures_universe()
        except Exception:
            logging.exception("Loi lay universe; thu lai sau 30s"); time.sleep(30); continue
        priority_st = run_cycle(state_st, symbols, "ST", path_st)
        priority_lt = run_cycle(state_lt, symbols, "LT", path_lt)
        try: update_manual_positions_full()
        except Exception: logging.exception("Loi cap nhat vi the nhap tay")
        if args.once:
            logging.info("Co --once, dung lai sau 1 vong quet"); return
        fast_price_loop(state_st, state_lt, priority_st, priority_lt, path_st, path_lt, SCAN_INTERVAL_SEC)

if __name__ == "__main__":
    main()