import os
import sys
import json
import time
import gc
import random
import signal
import requests
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# Browser fallback
from DrissionPage import ChromiumPage, ChromiumOptions

# Firebase
import firebase_admin
from firebase_admin import credentials, db

# =========================
# KONFIGURASI GLOBAL
# =========================
ORION_UI_URL = "https://orionterminal.com/screener"
ORION_API_URL = "https://orionterminal.com/api/screener"

# Batas waktu global (detik) — aman GA
GLOBAL_TIMEOUT = 270

# Interval reload data (detik)
RELOAD_INTERVAL = 60

# Timeout request API
TIMEOUT_REQUEST = 20

# Timeout keras untuk browser (detik)
BROWSER_PAGELOAD_TIMEOUT = 35
BROWSER_JS_TIMEOUT = 10

# Firebase
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
ROOT_PATH = "orion_screener"

# Retensi snapshot (jam) — auto-clean snapshot lama
SNAPSHOT_RETENTION_HOURS = 6

# Regex simbol (A-Z0-9_)
import re
SYMBOL_REGEX = re.compile(r"^[A-Z0-9_]{2,15}$")

# Kolom wajib (kontrak data)
COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d",
    "btc_corr_3d", "eth_corr_3d", "btc_beta_1d", "eth_beta_1d",
    "change_15m", "change_1h", "change_8h", "oi_change_15m",
    "oi_change_1d", "oi_change_1h", "oi_change_5m", "volatility_1h",
    "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m",
    "volume_1d", "volume_8h"
]

# =========================
# UTIL LOGGING
# =========================
def lg(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

# =========================
# TIMER KERAS
# =========================
START_TIME = time.time()

def time_left() -> float:
    return max(0.0, GLOBAL_TIMEOUT - (time.time() - START_TIME))

def ensure_time(min_left: float = 0.5):
    if time_left() <= min_left:
        raise TimeoutError("Global timeout reached")

# =========================
# FIREBASE INIT
# =========================
def init_firebase() -> None:
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON not set")
    cred_dict = json.loads(key_json)
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {"databaseURL": DATABASE_URL})
    lg("Firebase initialized")

# =========================
# API CLIENT
# =========================
class OrionAPI:
    def __init__(self):
        self.session = requests.Session()
        self.rotate_headers()

    def rotate_headers(self):
        self.session.headers.clear()
        self.session.headers.update({
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest",
            "referer": ORION_UI_URL,
            "user-agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ])
        })

    def set_cookies(self, cookies: Dict[str, str]):
        if cookies:
            self.session.cookies.update(cookies)
            self.session.headers["cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())

    def fetch(self) -> Any:
        ensure_time(1.0)
        try:
            r = self.session.get(ORION_API_URL, timeout=TIMEOUT_REQUEST)
        except Exception as e:
            raise RuntimeError(f"HTTP request failed: {e}")

        lg(f"API status: {r.status_code}")
        ctype = r.headers.get("content-type", "")
        if r.status_code in (204, 304):
            raise RuntimeError(f"Empty status {r.status_code}")
        if "html" in ctype.lower() or r.text.strip().startswith("<"):
            snippet = r.text[:800].replace("\n", " ")
            lg(f"API returned HTML/snippet: {snippet!r}")
            raise RuntimeError("Blocked/HTML response")

        try:
            js = r.json()
            if isinstance(js, dict):
                lg(f"API top keys: {list(js.keys())[:10]}")
            return js
        except Exception as e:
            snippet = r.text[:800].replace("\n", " ")
            lg(f"JSON parse failed: {e} | snippet: {snippet!r}")
            raise RuntimeError("JSON parse failed")

# =========================
# JSON PARSER (ROBUST)
# =========================
def find_rows_in_json(obj: Any) -> Optional[list]:
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj
    if isinstance(obj, dict):
        for k in ["data", "rows", "result", "payload", "markets", "items"]:
            v = obj.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        # BFS terbatas
        q = list(obj.values())
        for _ in range(3):
            nq = []
            for v in q:
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v
                if isinstance(v, dict):
                    nq.extend(v.values())
            q = nq
    return None

def normalize_data(raw: Any) -> Dict[str, Dict[str, Any]]:
    coins: Dict[str, Dict[str, Any]] = {}
    rows = find_rows_in_json(raw)
    if not rows:
        lg("⚠️ Tidak menemukan list rows pada JSON")
        try:
            lg(f"Top-level keys: {list(raw.keys())[:15]}")
        except Exception:
            pass
        return coins

    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol_raw = row.get("symbol") or row.get("market") or row.get("pair") or row.get("name") or row.get("s")
        if not symbol_raw:
            continue
        symbol = str(symbol_raw).replace("/", "_").replace("-", "_").replace(".", "_").upper()
        if not SYMBOL_REGEX.match(symbol):
            continue

        record = {"symbol": symbol, "updated_utc": datetime.utcnow().isoformat()}
        # isi semua kolom wajib jika ada
        for k in COLUMNS_KEYS:
            record[k] = row.get(k, None)
        # sertakan field lain (non-kontrak) jika ada
        for k, v in row.items():
            if k not in record:
                record[k] = v

        coins[symbol] = record
    return coins

# =========================
# BROWSER FALLBACK (COOKIE HARVEST)
# =========================
def browser_fallback() -> Dict[str, str]:
    ensure_time(8.0)
    lg("🔁 Browser fallback: panen cookie")
    co = ChromiumOptions()
    co.set_argument("--headless=new")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--window-size=5000,3000")
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/143 Safari/537.36")

    page = ChromiumPage(addr_or_opts=co)
    page.set.timeouts(page_load=BROWSER_PAGELOAD_TIMEOUT)

    try:
        page.get(ORION_UI_URL)
        # tunggu pendek, watchdog sleep
        for _ in range(5):
            ensure_time(5.0)
            time.sleep(0.6)

        # snake scrolling untuk memicu render
        js_steps = [
            "window.scrollTo(0, document.body.scrollHeight);",
            "window.scrollTo(document.body.scrollWidth, 0);",
            "window.scrollTo(0, 0);",
        ]
        for js in js_steps:
            ensure_time(3.0)
            page.run_js(js, timeout=BROWSER_JS_TIMEOUT)
            time.sleep(0.4)

        # ambil cookie
        cookies = {}
        for c in page.cookies():
            cookies[c.get("name")] = c.get("value")
        lg(f"🍪 Cookies harvested: {list(cookies.keys())[:6]}")
        return cookies
    finally:
        try:
            page.quit()
        except Exception:
            pass

# =========================
# FETCH + PUSH (RETRY)
# =========================
def fetch_and_push(api: OrionAPI, root_ref, start_ts: float) -> bool:
    max_attempts = 4
    backoff = 1.6
    raw = None

    for attempt in range(1, max_attempts + 1):
        ensure_time(2.0)
        try:
            lg(f"Attempt {attempt} fetch API")
            raw = api.fetch()
            coins = normalize_data(raw)
            if coins:
                ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
                root_ref.child("coins").update(coins)
                root_ref.child("metadata").update({
                    "last_update": ts,
                    "total_coins": len(coins),
                    "source": "orion_xhr_api"
                })
                root_ref.child("snapshots").child(ts).set(coins)
                lg(f"✅ Pushed {len(coins)} coins @ {ts}")
                return True
            else:
                lg("⚠️ No coins parsed")
                if attempt == 1:
                    # coba fallback
                    if time_left() > 8:
                        cookies = browser_fallback()
                        api.rotate_headers()
                        api.set_cookies(cookies)
        except Exception as e:
            lg(f"⚠️ Attempt {attempt} error: {e}")
            if attempt == 1 and time_left() > 8:
                try:
                    cookies = browser_fallback()
                    api.rotate_headers()
                    api.set_cookies(cookies)
                except Exception as efb:
                    lg(f"Fallback failed: {efb}")

        sleep_s = min(8.0, backoff ** attempt)
        # watchdog sleep (pecah)
        end = time.time() + sleep_s
        while time.time() < end:
            ensure_time(1.0)
            time.sleep(0.4)

    # debug snippet
    try:
        if raw is not None:
            lg("❌ All attempts failed. Raw snippet:")
            lg(json.dumps(raw)[:1200])
    except Exception:
        pass
    return False

# =========================
# AUTO-CLEAN SNAPSHOT LAMA
# =========================
def cleanup_old_snapshots(root_ref):
    ensure_time(1.0)
    snaps_ref = root_ref.child("snapshots")
    snaps = snaps_ref.get()
    if not snaps:
        return
    cutoff = datetime.utcnow() - timedelta(hours=SNAPSHOT_RETENTION_HOURS)
    removed = 0
    for k in list(snaps.keys()):
        try:
            ts = datetime.strptime(k, "%Y-%m-%d_%H-%M-%S")
            if ts < cutoff:
                snaps_ref.child(k).delete()
                removed += 1
        except Exception:
            continue
    if removed:
        lg(f"🧹 Cleaned {removed} old snapshots")

# =========================
# MAIN LOOP
# =========================
def run():
    lg("🚀 ORION QUANT BOT START")
    init_firebase()

    api = OrionAPI()
    root_ref = db.reference(ROOT_PATH)

    last_run = 0.0
    while True:
        ensure_time(2.0)
        now = time.time()
        if now - last_run >= RELOAD_INTERVAL:
            try:
                ok = fetch_and_push(api, root_ref, START_TIME)
                if ok:
                    cleanup_old_snapshots(root_ref)
            except TimeoutError:
                lg("⏰ Global timeout reached. Exit.")
                break
            except Exception:
                lg("❌ Unexpected error:\n" + traceback.format_exc())
            last_run = now

        # watchdog idle sleep (pecah)
        for _ in range(3):
            ensure_time(1.0)
            time.sleep(0.4)

        if time_left() <= 1.0:
            lg("⏰ Time almost up. Exit.")
            break

    lg("🏁 BOT STOP")
    gc.collect()

if __name__ == "__main__":
    try:
        run()
    except TimeoutError:
        lg("⏰ Hard stop by global timer")
    except Exception:
        lg("❌ Fatal error:\n" + traceback.format_exc())
    finally:
        sys.exit(0)
