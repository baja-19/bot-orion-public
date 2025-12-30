import os
import sys
import json
import time
import gc
import random
import re
import signal
import traceback
from datetime import datetime, timezone, timedelta

import requests

# Optional: DrissionPage (browser fallback). Ensure installed in runner if fallback needed.
try:
    from DrissionPage import ChromiumPage, ChromiumOptions
    _HAS_DRISSION = True
except Exception:
    _HAS_DRISSION = False

# Firebase admin
try:
    import firebase_admin
    from firebase_admin import credentials, db
    _HAS_FIREBASE_ADMIN = True
except Exception:
    _HAS_FIREBASE_ADMIN = False

# =========================
# CONFIG
# =========================
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
)
ORION_API_URL = os.environ.get("ORION_API_URL", "https://orionterminal.com/api/screener")
ORION_UI_URL = os.environ.get("ORION_UI_URL", "https://orionterminal.com/screener")

GLOBAL_TIMEOUT = int(os.environ.get("GLOBAL_TIMEOUT", "270"))  # seconds
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", "60"))   # seconds
TIMEOUT_REQUEST = int(os.environ.get("TIMEOUT_REQUEST", "20")) # seconds

BROWSER_PAGELOAD_TIMEOUT = int(os.environ.get("BROWSER_PAGELOAD_TIMEOUT", "30"))
BROWSER_JS_TIMEOUT = int(os.environ.get("BROWSER_JS_TIMEOUT", "8"))

SNAPSHOT_RETENTION_HOURS = int(os.environ.get("SNAPSHOT_RETENTION_HOURS", "24"))
CLEANUP_INTERVAL = int(os.environ.get("CLEANUP_INTERVAL", "900"))

# Column contract (36 keys) — used for index mapping if API returns numeric keys
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

SYMBOL_REGEX = re.compile(r'^[A-Z0-9_]{2,20}$')

SLEEP_CHUNK = 0.4  # adaptive sleep chunk

# =========================
# LOG + FLUSH
# =========================
def lg(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

# =========================
# GLOBAL TIMER HELPERS
# =========================
START_TIME = time.time()

def time_left() -> float:
    return max(0.0, GLOBAL_TIMEOUT - (time.time() - START_TIME))

def ensure_time(min_left: float = 0.5):
    if time_left() <= min_left:
        raise TimeoutError("Global timeout reached")

# =========================
# FIREBASE INIT (using FIREBASE_KEY_JSON env)
# =========================
def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON not found in environment (set as GitHub Secret)")

    if not _HAS_FIREBASE_ADMIN:
        raise RuntimeError("firebase_admin package not installed in runner")

    try:
        cred_dict = json.loads(key_json)
    except Exception as e:
        raise RuntimeError(f"FIREBASE_KEY_JSON parse error: {e}")

    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    lg("Firebase initialized")

# =========================
# Orion API client (requests)
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

    def set_cookies(self, cookies: dict):
        if cookies:
            # update Session cookies (reliable)
            self.session.cookies.update(cookies)
            # also set cookie header
            self.session.headers["cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())

    def fetch(self):
        ensure_time(1.0)
        try:
            r = self.session.get(ORION_API_URL, timeout=TIMEOUT_REQUEST)
        except Exception as e:
            raise RuntimeError(f"HTTP request failed: {e}")

        lg(f"API status: {r.status_code}")
        ctype = r.headers.get("content-type", "")
        # handle empty / not-modified
        if r.status_code in (204, 304):
            raise RuntimeError(f"API returned status {r.status_code}")

        # detect HTML (blocked)
        if "html" in ctype.lower() or r.text.strip().startswith("<"):
            snippet = r.text[:800].replace("\n", " ")
            lg(f"API returned HTML/snippet: {snippet!r}")
            raise RuntimeError("API returned HTML (likely blocked)")

        try:
            parsed = r.json()
            if isinstance(parsed, dict):
                lg(f"API top keys: {list(parsed.keys())[:12]}")
            return parsed
        except Exception as e:
            snippet = r.text[:800].replace("\n", " ")
            lg(f"JSON parse failed: {e} | snippet: {snippet!r}")
            raise RuntimeError("Failed to parse JSON from API")

# =========================
# JSON discovery helpers
# =========================
def find_rows_in_json(obj):
    # direct list-of-dict shape
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj
    if isinstance(obj, dict):
        # common keys first
        for k in ("data", "rows", "result", "payload", "items", "markets"):
            v = obj.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        # BFS shallow
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

# sanitize symbol
def _sanitize_symbol(s):
    try:
        s2 = str(s)
    except:
        return None
    s2 = s2.replace("/", "_").replace("-", "_").replace(".", "_").upper()
    s2 = re.sub(r'[^A-Z0-9_]', '_', s2)
    return s2

# =========================
# normalize_data (robust, handles indexed dict-of-symbols)
# =========================
def normalize_data(raw):
    coins = {}

    # 1) list of dicts?
    rows = find_rows_in_json(raw)
    if rows:
        lg("Parsing mode: rows(list-of-dicts)")
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol_raw = row.get("symbol") or row.get("market") or row.get("pair") or row.get("name") or row.get("s")
            if not symbol_raw:
                continue
            symbol = _sanitize_symbol(symbol_raw)
            if not symbol or not SYMBOL_REGEX.match(symbol):
                continue
            record = {"symbol": symbol, "updated_utc": datetime.utcnow().isoformat()}
            # include mandatory contract keys if available
            for k in COLUMNS_KEYS:
                record[k] = row.get(k, None)
            # include any other fields
            for k, v in row.items():
                if k not in record:
                    record[k] = v
            coins[symbol] = record
        return coins

    # 2) dict-of-symbols with numeric-indexed inner dicts?
    if isinstance(raw, dict):
        # quick heuristic: count how many child values are dicts and how many have numeric keys
        total = 0
        dict_children = 0
        numeric_inner = 0
        for k, v in raw.items():
            total += 1
            if isinstance(v, dict):
                dict_children += 1
                inner_nums = sum(1 for ik in v.keys() if re.fullmatch(r'\d+', str(ik)))
                if inner_nums > 0:
                    numeric_inner += 1
        if dict_children > 0 and numeric_inner >= max(1, dict_children // 4):
            lg("Parsing mode: dict-of-symbols with numeric indices detected")
            for sym_key, inner in raw.items():
                if not isinstance(inner, dict):
                    continue
                symbol = _sanitize_symbol(sym_key)
                if not symbol or not SYMBOL_REGEX.match(symbol):
                    continue
                record = {"symbol": symbol, "updated_utc": datetime.utcnow().isoformat()}
                for inner_k, val in inner.items():
                    sk = str(inner_k)
                    if re.fullmatch(r'\d+', sk):
                        idx = int(sk)
                        # If Orion indexes start at 1 instead of 0, we cannot be certain.
                        # Heuristic: if idx==0 used, keep as 0; if majority >=1, we leave as-is.
                        if 0 <= idx < len(COLUMNS_KEYS):
                            mapped = COLUMNS_KEYS[idx]
                        else:
                            mapped = f"col_{idx}"
                        record[mapped] = val
                    else:
                        # non-numeric keys stored as-is
                        record[sk] = val
                # ensure required columns exist
                for ck in COLUMNS_KEYS:
                    if ck not in record:
                        record[ck] = None
                coins[symbol] = record
            return coins

    # fallback: unknown format
    try:
        if isinstance(raw, dict):
            lg("⚠️ normalize_data: unknown JSON shape. Top-level keys sample:")
            lg(str(list(raw.keys())[:40]))
    except Exception:
        pass
    return coins

# =========================
# Browser fallback (only if DrissionPage available)
# =========================
def browser_fallback(start_time):
    if not _HAS_DRISSION:
        raise RuntimeError("DrissionPage not installed; cannot browser-fallback")

    ensure_time(8.0)
    lg("Starting browser fallback (DrissionPage) to harvest cookies & force render")

    co = ChromiumOptions()
    co.set_argument("--headless=new")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--window-size=5000,3000")
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

    page = ChromiumPage(addr_or_opts=co)
    # set page load timeout
    try:
        page.set.timeouts(page_load=BROWSER_PAGELOAD_TIMEOUT)
    except Exception:
        pass

    try:
        # page.get with hard timeout via alarm if on unix main thread
        try:
            ensure_time(5.0)
            page.get(ORION_UI_URL)
        except Exception as e:
            lg(f"⚠️ page.get warning: {e}")

        # small adaptive wait
        t0 = time.time()
        while time.time() - t0 < 4:
            if time_left() <= 2:
                break
            time.sleep(0.4)

        # Try click Columns panel via JS (best-effort)
        js_click_columns = """
        (function(){
            try{
                let btn = document.querySelector('button[aria-label="Columns"], button[title*="Columns"], .columns-btn, .btn-columns');
                if(btn) btn.click();
                const panels = document.querySelectorAll('div, section');
                panels.forEach(p=>{
                    const inputs = p.querySelectorAll('input[type=checkbox]');
                    inputs.forEach(i=>{ if(!i.checked) i.click(); });
                });
                return true;
            }catch(e){ return false; }
        })();
        """
        try:
            page.run_js(js_click_columns, timeout=BROWSER_JS_TIMEOUT)
        except Exception:
            pass

        # Snake scrolling
        for _ in range(2):
            if time_left() <= 3:
                break
            try:
                page.run_js("window.scrollTo(0, document.body.scrollHeight);", timeout=BROWSER_JS_TIMEOUT)
            except:
                pass
            time.sleep(0.6)
            try:
                page.run_js("window.scrollTo(document.body.scrollWidth, 0);", timeout=BROWSER_JS_TIMEOUT)
            except:
                pass
            time.sleep(0.6)
            try:
                page.run_js("window.scrollTo(0, 0);", timeout=BROWSER_JS_TIMEOUT)
            except:
                pass
            time.sleep(0.6)

        # collect cookies
        cookies = {}
        try:
            for c in page.cookies:
                cookies[c['name']] = c['value']
        except Exception:
            try:
                # fallback: page.cookies() call
                for c in page.cookies():
                    cookies[c.get('name')] = c.get('value')
            except:
                pass

        lg(f"Cookies harvested: {list(cookies.keys())[:6]}")
        return cookies
    finally:
        try:
            page.quit()
        except:
            pass

# =========================
# fetch_and_push (retry/backoff, fallback)
# =========================
def fetch_and_push(api, root_ref, start_time):
    max_attempts = 4
    attempt = 0
    backoff_base = 1.6
    raw = None
    while attempt < max_attempts:
        attempt += 1
        ensure_time(1.0)
        try:
            lg(f"Attempt {attempt} fetch API")
            raw = api.fetch()
            coins = normalize_data(raw)
            if coins:
                ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
                # push to firebase
                root_ref.child("coins").update(coins)
                root_ref.child("metadata").update({
                    "last_update": ts,
                    "total_coins": len(coins),
                    "source": "orion_xhr_api"
                })
                root_ref.child("snapshots").child(ts).set(coins)
                lg(f"✅ {len(coins)} coins pushed @ {ts}")
                return True
            else:
                lg(f"⚠️ No coins parsed on attempt {attempt}")
                if attempt == 1:
                    if time_left() > 8 and _HAS_DRISSION:
                        try:
                            cookies = browser_fallback(start_time)
                            api.rotate_headers()
                            api.set_cookies(cookies)
                            lg("🔁 retry after cookie harvest")
                        except Exception as e_fb:
                            lg(f"⚠️ Browser fallback failed: {e_fb}")
                # backoff sleep (adaptive)
                sleep_for = min(8, (backoff_base ** attempt))
                lg(f"⏳ backoff sleeping {sleep_for}s")
                t_end = time.time() + sleep_for
                while time.time() < t_end:
                    ensure_time(0.8)
                    time.sleep(SLEEP_CHUNK)
                continue
        except Exception as e:
            lg(f"⚠️ Fetch attempt {attempt} raised: {e}")
            if attempt == 1 and time_left() > 8 and _HAS_DRISSION:
                try:
                    cookies = browser_fallback(start_time)
                    api.rotate_headers()
                    api.set_cookies(cookies)
                except Exception as e_fb:
                    lg(f"Fallback failed: {e_fb}")
            sleep_for = min(8, (backoff_base ** attempt))
            lg(f"⏳ after-exception sleeping {sleep_for}s")
            t_end = time.time() + sleep_for
            while time.time() < t_end:
                ensure_time(0.8)
                time.sleep(SLEEP_CHUNK)
            continue

    # after attempts exhausted
    try:
        lg("❌ All attempts failed. Raw snippet (first 1500 chars):")
        if raw is not None:
            try:
                lg(json.dumps(raw)[:1500])
            except Exception:
                lg(str(raw)[:1500])
    except Exception:
        pass
    return False

# =========================
# cleanup old snapshots (timestamp keys)
# =========================
def cleanup_old_snapshots(root_ref):
    try:
        snap_ref = root_ref.child("snapshots")
        snaps = snap_ref.get()
        if not snaps:
            return 0
        now = datetime.utcnow()
        deleted = 0
        for key in list(snaps.keys()):
            try:
                ts = datetime.strptime(key, "%Y-%m-%d_%H-%M-%S")
                age_hours = (now - ts).total_seconds() / 3600.0
                if age_hours > SNAPSHOT_RETENTION_HOURS:
                    snap_ref.child(key).delete()
                    deleted += 1
            except Exception:
                continue
        if deleted:
            lg(f"🧹 Cleanup: {deleted} snapshots removed")
        return deleted
    except Exception as e:
        lg(f"⚠️ cleanup_old_snapshots error: {e}")
        return 0

# =========================
# MAIN
# =========================
def run():
    lg("🚀 ORION QUANT BOT START")
    try:
        init_firebase()
    except Exception as e:
        lg(f"❌ Firebase init failed: {e}")
        lg(traceback.format_exc())
        return

    if not _HAS_DRISSION:
        lg("⚠️ DrissionPage not installed — browser fallback disabled (may still work if API is stable)")

    api = OrionAPI()
    root_ref = db.reference('screener_orion')

    start_time = time.time()
    last_cleanup = 0

    while True:
        if time.time() - start_time > GLOBAL_TIMEOUT:
            lg("⏹️ Timeout global tercapai, exit")
            break

        try:
            ok = fetch_and_push(api, root_ref, start_time)
            if ok:
                # cleanup occasionally
                if time.time() - last_cleanup > CLEANUP_INTERVAL:
                    cleanup_old_snapshots(root_ref)
                    last_cleanup = time.time()
        except TimeoutError:
            lg("⏰ Global timeout encountered — exiting")
            break
        except Exception as e:
            lg(f"❌ Error cycle: {e}")
            lg(traceback.format_exc())

        # adaptive sleep broken into chunks so we can exit quickly if time's up
        t_end = time.time() + FETCH_INTERVAL
        while time.time() < t_end:
            if time.time() - start_time > GLOBAL_TIMEOUT:
                break
            time.sleep(SLEEP_CHUNK)

    lg("🏁 ORION QUANT BOT FINISHED (graceful)")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        lg(f"FATAL: {e}")
        lg(traceback.format_exc())
    finally:
        try:
            sys.stdout.flush()
        except:
            pass
        sys.exit(0)
