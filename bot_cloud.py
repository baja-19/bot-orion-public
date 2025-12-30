import os
import sys
import json
import time
import random
import re
from datetime import datetime
import requests
import firebase_admin
from firebase_admin import credentials, db
from DrissionPage import ChromiumPage, ChromiumOptions

# ==============================================================================
# KONFIGURASI UTAMA
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
ORION_API_URL = "https://orionterminal.com/api/screener"
ORION_UI_URL = "https://orionterminal.com/screener"

# Batas aman GitHub Actions
GLOBAL_TIMEOUT = 270            # detik
FETCH_INTERVAL = 60             # reload 1 menit
TIMEOUT_REQUEST = 20

# Snapshot management
SNAPSHOT_RETENTION_HOURS = 24
CLEANUP_INTERVAL = 900          # 15 menit

# Regex simbol valid (Latin + angka)
SYMBOL_REGEX = re.compile(r"^[A-Z0-9_]+$")

# ==============================================================================
# FIREBASE INIT
# ==============================================================================
def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON tidak ditemukan")

    cred_dict = json.loads(key_json)
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(
            cred, {"databaseURL": DATABASE_URL}
        )

# ==============================================================================
# ORION API CLIENT (MODE UTAMA)
# ==============================================================================
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

    def set_auth(self, cookies):
        if cookies:
            self.session.headers["cookie"] = "; ".join(
                f"{k}={v}" for k, v in cookies.items()
            )

    def fetch(self):
        r = self.session.get(ORION_API_URL, timeout=TIMEOUT_REQUEST)
        if r.status_code == 200 and r.text:
            return r.json()
        raise RuntimeError(f"API ERROR {r.status_code}")

# ==============================================================================
# BROWSER FALLBACK (COOKIE HARVEST + UI FORCE RENDER)
# ==============================================================================
def browser_fallback():
    print("🧠 Browser fallback aktif (harvest cookie + force render)")

    co = ChromiumOptions()
    co.set_argument("--headless=new")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--window-size=5000,3000")
    co.set_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    )

    page = ChromiumPage(addr_or_opts=co)
    page.get(ORION_UI_URL)
    page.wait(15)

    # Paksa render semua kolom via zoom
    try:
        page.run_js("document.body.style.zoom='25%'")
    except:
        pass

    # Snake scrolling untuk lazy-load
    for _ in range(3):
        page.run_js("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        page.run_js("window.scrollTo(document.body.scrollWidth, 0)")
        time.sleep(1)
        page.run_js("window.scrollTo(0, 0)")
        time.sleep(1)

    cookies = {c['name']: c['value'] for c in page.cookies}
    page.quit()

    if not cookies:
        raise RuntimeError("Cookie gagal dipanen")

    print("✅ Cookie berhasil dipanen")
    return cookies

# ==============================================================================
# NORMALISASI DATA (36 VARIABEL + FILTER SIMBOL)
# ==============================================================================
def normalize_data(raw):
    rows = raw.get("data") or raw.get("rows") or raw
    coins = {}

    if not isinstance(rows, list):
        return coins

    for row in rows:
        symbol_raw = row.get("symbol") or row.get("market")
        if not symbol_raw:
            continue

        symbol = (
            symbol_raw.replace("/", "_")
                      .replace("-", "_")
                      .replace(".", "_")
                      .upper()
        )

        # Filter simbol non-latin / sampah
        if not SYMBOL_REGEX.match(symbol):
            continue

        coins[symbol] = {
            "symbol": symbol,
            "updated_utc": datetime.utcnow().isoformat(),
            **row
        }

    return coins

# ==============================================================================
# FETCH → PUSH FIREBASE
# ==============================================================================
def fetch_and_push(api, root_ref):
    try:
        raw = api.fetch()
    except Exception as e:
        print(f"⚠️ API gagal: {e}")
        cookies = browser_fallback()
        api.rotate_headers()
        api.set_auth(cookies)
        raw = api.fetch()

    coins = normalize_data(raw)
    if not coins:
        raise RuntimeError("Data koin kosong")

    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    root_ref.child("coins").update(coins)
    root_ref.child("metadata").update({
        "last_update": ts,
        "total_coins": len(coins),
        "source": "orion_xhr_api"
    })
    root_ref.child("snapshots").child(ts).set(coins)

    print(f"✅ {len(coins)} koin berhasil dikirim @ {ts}")

# ==============================================================================
# AUTO CLEAN SNAPSHOT LAMA
# ==============================================================================
def cleanup_old_snapshots(root_ref):
    snap_ref = root_ref.child("snapshots")
    snaps = snap_ref.get()
    if not snaps:
        return

    now = datetime.utcnow()
    deleted = 0

    for key in snaps.keys():
        try:
            ts = datetime.strptime(key, "%Y-%m-%d_%H-%M-%S")
            age_hours = (now - ts).total_seconds() / 3600
            if age_hours > SNAPSHOT_RETENTION_HOURS:
                snap_ref.child(key).delete()
                deleted += 1
        except:
            continue

    if deleted:
        print(f"🧹 Cleanup: {deleted} snapshot dihapus")

# ==============================================================================
# MAIN LOOP (AMAN GITHUB ACTIONS)
# ==============================================================================
def run():
    print("🚀 ORION QUANT BOT — FINAL PRODUCTION")
    init_firebase()

    api = OrionAPI()
    root_ref = db.reference("screener_orion")

    start_time = time.time()
    last_cleanup = 0

    while True:
        if time.time() - start_time > GLOBAL_TIMEOUT:
            print("⏹️ Timeout global tercapai, exit aman")
            break

        try:
            fetch_and_push(api, root_ref)

            if time.time() - last_cleanup > CLEANUP_INTERVAL:
                cleanup_old_snapshots(root_ref)
                last_cleanup = time.time()

        except Exception as e:
            print(f"❌ Error cycle: {e}")
            time.sleep(5)

        time.sleep(FETCH_INTERVAL)

# ==============================================================================
if __name__ == "__main__":
    run()
    sys.exit(0)
