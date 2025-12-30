import os
import sys
import json
import time
import random
from datetime import datetime
import requests
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# CONFIG
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
ORION_API_URL = "https://orionterminal.com/api/screener"

FETCH_INTERVAL = 60                 # reload data tiap 1 menit
TIMEOUT = 20

SNAPSHOT_RETENTION_HOURS = 24       # simpan snapshot 24 jam
CLEANUP_INTERVAL = 900              # cleanup tiap 15 menit

# ==============================================================================
# FIREBASE INIT
# ==============================================================================
def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        raise RuntimeError("FIREBASE_KEY_JSON env not found")

    cred_dict = json.loads(key_json)
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(
            cred,
            {"databaseURL": DATABASE_URL}
        )

# ==============================================================================
# ORION API CLIENT (PURE XHR)
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
            "referer": "https://orionterminal.com/screener",
            "user-agent": random.choice([
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
            ])
        })

    def fetch(self):
        r = self.session.get(ORION_API_URL, timeout=TIMEOUT)
        if r.status_code == 200 and r.text:
            return r.json()
        raise RuntimeError(f"XHR failed {r.status_code}")

# ==============================================================================
# NORMALIZE DATA (ALL VARIABLES, NO FILTER)
# ==============================================================================
def normalize_data(raw):
    rows = raw.get("data") or raw.get("rows") or raw
    coins = {}

    if not isinstance(rows, list):
        return coins

    for row in rows:
        symbol = row.get("symbol") or row.get("market")
        if not symbol:
            continue

        key = (
            symbol.replace("/", "_")
                  .replace("-", "_")
                  .replace(".", "_")
                  .upper()
        )

        coins[key] = {
            "symbol": key,
            "updated_utc": datetime.utcnow().isoformat(),
            **row
        }

    return coins

# ==============================================================================
# FETCH DATA → PUSH FIREBASE
# ==============================================================================
def fetch_and_push(api, root_ref):
    raw = api.fetch()
    coins = normalize_data(raw)

    if not coins:
        raise RuntimeError("No data parsed")

    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    root_ref.child("coins").update(coins)
    root_ref.child("metadata").update({
        "last_update": ts,
        "total_coins": len(coins),
        "source": "orion_xhr_api"
    })
    root_ref.child("snapshots").child(ts).set(coins)

    print(f"✅ {len(coins)} coins updated @ {ts}")

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
        print(f"🧹 Cleanup: {deleted} snapshot lama dihapus")

# ==============================================================================
# MAIN LOOP
# ==============================================================================
def run():
    print("🚀 ORION XHR BOT — AUTO RELOAD + AUTO CLEAN")
    init_firebase()

    api = OrionAPI()
    root_ref = db.reference("screener_orion")

    last_cleanup = 0

    while True:
        try:
            fetch_and_push(api, root_ref)

            if time.time() - last_cleanup > CLEANUP_INTERVAL:
                cleanup_old_snapshots(root_ref)
                last_cleanup = time.time()

        except Exception as e:
            print(f"⚠️ Error: {e}")
            api.rotate_headers()

        print(f"⏳ Sleep {FETCH_INTERVAL}s\n")
        time.sleep(FETCH_INTERVAL)

# ==============================================================================
if __name__ == "__main__":
    run()
    sys.exit(0)
