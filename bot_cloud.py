
import requests
import time
import json
import sys
import os
import signal
from datetime import datetime, timezone
from typing import Dict, Any

# ================== CONFIG ==================
API_URL = "https://orionterminal.com/api/screener"
FETCH_INTERVAL = 60  # seconds
GLOBAL_MAX_RUNTIME = 55 * 60  # 55 menit (aman GH Actions)
REQUEST_TIMEOUT = 15  # hard timeout API
MAX_RETRY = 4

# Firebase
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL")  # wajib
FIREBASE_TOKEN = os.getenv("FIREBASE_TOKEN")    # wajib

# Cleanup
MAX_SNAPSHOTS = 5

# ================== UTILS ==================
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")
    sys.stdout.flush()

def utc_ts():
    return datetime.now(timezone.utc).isoformat()

def check_time(start):
    if time.time() - start > GLOBAL_MAX_RUNTIME:
        log("⏰ Global timer exceeded, exiting safely.")
        sys.exit(0)

# ================== FIREBASE ==================
class Firebase:
    def __init__(self, base_url, token):
        self.base = base_url.rstrip("/")
        self.token = token

    def _url(self, path):
        return f"{self.base}/{path}.json?auth={self.token}"

    def put(self, path, data):
        r = requests.put(self._url(path), json=data, timeout=10)
        r.raise_for_status()

    def get(self, path):
        r = requests.get(self._url(path), timeout=10)
        r.raise_for_status()
        return r.json()

    def delete(self, path):
        r = requests.delete(self._url(path), timeout=10)
        r.raise_for_status()

# ================== API ==================
def fetch_api() -> Dict[str, Any]:
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "x-requested-with": "XMLHttpRequest",
        "user-agent": "Mozilla/5.0"
    }

    r = requests.get(API_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    log(f"API status: {r.status_code}")
    r.raise_for_status()
    return r.json()

# ================== PARSER (PATCHED) ==================
def parse_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    PATCH:
    API response langsung berbentuk:
    { "BTC/USDT-binanceusdm": { "11": ..., ... }, ... }
    """
    if not isinstance(data, dict):
        raise ValueError("API response bukan object")

    coins = {}
    for symbol, metrics in data.items():
        if not isinstance(metrics, dict):
            continue

        coins[symbol] = {
            "symbol": symbol,
            "exchange": symbol.split("-")[-1],
            "pair": symbol.split("-")[0],
            "metrics": metrics,
            "fetched_at": utc_ts()
        }

    return coins

# ================== CLEANUP ==================
def cleanup_snapshots(firebase: Firebase):
    snapshots = firebase.get("snapshots") or {}
    keys = sorted(snapshots.keys())
    if len(keys) <= MAX_SNAPSHOTS:
        return

    for k in keys[:-MAX_SNAPSHOTS]:
        firebase.delete(f"snapshots/{k}")
        log(f"🧹 Deleted old snapshot {k}")

# ================== MAIN LOOP ==================
def run():
    if not FIREBASE_DB_URL or not FIREBASE_TOKEN:
        log("❌ Firebase env missing")
        sys.exit(1)

    fb = Firebase(FIREBASE_DB_URL, FIREBASE_TOKEN)
    start_time = time.time()

    log("🚀 ORION QUANT BOT START")
    log("Firebase initialized")

    while True:
        check_time(start_time)

        success = False
        raw_dump = None

        for attempt in range(1, MAX_RETRY + 1):
            check_time(start_time)
            try:
                log(f"Attempt {attempt} fetch API")
                data = fetch_api()

                log(f"API top keys: {list(data.keys())[:10]}")

                coins = parse_response(data)

                if not coins:
                    raise RuntimeError("No coins parsed")

                snapshot_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

                payload = {
                    "timestamp": utc_ts(),
                    "count": len(coins),
                    "data": coins
                }

                fb.put(f"snapshots/{snapshot_id}", payload)
                fb.put("latest", payload)

                cleanup_snapshots(fb)

                log(f"✅ Pushed {len(coins)} coins to Firebase")
                success = True
                break

            except Exception as e:
                raw_dump = str(data)[:500] if "data" in locals() else None
                log(f"⚠️ Error: {e}")
                time.sleep(2)

        if not success:
            log("❌ All attempts failed. Raw snippet:")
            log(raw_dump or "N/A")

        # ===== Responsive sleep =====
        slept = 0
        while slept < FETCH_INTERVAL:
            check_time(start_time)
            time.sleep(1)
            slept += 1

# ================== ENTRY ==================
if __name__ == "__main__":
    run()
