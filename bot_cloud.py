import os
import sys
import json
import time
import gc
import re
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# KONFIGURASI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280  # detik

BLACKLIST = {
    "ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS",
    "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER",
    "COLUMNS", "EXPORT", "SHARE", "FEEDBACK", "HELP", "MARKET", "OPENINTEREST"
}

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

SYMBOL_RE = re.compile(r'^[A-Z0-9]{2,12}(?:[\/\.\-_][A-Z0-9]{2,12})?$')

# ==============================================================================
# FIREBASE INIT
# ==============================================================================
def init_firebase():
    key_json = os.environ.get("FIREBASE_KEY_JSON")
    if not key_json:
        print("❌ FIREBASE_KEY_JSON tidak ditemukan")
        return False
    try:
        cred_dict = json.loads(key_json)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(
                cred, {"databaseURL": DATABASE_URL}
            )
        return True
    except Exception as e:
        print(f"❌ Firebase error: {e}")
        return False

# ==============================================================================
# DOM EXTRACTION
# ==============================================================================
def extract_rows(page):
    js = """
    (function(){
        const selectors = [
            'div[data-row]',
            'div[role="row"]',
            'tr',
            '.rt-tr',
            '.ag-row',
            '.ant-table-row'
        ];
        for (const s of selectors){
            const els = document.querySelectorAll(s);
            if (els.length > 0){
                let rows = [];
                els.forEach(e => {
                    if (e.innerText) rows.push(e.innerText.trim());
                });
                if (rows.length > 0) return rows;
            }
        }
        return document.body.innerText;
    })();
    """
    result = page.run_js(js)
    if isinstance(result, list):
        return result
    elif isinstance(result, str):
        return [l.strip() for l in result.split("\n") if l.strip()]
    return []

def split_columns(text):
    parts = re.split(r"\t+|\s{2,}", text)
    return [p.strip() for p in parts if p.strip()]

# ==============================================================================
# MAIN
# ==============================================================================
def run_extreme_extraction():
    print("🚀 BOT ORION – FINAL VERSION STARTED")
    start_time = time.time()

    if not init_firebase():
        return

    # Firebase structure
    root_ref = db.reference("screener_orion")
    coins_ref = root_ref.child("coins")
    meta_ref = root_ref.child("metadata")
    snapshot_ref = root_ref.child("raw_snapshot")

    # Browser setup
    co = ChromiumOptions()
    co.set_argument("--headless=new")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    co.set_argument("--window-size=2560,1440")
    co.set_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    page = ChromiumPage(addr_or_opts=co)
    page.set.timeouts(page_load=60)

    print(f"🌐 Open: {URL_TARGET}")
    page.get(URL_TARGET)
    time.sleep(20)

    try:
        page.run_js("document.body.style.zoom='20%'")
    except:
        pass

    while time.time() - start_time < TIMEOUT_LIMIT:
        try:
            page.run_js("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)

            rows = extract_rows(page)
            print(f"📄 Rows detected: {len(rows)}")

            data_batch = {}
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for row in rows:
                cols = split_columns(row)
                if len(cols) < 5:
                    continue

                symbol = cols[0].upper()
                symbol_clean = (
                    symbol.replace("/", "_")
                          .replace(".", "_")
                          .replace("-", "_")
                )

                if (
                    not SYMBOL_RE.match(symbol)
                    or symbol_clean in BLACKLIST
                ):
                    continue

                coin_data = {
                    "symbol": symbol_clean,
                    "updated": ts
                }

                for i, key in enumerate(COLUMNS_KEYS):
                    if i + 1 < len(cols):
                        coin_data[key] = cols[i + 1]
                    else:
                        coin_data[key] = "-"

                data_batch[symbol_clean] = coin_data

            if data_batch:
                coins_ref.update(data_batch)

                meta_ref.update({
                    "source": "orionterminal",
                    "last_update": ts,
                    "total_coins": len(data_batch)
                })

                snapshot_ref.child(
                    ts.replace(" ", "_").replace(":", "-")
                ).set(data_batch)

                print(f"✅ [{ts}] Uploaded {len(data_batch)} coins")
                break
            else:
                print("⚠️ No data detected, refreshing...")
                page.refresh()
                time.sleep(10)

            gc.collect()

        except Exception as e:
            print(f"⚠️ Loop error: {e}")
            time.sleep(5)

    page.quit()
    print("🏁 BOT ORION FINISHED")

# ==============================================================================
if __name__ == "__main__":
    run_extreme_extraction()
    sys.exit(0)
