import os
import sys
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db
import json
import time
from datetime import datetime

# KONFIGURASI
DATABASE_URL = "[https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/](https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/)"
URL_TARGET = "[https://orionterminal.com/screener](https://orionterminal.com/screener)"
TIMEOUT_LIMIT = 280

# 36 VARIABEL
COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d", "btc_corr_3d",
    "eth_corr_3d", "btc_beta_1d", "eth_beta_1d", "change_15m", "change_1h",
    "change_8h", "oi_change_15m", "oi_change_1d", "oi_change_1h", "oi_change_5m",
    "volatility_1h", "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m", "volume_1d", "volume_8h"
]

def init_firebase():
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: return False
    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except: return False

def run_debug_screenshot():
    print("BOT DEBUGGING (SCREENSHOT)...")
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        print("Membuka URL...")
        
        # Set timeout loading
        page.set.timeouts(page_load=60)
        try: page.get(URL_TARGET)
        except: pass

        print("Menunggu 30 detik...")
        time.sleep(30)
        
        # === AMBIL FOTO BUKTI ===
        print("Cekrek! Mengambil screenshot...")
        page.get_screenshot(path='bukti.png', full_page=True)
        print("Screenshot tersimpan sebagai 'bukti.png'")

        # Cek Data
        raw_text = page.run_js("return document.body.innerText")
        print("Panjang Teks di Layar: " + str(len(raw_text)) + " karakter")
        
        if "Verify you are human" in raw_text:
            print("TERDETEKSI BLOKIR CLOUDFLARE!")
        
        page.quit()

    except Exception as e:
        print("Error: " + str(e))

if __name__ == "__main__":
    run_debug_screenshot()
