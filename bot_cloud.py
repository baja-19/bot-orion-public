import os
import sys
import json
import time
import requests
import gc
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# KONFIGURASI TINGKAT DEWA
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280

# Ambang Batas Alert (Kirim notif jika...)
ALERT_PUMP_PERCENT = 5.0   # Harga Naik > 5% (1h)
ALERT_VOLUME_MIN = 500_000 # Volume > $500k (Biar gak kena koin micin)

# Blacklist Menu (Agar tidak salah ambil)
BLACKLIST = [
    "ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS",
    "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER", 
    "COLUMNS", "EXPORT", "SHARE", "FEEDBACK", "HELP"
]

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

# Cache untuk mencegah spam notifikasi yang sama
alert_history = [] 

def init_services():
    # 1. Init Firebase
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: return False
    
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(json_str))
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
    except: return False
    return True

def send_telegram(msg):
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id: return
    
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    except: pass

def clean_number(val):
    """Mengubah string $1.5M menjadi float 1500000"""
    if not isinstance(val, str): return 0
    val = val.replace('$', '').replace(',', '').replace('%', '').strip()
    multiplier = 1
    if 'M' in val: 
        multiplier = 1_000_000
        val = val.replace('M', '')
    elif 'K' in val: 
        multiplier = 1_000
        val = val.replace('K', '')
    try: return float(val) * multiplier
    except: return 0

def run_ultimate_bot():
    print("🚀 BOT ORION ULTIMATE (Smart Analysis)...")
    start_global = time.time()
    
    if not init_services(): return

    # Setup Browser
    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        try: page.get(URL_TARGET)
        except: pass

        time.sleep(20)
        
        # Referensi DB
        ref_data = db.reference('screener_full_data')
        ref_status = db.reference('bot_status') # Fitur Heartbeat

        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT: break

            try:
                # Manuver Scroll
                page.run_js("window.scrollTo(0, 10000);"); time.sleep(0.5)
                page.run_js("window.scrollTo(0, 0);"); time.sleep(0.5)
                
                raw_text = page.run_js("return document.body.innerText")
                lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                i = 0
                while i < len(lines):
                    line = lines[i]
                    # Filter Koin Valid
                    if (2 <= len(line) <= 6 and line.isalpha() and line.isupper() and line not in BLACKLIST):
                        if i + 1 < len(lines):
                            next_line = lines[i+1].replace('$', '').strip()
                            if any(c.isdigit() for c in next_line):
                                # --- PARSING DATA ---
                                symbol = line
                                coin_data = {'updated': ts}
                                data_idx = i + 1
                                
                                # Ambil data mentah untuk analisis
                                raw_chg_1h = "0"
                                raw_vol_1h = "0"

                                for key in COLUMNS_KEYS:
                                    if data_idx < len(lines):
                                        val = lines[data_idx]
                                        coin_data[key] = val
                                        
                                        # Tangkap variabel penting untuk Alert
                                        if key == "change_1h": raw_chg_1h = val
                                        if key == "volume_1h": raw_vol_1h = val
                                        
                                        data_idx += 1
                                    else:
                                        coin_data[key] = "-"
                                
                                # --- ANALISIS CERDAS (DI DALAM SERVER) ---
                                chg_val = clean_number(raw_chg_1h)
                                vol_val = clean_number(raw_vol_1h)
                                
                                # Hitung Momentum Score (Volume * %Change)
                                momentum_score = vol_val * abs(chg_val)
                                coin_data['momentum_score'] = momentum_score # Kirim skor ke Firebase

                                # --- CEK ALERT TELEGRAM ---
                                # Jika naik > 5% dan Volume Besar
                                if chg_val > ALERT_PUMP_PERCENT and vol_val > ALERT_VOLUME_MIN:
                                    if symbol not in alert_history:
                                        msg = f"🚀 *PUMP ALERT: {symbol}*\nChange 1h: +{chg_val}%\nVol: ${vol_val:,.0f}\nSrc: Orion Terminal"
                                        send_telegram(msg)
                                        alert_history.append(symbol) # Tandai biar gak spam
                                
                                # Simpan ke Batch
                                symbol_clean = symbol.replace('.', '_')
                                data_batch[symbol_clean] = coin_data
                                count += 1
                                i = data_idx - 1
                            else: i += 1
                        else: i += 1
                    else: i += 1

                if data_batch:
                    # 1. Upload Data
                    ref_data.update(data_batch)
                    
                    # 2. Update Heartbeat (Tanda Bot Masih Hidup)
                    ref_status.set({
                        'last_active': ts,
                        'total_coins': count,
                        'status': 'ONLINE',
                        'server': 'GitHub Actions'
                    })
                    
                    print(f"✅ [{ts}] Upload: {count} Koin + Smart Analysis")
                    sys.stdout.flush()
                
                time.sleep(5) 

            except Exception: pass
        
        page.quit()

    except Exception: pass

if __name__ == "__main__":
    run_ultimate_bot()
    sys.exit(0)
