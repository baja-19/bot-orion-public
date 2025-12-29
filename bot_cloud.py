import os
import sys
import json
import time
import requests # Untuk Telegram
import gc
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# ==============================================================================
# KONFIGURASI TINGKAT TINGGI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 280

# SETTING ALERT TELEGRAM
ALERT_MIN_PRICE_CHANGE = 5.0 # Lapor jika harga naik/turun > 5%
ALERT_MIN_VOLUME = 500_000   # Lapor hanya jika volume > $500k (Anti koin micin)

# DAFTAR MENU YANG HARUS DIHINDARI
BLACKLIST = [
    "ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS",
    "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER",
    "COLUMNS", "EXPORT", "SHARE", "FEEDBACK", "HELP", "MARKET"
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

# Cache untuk mencegah spam notifikasi (Reset setiap restart bot)
alert_history = []

def init_firebase():
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: return False
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(json_str))
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except: return False

def send_telegram(message):
    """Mengirim pesan ke HP Anda"""
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id: return # Skip jika tidak disetting
    
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        requests.post(url, data=payload)
    except: pass

def clean_number(text):
    """Mengubah $1.5M menjadi 1500000.0 (Float)"""
    if not isinstance(text, str): return 0.0
    text = text.replace('$', '').replace(',', '').replace('%', '').replace('+', '').strip()
    multiplier = 1
    if 'M' in text:
        multiplier = 1_000_000
        text = text.replace('M', '')
    elif 'K' in text:
        multiplier = 1_000
        text = text.replace('K', '')
    elif 'B' in text:
        multiplier = 1_000_000_000
        text = text.replace('B', '')
    
    try: return float(text) * multiplier
    except: return 0.0

def run_ultimate_bot():
    print("🚀 BOT ORION ULTIMATE (SMART ANALYSIS)...")
    start_global = time.time()
    
    if not init_firebase(): return

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

        print("⏳ Menunggu Loading (25 detik)...")
        time.sleep(25)
        
        # Cek Body
        if not page.ele('tag:body'):
            print("❌ Gagal Load.")
            return

        # Zoom Out
        try: page.run_js("document.body.style.zoom = '25%'")
        except: pass

        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status') # Folder status

        cycle = 0
        while True:
            if (time.time() - start_global) > TIMEOUT_LIMIT: break

            try:
                # Scroll
                page.run_js("window.scrollTo(0, 10000);"); time.sleep(0.5)
                page.run_js("window.scrollTo(0, 0);"); time.sleep(0.5)
                
                raw_text = page.run_js("return document.body.innerText")
                lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # FILTER CERDAS
                    if (2 <= len(line) <= 6 and line.isalpha() and line.isupper() and line not in BLACKLIST):
                        if i + 1 < len(lines):
                            next_line = lines[i+1].replace('$', '').replace(',', '').strip()
                            
                            # Cek apakah baris bawahnya Angka?
                            is_number = False
                            try: 
                                float(next_line.replace('%', ''))
                                is_number = True
                            except: is_number = False
                            
                            if is_number:
                                # === DATA VALID DITEMUKAN ===
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # Variabel Sementara untuk Analisis
                                raw_price_chg = "0"
                                raw_volume = "0"
                                
                                data_idx = i + 1
                                for key in COLUMNS_KEYS:
                                    if data_idx < len(lines):
                                        val = lines[data_idx]
                                        coin_data[key] = val
                                        
                                        # Tangkap data untuk analisis
                                        if key == "change_1h": raw_price_chg = val
                                        if key == "volume_1h": raw_volume = val
                                        
                                        data_idx += 1
                                    else:
                                        coin_data[key] = "-"
                                
                                # === ANALISIS KECERDASAN BUATAN (AI SEDERHANA) ===
                                chg_num = clean_number(raw_price_chg)
                                vol_num = clean_number(raw_volume)
                                
                                # 1. Hitung Momentum Score (Vol x Change)
                                momentum = vol_num * abs(chg_num)
                                coin_data['momentum_score'] = momentum
                                
                                # 2. Cek Alert Telegram
                                if vol_num > ALERT_MIN_VOLUME and abs(chg_num) > ALERT_MIN_PRICE_CHANGE:
                                    if symbol not in alert_history:
                                        icon = "🚀" if chg_num > 0 else "🔻"
                                        msg = f"{icon} *ORION ALERT: {symbol}*\nChange 1h: {raw_price_chg}\nVol 1h: ${vol_num:,.0f}\nSrc: GitHub Bot"
                                        send_telegram(msg)
                                        alert_history.append(symbol)

                                # Simpan ke Batch
                                symbol_clean = symbol.replace('.', '_')
                                data_batch[symbol_clean] = coin_data
                                count += 1
                                
                                i = data_idx - 1
                            else: i += 1
                        else: i += 1
                    else: i += 1

                if data_batch:
                    # Upload Data
                    ref.update(data_batch)
                    
                    # Upload Detak Jantung (Heartbeat)
                    status_ref.set({
                        "status": "ONLINE",
                        "last_update": ts,
                        "coins_tracked": count,
                        "server": "GitHub Actions"
                    })
                    
                    print(f"✅ [{ts}] Upload: {count} Koin + Smart Analysis")
                    sys.stdout.flush()
                
                cycle += 1
                if cycle >= 10: gc.collect(); cycle = 0
                time.sleep(5)

            except Exception:
                pass
        
        page.quit()

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_ultimate_bot()
    sys.exit(0)
