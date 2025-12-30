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
# KONFIGURASI TINGKAT TINGGI
# ==============================================================================
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"
TIMEOUT_LIMIT = 285 

# DAFTAR 36 VARIABEL LENGKAP
COLUMNS_KEYS = [
    "price", "ticks_5m", "change_5m", "volume_5m", "volatility_15m",
    "volume_1h", "vdelta_1h", "oi_change_8h", "change_1d", "funding_rate",
    "open_interest", "oi_mc_ratio", "btc_corr_1d", "eth_corr_1d", "btc_corr_3d",
    "eth_corr_3d", "btc_beta_1d", "eth_beta_1d", "change_15m", "change_1h",
    "change_8h", "oi_change_15m", "oi_change_1d", "oi_change_1h", "oi_change_5m",
    "volatility_1h", "volatility_5m", "ticks_15m", "ticks_1h", "vdelta_15m",
    "vdelta_1d", "vdelta_5m", "vdelta_8h", "volume_15m", "volume_1d", "volume_8h"
]

# Blacklist Menu Orion agar tidak terbaca sebagai koin
BLACKLIST = ["ALERTS", "CHARTS", "CLI", "SCREENER", "PORTFOLIO", "SETTINGS", "LOGIN", "SIGNUP", "CONNECT", "WALLET", "SEARCH", "FILTER", "COLUMNS", "MARKET"]

def init_firebase():
    """Koneksi Firebase dengan Safe Mode"""
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: 
        print("❌ Kunci FIREBASE_KEY_JSON tidak ditemukan!")
        return False
    try:
        cred_dict = json.loads(json_str)
        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except Exception as e:
        print(f"❌ Firebase Error: {e}")
        return False

def is_clean_symbol(text):
    """Pastikan hanya koin Latin (Bukan Cina/Aneh)"""
    if not text or len(text) < 2: return False
    return bool(re.match(r'^[A-Z0-9/_$-]+$', text))

def activate_all_columns(page):
    """Mencoba mengaktifkan semua kolom lewat JS agar 36 variabel muncul"""
    print("🛠️ Mencoba mengaktifkan semua kolom tabel...")
    js_activate = """
    try {
        // Cari tombol 'Columns' dan klik
        let buttons = Array.from(document.querySelectorAll('button'));
        let colBtn = buttons.find(b => b.innerText.includes('Columns'));
        if(colBtn) colBtn.click();
        
        // Tunggu sebentar dan centang semua checkbox yang ada
        setTimeout(() => {
            let checks = document.querySelectorAll('input[type="checkbox"]');
            checks.forEach(c => { if(!c.checked) c.click(); });
        }, 1000);
    } catch(e) {}
    """
    page.run_js(js_activate)
    time.sleep(2)

def run_beast_mode():
    print("🔥 INITIALIZING ORION BEAST v5.0 (GITHUB ACTIONS)...")
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_argument('--disable-dev-shm-usage')
    co.set_argument('--window-size=2560,1440')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        page.set.timeouts(page_load=60)
        
        print(f"🌐 Menyerbu: {URL_TARGET}")
        page.get(URL_TARGET)
        time.sleep(30)

        # Aktifkan semua menu yang belum ada
        activate_all_columns(page)
        
        # Zoom Out Ekstrem
        page.run_js("document.body.style.zoom = '15%'")
        
        ref = db.reference('screener_full_data')
        status_ref = db.reference('bot_status')
        start_time = time.time()

        while (time.time() - start_time) < TIMEOUT_LIMIT:
            try:
                # 1. MANUVER SCROLL 4 ARAH
                print("🔄 Manuver Snake Scrolling...")
                page.run_js("window.scrollTo(0, 5000);")
                time.sleep(1)
                page.run_js("window.scrollTo(5000, 5000);") # Samping kanan
                time.sleep(1)
                page.run_js("window.scrollTo(0, 0);")
                time.sleep(1)

                # 2. METODE EKSTRAKSI A: Surgical DOM
                js_extract = """
                let data = [];
                let rows = Array.from(document.querySelectorAll('div[role="row"], .table-row, .rt-tr-group'));
                rows.forEach(r => {
                    let text = r.innerText.trim();
                    if(text.length > 10) data.append(text);
                });
                return document.body.innerText;
                """
                raw_text = page.run_js(js_extract)
                lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
                
                data_batch = {}
                count = 0
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # 3. PARSING CERDAS (Adaptive Parsing)
                i = 0
                while i < len(lines):
                    line = lines[i]
                    
                    # Identifikasi Koin: Huruf Besar, Is Latin, Bukan Blacklist
                    if (2 <= len(line) <= 15 and line.isupper() and is_clean_symbol(line) and line not in BLACKLIST):
                        
                        # Cek baris di bawahnya (Harus berupa angka/harga)
                        if i + 1 < len(lines):
                            price_candidate = lines[i+1].replace('$', '').replace(',', '').replace('%', '').strip()
                            
                            is_valid_entry = False
                            try:
                                # Jika baris i+1 berisi angka, berarti koin valid
                                if any(c.isdigit() for c in price_candidate):
                                    is_valid_entry = True
                            except: pass
                            
                            if is_valid_entry:
                                symbol = line
                                coin_data = {'updated': ts}
                                
                                # Ambil 36 variabel ke bawah
                                data_pointer = i + 1
                                vars_found = 0
                                
                                while vars_found < 36 and data_pointer < len(lines):
                                    val = lines[data_pointer]
                                    # Jika menabrak koin selanjutnya, berhenti
                                    if (vars_found > 5 and len(val) <= 15 and val.isupper() and 
                                        is_clean_symbol(val) and val not in BLACKLIST):
                                        break
                                    
                                    key_name = COLUMNS_KEYS[vars_found]
                                    coin_data[key_name] = val
                                    vars_found += 1
                                    data_pointer += 1
                                
                                # Simpan ke batch
                                safe_sym = symbol.replace('.', '_').replace('/', '_')
                                data_batch[safe_sym] = coin_data
                                count += 1
                                i = data_pointer - 1
                            else: i += 1
                        else: i += 1
                    else: i += 1

                # 4. KIRIM DATA
                if data_batch:
                    ref.update(data_batch)
                    status_ref.set({
                        'status': 'ONLINE',
                        'last_active': ts,
                        'coins': count,
                        'mode': 'Beast v5.0'
                    })
                    print(f"✅ [{ts}] BERHASIL: Sedot {count} Koin (36 Variabel).")
                    sys.stdout.flush()
                else:
                    print("⚠️ Data belum terdeteksi. Mencoba refresh browser...")
                    page.refresh()
                    time.sleep(15)

                gc.collect()
                time.sleep(15)

            except Exception as e:
                print(f"⚠️ Glitch di Loop: {e}")
                time.sleep(5)

        page.quit()

    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    run_beast_mode()
    sys.exit(0)
