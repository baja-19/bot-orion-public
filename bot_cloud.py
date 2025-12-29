import os
import sys
import json
import time
from DrissionPage import ChromiumPage, ChromiumOptions
import firebase_admin
from firebase_admin import credentials, db

# KONFIGURASI
DATABASE_URL = "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/"
URL_TARGET = "https://orionterminal.com/screener"

def init_firebase():
    json_str = os.environ.get("FIREBASE_KEY_JSON")
    if not json_str: return False
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(json_str))
            firebase_admin.initialize_app(cred, {'databaseURL': DATABASE_URL})
        return True
    except: return False

def run_spy_bot():
    print("🕵️‍♂️ BOT MATA-MATA (DEBUG MODE)...")
    if not init_firebase(): return

    co = ChromiumOptions()
    co.set_argument('--headless=new')
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-gpu')
    co.set_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        page = ChromiumPage(addr_or_opts=co)
        print(f"🌐 Mencoba membuka: {URL_TARGET}")
        
        page.get(URL_TARGET)
        
        print("⏳ Menunggu 15 detik...")
        time.sleep(15)

        # === DIAGNOSA APA YANG DILIHAT BOT ===
        judul = page.title
        print(f"\n🏷️ JUDUL HALAMAN: {judul}")
        
        body_text = page.ele('tag:body').text[:500] # Ambil 500 huruf pertama
        print(f"📄 ISI HALAMAN (500 Huruf Pertama):\n{'-'*30}\n{body_text}\n{'-'*30}")

        # Cek Indikator Blokir
        if "Just a moment" in judul or "Verify" in body_text or "Cloudflare" in body_text:
            print("🚨 KESIMPULAN: DIBLOKIR CLOUDFLARE! (IP GitHub ditolak)")
        elif "Screener" in judul or "BTC" in body_text:
            print("✅ KESIMPULAN: BERHASIL MASUK! (Masalah ada di parsing data)")
        else:
            print("⚠️ KESIMPULAN: Halaman aneh/kosong.")

        page.quit()

    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    run_spy_bot()
