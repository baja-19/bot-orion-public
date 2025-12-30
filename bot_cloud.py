import os
import sys
import time
import json
import random
import requests
import logging
import hashlib
import threading
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

# ==========================================
# 1. KONFIGURASI GLOBAL
# ==========================================
# Masukkan URL Firebase Anda di Environment Variables atau ganti string di bawah
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL", "https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/")
INITIAL_COOKIES = os.environ.get("ORION_COOKIES_JSON", "{}") 

# Konfigurasi Interval
ORION_API_URL = "https://orionterminal.com/api/screener"
CYCLE_ACTIVE_SEC = 270   # Bot aktif selama 4.5 menit
CYCLE_PAUSE_SEC = 30     # Istirahat 30 detik untuk menghindari ban IP
POLL_INTERVAL = 3        # Ambil data setiap 3 detik saat siklus aktif
DATA_LIMIT = 1000        # Ambil maksimal koin yang tersedia
MAX_SNAPSHOTS_TO_KEEP = 100 # Simpan 100 data terakhir di Firebase

# Setup Logging Indonesian
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Harvester-V5")

# ==========================================
# 2. MESIN INTEGRITAS (CHAINING)
# ==========================================
class IntegrityEngine:
    def __init__(self, firebase_url):
        self.firebase_url = firebase_url.rstrip('/')
        self.prev_hash = self._load_last_hash()

    def _load_last_hash(self) -> str:
        """Mengambil hash terakhir dari DB agar audit trail tidak putus."""
        try:
            url = f"{self.firebase_url}/orion_snapshots.json?orderBy=\"$key\"&limitToLast=1"
            res = requests.get(url, timeout=10)
            if res.status_code == 200 and res.json():
                last_key = list(res.json().keys())[0]
                last_data = res.json()[last_key]
                if '_integrity' in last_data:
                    h = last_data['_integrity'].get('chain_hash')
                    logger.info(f"🔗 Melanjutkan rantai data dari hash: {h[:8]}...")
                    return h
        except: pass
        return "0" * 64

    def compute_hashes(self, data: Dict) -> Tuple[str, str]:
        """Menghasilkan hash data murni dan hash rantai (Blockchain-style)."""
        data_str = json.dumps(data, sort_keys=True)
        data_hash = hashlib.sha256(data_str.encode()).hexdigest()
        chain_hash = hashlib.sha256(f"{self.prev_hash}{data_hash}".encode()).hexdigest()
        self.prev_hash = chain_hash
        return data_hash, chain_hash

# ==========================================
# 3. PEMROSES DATA (CLEANING)
# ==========================================
class DataProcessor:
    @staticmethod
    def clean_ticker(raw_key: str) -> str:
        """Membersihkan nama koin: 'BTC/USDT-binance' -> 'BTC'."""
        return raw_key.split('-')[0].split('/')[0].upper().replace('USDT', '')

    @staticmethod
    def parse(raw_data: Dict) -> Dict:
        """Mengekstraksi seluruh field penting dari Orion tanpa filter ketat."""
        processed = {}
        fetch_time = datetime.now().isoformat()

        # Handle format data Orion (Bisa Dict atau List)
        items = []
        if isinstance(raw_data, dict):
            items = raw_data.items()
        elif isinstance(raw_data, list):
            items = [(x.get('ticker', 'UNK'), x) for x in raw_data]

        for k, v in items:
            try:
                symbol = DataProcessor.clean_ticker(str(k))
                if symbol in ['USDT', 'USDC', 'DAI', 'BGB']: continue
                
                # Ambil semua data numerik yang tersedia
                processed[symbol] = {
                    'price': float(v.get('11') or v.get('last_price') or v.get('price') or 0),
                    'vol_24h': float(v.get('10') or v.get('volume_24h') or v.get('volume') or 0),
                    'change_24h': float(v.get('6') or v.get('change_24h') or v.get('change') or 0),
                    'rsi': float(v.get('rsi') or v.get('rsi_14') or 50),
                    'funding': float(v.get('funding_rate') or v.get('funding') or 0),
                    'oi': float(v.get('oi') or v.get('open_interest') or 0),
                    'ts': fetch_time
                }
            except: continue
            
        return processed

# ==========================================
# 4. KONEKSI & SELF-HEALING
# ==========================================
class OrionScraper:
    def __init__(self, firebase_url):
        self.session = requests.Session()
        self.fb_url = firebase_url.rstrip('/')
        self._setup_headers()
        self._load_initial_cookies()

    def _setup_headers(self):
        self.session.headers.update({
            'accept': 'application/json',
            'referer': 'https://orionterminal.com/screener',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        })

    def _load_initial_cookies(self):
        try:
            cookies = json.loads(INITIAL_COOKIES)
            self.session.cookies.update(cookies)
        except: pass

    def scavenge_cookies(self) -> bool:
        """Mencoba memuat cookie baru dari Firebase jika kena blokir."""
        logger.warning("🔄 Mendeteksi blokir (403). Mencoba ambil cookie baru dari Firebase...")
        try:
            res = requests.get(f"{self.fb_url}/config/orion_cookies.json", timeout=10)
            if res.status_code == 200 and res.json():
                self.session.cookies.update(res.json())
                logger.info("✅ Cookie berhasil diperbarui secara otomatis.")
                return True
        except: pass
        return False

    def fetch_all(self) -> Optional[Dict]:
        """Mengambil data mentah dari Orion."""
        params = {"limit": DATA_LIMIT, "sort": "volume", "order": "desc"}
        try:
            res = self.session.get(ORION_API_URL, params=params, timeout=15)
            if res.status_code == 200:
                return res.json()
            elif res.status_code in [403, 401]:
                if self.scavenge_cookies():
                    return self.fetch_all() # Retry
            return None
        except: return None

# ==========================================
# 5. FIREBASE WRITER
# ==========================================
class FirebaseWriter:
    def __init__(self, db_url):
        self.db_url = db_url.rstrip('/')

    def push_snapshot(self, market_data: Dict, integrity: Tuple[str, str]):
        if not market_data: return
        
        ts_key = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        data_hash, chain_hash = integrity
        
        payload = {
            "market": market_data,
            "_integrity": {
                "hash": data_hash,
                "chain_hash": chain_hash,
                "count": len(market_data),
                "server_ts": time.time()
            }
        }
        
        try:
            res = requests.put(f"{self.db_url}/orion_snapshots/{ts_key}.json", json=payload, timeout=10)
            if res.status_code == 200:
                logger.info(f"✅ Data Terkirim: {ts_key} | Koin: {len(market_data)} | Hash: {chain_hash[:8]}")
                self.cleanup()
        except Exception as e:
            logger.error(f"❌ Gagal kirim ke Firebase: {e}")

    def cleanup(self):
        """Menghapus data lama agar Firebase tidak bengkak."""
        try:
            res = requests.get(f"{self.db_url}/orion_snapshots.json?shallow=true", timeout=5)
            if res.status_code != 200: return
            
            keys = sorted(list(res.json().keys()))
            if len(keys) > MAX_SNAPSHOTS_TO_KEEP:
                to_delete = keys[:-MAX_SNAPSHOTS_TO_KEEP]
                for k in to_delete[:5]: # Hapus 5 sekaligus
                    requests.delete(f"{self.db_url}/orion_snapshots/{k}.json")
        except: pass

# ==========================================
# 🚀 MAIN LOOP (SINKRONISASI)
# ==========================================
def start_harvester():
    if not FIREBASE_DB_URL:
        logger.critical("🔥 Error: FIREBASE_DB_URL belum diatur!")
        return

    scraper = OrionScraper(FIREBASE_DB_URL)
    writer = FirebaseWriter(FIREBASE_DB_URL)
    integrity = IntegrityEngine(FIREBASE_DB_URL)
    
    logger.info("🚀 ORION DATA HARVESTER v5.2 DIMULAI")
    
    while True:
        cycle_start = time.time()
        logger.info("🟢 Memulai Siklus Pengambilan Data...")

        while (time.time() - cycle_start) < CYCLE_ACTIVE_SEC:
            loop_start = time.time()
            
            # 1. Ambil Data Mentah
            raw = scraper.fetch_all()
            
            if raw:
                # 2. Proses & Bersihkan
                clean_data = DataProcessor.parse(raw)
                
                if len(clean_data) > 50: # Validasi minimal koin
                    # 3. Hitung Hash Integritas
                    hashes = integrity.compute_hashes(clean_data)
                    
                    # 4. Kirim ke Firebase
                    writer.push_snapshot(clean_data, hashes)
                else:
                    logger.warning(f"⚠️ Data terlalu sedikit ({len(clean_data)}), melewati snapshot ini.")
            else:
                logger.error("❌ Gagal mendapatkan data dari Orion Terminal.")

            # Jeda antar request
            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))

        logger.info(f"⏸️ Siklus Selesai. Istirahat {CYCLE_PAUSE_SEC}s untuk keamanan IP...")
        time.sleep(CYCLE_PAUSE_SEC)

if __name__ == "__main__":
    try:
        start_harvester()
    except KeyboardInterrupt:
        logger.info("🛑 Bot dihentikan manual.")
    except Exception as e:
        logger.critical(f"💀 CRASH FATAL: {e}")
