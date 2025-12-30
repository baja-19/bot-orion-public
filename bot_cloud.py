import os
import json
import time
import random
import hashlib
import threading
import warnings
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

import requests
import ccxt

warnings.filterwarnings('ignore')
import logging

# Setup Logging Sederhana
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("SmartBot")

# ==========================================
# 1. KONFIGURASI (WAJIB DIISI)
# ==========================================
# 🔐 API KEY BITGET
BITGET_API_KEY = 'bg_816a44ad290443577691a0aa29f70879'
BITGET_SECRET_KEY = '6005af3430fc476d8bef74be746f29385da8a48749d2fcca2a9eff2033b30209'
BITGET_PASSPHRASE = 'dzakki04'

# 🔥 FIREBASE
FIREBASE_ROOT = 'https://quant-trading-d5411-default-rtdb.asia-southeast1.firebasedatabase.app/'
URL_STATUS = FIREBASE_ROOT + 'account_status.json'
URL_TRADES_BASE = FIREBASE_ROOT + 'trades' 

# ⚙️ SETTING TRADING
# Ganti ke 'LIVE' jika ingin pakai uang asli, 'DEMO' untuk uang mainan
TRADING_MODE = "DEMO" 

MODAL_PER_TRADE = 15.0  # USDT
LEVERAGE = 5
MAX_POSITIONS = 3
STOP_LOSS_PCT = 0.015   # 1.5%
TAKE_PROFIT_RATIO = 2.0 

# Indikator RSI
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

# 🌐 ORION CONFIG (COOKIES & HEADERS)
ORION_URL = 'https://orionterminal.com/api/screener'
ORION_COOKIES = {
    '_ga': 'GA1.1.1647406654.1766912547',
    '_ga_9CYVGBD33S': 'GS2.1.s1766927197$o3$g1$t1766927218$j39$l0$h0'
}
ORION_HEADERS = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'referer': 'https://orionterminal.com/screener',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

# ==========================================
# 2. SMART DATA PARSER (INTI PERBAIKAN)
# ==========================================
@dataclass
class CoinData:
    symbol: str
    price: float
    rsi: float
    
    @classmethod
    def parse(cls, ticker_raw: str, data: Dict) -> 'CoinData':
        """
        Fungsi pintar untuk mencari data di antara tumpukan kode angka Orion.
        """
        # 1. Cari Harga (Prioritas: Key '11', lalu 'last_price', lalu 'price')
        price = 0.0
        # List kemungkinan key untuk HARGA
        price_keys = ['11', 'last_price', 'price', 'close', 'p', 'last']
        
        for k in price_keys:
            val = data.get(k)
            if val is not None:
                try:
                    price = float(val)
                    if price > 0: break 
                except: continue

        # 2. Cari RSI (Prioritas: Key '50', lalu 'rsi_14', lalu 'rsi')
        rsi = 50.0
        # List kemungkinan key untuk RSI
        rsi_keys = ['50', 'rsi_14', 'rsi', 'r', '48']
        
        for k in rsi_keys:
            val = data.get(k)
            if val is not None:
                try:
                    r_val = float(val)
                    if 0 <= r_val <= 100: # RSI harus 0-100
                        rsi = r_val
                        break
                except: continue

        return cls(symbol=ticker_raw, price=price, rsi=rsi)

# ==========================================
# 3. ORION COLLECTOR
# ==========================================
class OrionCollector:
    def fetch_data(self):
        print("\n🔍 Mengambil Data Orion...", end="")
        try:
            # Tambahkan parameter sort agar data tidak kosong
            params = {"limit": 500, "sort": "volume", "order": "desc"}
            res = requests.get(ORION_URL, params=params, cookies=ORION_COOKIES, headers=ORION_HEADERS, timeout=15)
            
            if res.status_code == 200:
                raw = res.json()
                market_map = {}
                
                # Handle format List atau Dict
                items = []
                if isinstance(raw, dict):
                    if 'data' in raw: items = [(x.get('ticker'), x) for x in raw['data']]
                    else: items = raw.items()
                elif isinstance(raw, list):
                    items = [(x.get('ticker'), x) for x in raw]

                count = 0
                for key, val in items:
                    try:
                        # Bersihkan Ticker
                        t_raw = val.get('ticker') or key
                        if not t_raw: continue
                        
                        # Ambil bagian depan sebelum '/' atau '-'
                        if '/' in str(t_raw): base = str(t_raw).split('/')[0]
                        elif '-' in str(t_raw): base = str(t_raw).split('-')[0]
                        else: base = str(t_raw)
                        
                        clean_ticker = base.upper().replace('USDT', '')
                        
                        # Parse menggunakan Smart Parser
                        coin_obj = CoinData.parse(clean_ticker, val)
                        
                        if coin_obj.price > 0:
                            market_map[clean_ticker] = coin_obj
                            count += 1
                    except: continue
                
                print(f" ✅ Sukses: {count} koin terpindai.")
                return market_map
            
            print(f" ❌ Gagal (Status: {res.status_code})")
            return {}
        except Exception as e:
            print(f" ❌ Error Koneksi: {e}")
            return {}

# ==========================================
# 4. FIREBASE & UTILS
# ==========================================
class FirebaseManager:
    def generate_id(self, symbol, side):
        return hashlib.md5(f"{symbol}{side}{time.time()}".encode()).hexdigest()[:12]

    def log_trade(self, data):
        try:
            url = f"{URL_TRADES_BASE}/{data['id']}.json"
            requests.patch(url, json=data, timeout=5)
        except: pass

    def stream_status(self, exchange):
        try:
            bal = exchange.fetch_balance()
            # Cek USDT atau SUSD
            eq = float(bal.get('total', {}).get('USDT', 0))
            if eq == 0: eq = float(bal.get('total', {}).get('SUSD', 0))
            
            pos = exchange.fetch_positions()
            active_list = []
            pnl_total = 0

            for p in pos:
                if float(p.get('contracts', 0)) > 0:
                    pnl_total += float(p['unrealizedPnl'])
                    active_list.append({
                        "symbol": p['symbol'], "side": p['side'], "pnl": float(p['unrealizedPnl']),
                        "entry": float(p['entryPrice']), "roe": float(p['percentage'])
                    })

            requests.put(URL_STATUS, json={
                "equity": eq, "pnl_floating": pnl_total,
                "positions": active_list, "last_update": datetime.now().strftime("%H:%M:%S")
            }, timeout=3)
        except: pass

# ==========================================
# 5. TRADING ENGINE
# ==========================================
class TradingBot:
    def __init__(self):
        self.orion = OrionCollector()
        self.firebase = FirebaseManager()
        self.exchange = None
        self.active_monitors = []
        # Watchlist Top Coins
        self.watchlist = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'AVAX', 'DOT', 'LINK', 'DOGE', 'MATIC']

    def connect(self):
        try:
            self.exchange = ccxt.bitget({
                'apiKey': BITGET_API_KEY, 'secret': BITGET_SECRET_KEY, 'password': BITGET_PASSPHRASE,
                'options': {'defaultType': 'swap'}
            })
            if TRADING_MODE == 'DEMO': 
                self.exchange.set_sandbox_mode(True)
                logger.info("🔧 MODE: DEMO (SANDBOX)")
            
            self.exchange.load_markets()
            logger.info("✅ Exchange Connected!")
        except Exception as e:
            logger.error(f"❌ Gagal Connect: {e}")
            sys.exit()

    def run(self):
        self.connect()
        
        # Background Stream
        def bg_stream():
            while True:
                self.firebase.stream_status(self.exchange)
                time.sleep(5)
        threading.Thread(target=bg_stream, daemon=True).start()

        logger.info("🚀 Bot Berjalan... Menunggu Sinyal RSI...")
        
        cooldowns = {}
        
        while True:
            try:
                # 1. Ambil Data Market
                market_data = self.orion.fetch_data()
                if not market_data:
                    time.sleep(5); continue

                # 2. Cek Watchlist
                for coin in self.watchlist:
                    # Mapping Nama (Orion -> Bitget)
                    search_key = coin
                    bitget_symbol = f"{coin}/USDT:USDT"
                    
                    if coin == 'MATIC': 
                        # Update: Bitget pakai POL, Orion mungkin masih MATIC
                        bitget_symbol = "POL/USDT:USDT"

                    data = market_data.get(search_key)
                    
                    # Jika data tidak ketemu, skip
                    if not data: continue 

                    # Cek RSI
                    rsi = data.rsi
                    price = data.price
                    
                    signal = None
                    if rsi > 0 and rsi < RSI_OVERSOLD:
                        signal = 'buy'
                        reason = f"RSI Oversold ({rsi:.1f})"
                    elif rsi > RSI_OVERBOUGHT:
                        signal = 'sell'
                        reason = f"RSI Overbought ({rsi:.1f})"

                    # Eksekusi
                    if signal:
                        # Cek Cooldown (Jangan beli coin yg sama dlm 5 menit)
                        if (time.time() - cooldowns.get(coin, 0)) > 300:
                            # Cek Slot
                            if len(self.active_monitors) < MAX_POSITIONS:
                                self.execute_trade(bitget_symbol, signal, price, reason)
                                cooldowns[coin] = time.time()
                            else:
                                logger.info(f"⚠️ Slot Penuh. Skip {coin}")
                        else:
                            # logger.info(f"⏳ Cooldown: {coin}")
                            pass

                time.sleep(10 + random.randint(1, 5))

            except KeyboardInterrupt: sys.exit()
            except Exception as e:
                logger.error(f"Loop Error: {e}")
                time.sleep(10)

    def execute_trade(self, symbol, side, price, reason):
        logger.info(f"\n🔥 SIGNAL: {symbol} [{side.upper()}] | {reason}")
        try:
            # 1. Leverage
            try: self.exchange.set_leverage(LEVERAGE, symbol)
            except: pass
            
            # 2. Hitung Size
            # Minimal order bitget biasanya $5. Kita pakai buffer modal per trade.
            amount = (MODAL_PER_TRADE * LEVERAGE) / price
            
            # 3. Market Order
            order = self.exchange.create_order(symbol, 'market', side, amount)
            logger.info(f"✅ Order Terkirim! ID: {order['id']}")
            
            # 4. Log Firebase
            tid = self.firebase.generate_id(symbol, side)
            trade_data = {
                "id": tid, "symbol": symbol, "dir": side.upper(), "entry": price,
                "size": amount, "leverage": LEVERAGE, "status": "OPEN",
                "note": reason, "date": datetime.now().isoformat()
            }
            self.firebase.log_trade(trade_data)
            
            # 5. Monitor Thread
            t = threading.Thread(target=self.monitor_position, args=(symbol, side, price, amount, tid))
            t.daemon = True
            t.start()
            self.active_monitors.append(tid)
            
        except Exception as e:
            logger.error(f"❌ Gagal Eksekusi: {e}")

    def monitor_position(self, symbol, side, entry, amount, tid):
        # Hitung TP/SL
        tp = entry * (1 + STOP_LOSS_PCT * TAKE_PROFIT_RATIO) if side == 'buy' else entry * (1 - STOP_LOSS_PCT * TAKE_PROFIT_RATIO)
        sl = entry * (1 - STOP_LOSS_PCT) if side == 'buy' else entry * (1 + STOP_LOSS_PCT)
        
        logger.info(f"👀 Monitoring {symbol}... TP:{tp:.4f} SL:{sl:.4f}")
        
        start = time.time()
        while (time.time() - start) < 1800: # Max 30 menit
            try:
                ticker = self.exchange.fetch_ticker(symbol)
                curr = ticker['last']
                
                close = False
                reason = ""
                
                if side == 'buy':
                    if curr >= tp: close=True; reason="Take Profit"
                    elif curr <= sl: close=True; reason="Stop Loss"
                else:
                    if curr <= tp: close=True; reason="Take Profit"
                    elif curr >= sl: close=True; reason="Stop Loss"
                
                if close:
                    c_side = 'sell' if side == 'buy' else 'buy'
                    self.exchange.create_order(symbol, 'market', c_side, amount)
                    
                    self.firebase.log_trade({
                        "id": tid, "status": "CLOSED", "exit_price": curr,
                        "close_reason": reason, "exit_time": datetime.now().isoformat()
                    })
                    logger.info(f"🎯 CLOSED {symbol}: {reason}")
                    break
                time.sleep(2)
            except: time.sleep(5)
            
        if tid in self.active_monitors:
            self.active_monitors.remove(tid)

if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
