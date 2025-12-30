import os
import sys
import time
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import requests

# ============================================================================
# CONFIGURATION
# ============================================================================

# Hardcoded fallback for Colab/local testing (FILL THIS IN)
DEFAULT_FIREBASE_URL = "https://your-project.firebaseio.com"

# Environment variables (GitHub Actions will provide these)
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL", DEFAULT_FIREBASE_URL)
ORION_COOKIES_JSON = os.environ.get("ORION_COOKIES_JSON", "{}")

# Timing constraints for GitHub Actions
RUNTIME_LIMIT_SECONDS = 270  # 4 minutes 30 seconds
COOLDOWN_SECONDS = 30
REQUEST_DELAY = 3

# Data validation
MIN_COINS_THRESHOLD = 50

# Orion Terminal endpoint
ORION_API_URL = "https://app.orionprotocol.io/api/market/aggregated"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# UNIVERSAL DATA PROCESSOR (THE ANTI-FRAGILE ENGINE)
# ============================================================================

class DataProcessor:
    """
    Universal parser that handles ANY structure Orion might send:
    - List of objects
    - Dict of objects
    - Nested {"data": [...]} structures
    - Obfuscated numeric keys
    """
    
    # Fallback key mappings (ordered by priority)
    PRICE_KEYS = ['11', 'last_price', 'close', 'price', 'p', 'lastPrice']
    VOLUME_KEYS = ['10', 'volume_24h', 'volume', 'v', 'volume24h', 'vol']
    CHANGE_KEYS = ['6', 'change_24h', 'change', 'ch', 'change24h', 'priceChange']
    RSI_KEYS = ['50', 'rsi_14', 'rsi', '14', 'rsi14']
    SYMBOL_KEYS = ['symbol', 'pair', 's', 'ticker', 'coin']
    
    @staticmethod
    def normalize_structure(raw_data: Any) -> List[Dict]:
        """
        Convert ANY input structure into a List of Dict.
        
        Handles:
        - List directly
        - Dict with 'data' key containing list
        - Dict where values are the items
        """
        logger.info(f"🔍 Raw data type: {type(raw_data).__name__}")
        
        if isinstance(raw_data, list):
            logger.info(f"📦 Structure: Direct list with {len(raw_data)} items")
            return raw_data
        
        elif isinstance(raw_data, dict):
            # Check for nested 'data' key
            if 'data' in raw_data and isinstance(raw_data['data'], list):
                logger.info(f"📦 Structure: Nested dict with 'data' key, {len(raw_data['data'])} items")
                return raw_data['data']
            
            # Check for other common wrapper keys
            for wrapper_key in ['results', 'items', 'coins', 'markets']:
                if wrapper_key in raw_data and isinstance(raw_data[wrapper_key], list):
                    logger.info(f"📦 Structure: Nested dict with '{wrapper_key}' key, {len(raw_data[wrapper_key])} items")
                    return raw_data[wrapper_key]
            
            # Treat dict values as items
            items = list(raw_data.values())
            logger.info(f"📦 Structure: Dict values extracted, {len(items)} items")
            return items
        
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(raw_data)}")
            return []
    
    @staticmethod
    def extract_value(item: Dict, key_candidates: List[str], default: float = 0.0) -> float:
        """
        Try multiple keys in priority order, return first valid value.
        Converts to float with error handling.
        """
        for key in key_candidates:
            if key in item:
                try:
                    value = item[key]
                    if value is None or value == '':
                        continue
                    return float(value)
                except (ValueError, TypeError):
                    continue
        return default
    
    @classmethod
    def parse_item(cls, item: Dict) -> Optional[Dict[str, Union[str, float]]]:
        """
        Extract normalized data from a single item.
        Returns None if critical fields are missing.
        """
        if not isinstance(item, dict):
            return None
        
        # Extract symbol (critical field)
        symbol = None
        for key in cls.SYMBOL_KEYS:
            if key in item and item[key]:
                symbol = str(item[key])
                break
        
        if not symbol:
            return None
        
        # Extract numerical fields with fallbacks
        price = cls.extract_value(item, cls.PRICE_KEYS)
        volume = cls.extract_value(item, cls.VOLUME_KEYS)
        change = cls.extract_value(item, cls.CHANGE_KEYS)
        rsi = cls.extract_value(item, cls.RSI_KEYS)
        
        return {
            'symbol': symbol,
            'price': price,
            'volume_24h': volume,
            'change_24h': change,
            'rsi': rsi,
            'raw_keys': list(item.keys())[:10]  # Store sample for debugging
        }
    
    @classmethod
    def process(cls, raw_data: Any) -> List[Dict]:
        """
        Main entry point: Convert any structure to normalized coin list.
        """
        # Step 1: Normalize to list
        items = cls.normalize_structure(raw_data)
        
        # Step 2: Parse each item
        parsed_coins = []
        failed_count = 0
        
        for i, item in enumerate(items):
            parsed = cls.parse_item(item)
            if parsed:
                parsed_coins.append(parsed)
            else:
                failed_count += 1
                if failed_count <= 3:  # Log first 3 failures
                    logger.warning(f"⚠️ Parsing failed for item {i}: {str(item)[:100]}")
        
        logger.info(f"✅ Parsed {len(parsed_coins)} coins successfully")
        if failed_count > 0:
            logger.info(f"⚠️ Failed to parse {failed_count} items")
        
        return parsed_coins


# ============================================================================
# FIREBASE CLIENT
# ============================================================================

class FirebaseClient:
    """Handles all Firebase operations with integrity validation."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
    
    @staticmethod
    def calculate_hash(data: Dict) -> str:
        """Calculate SHA256 hash for data integrity."""
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def push_snapshot(self, coins: List[Dict]) -> bool:
        """
        Push data snapshot to Firebase with atomic update.
        Returns True on success.
        """
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        payload = {
            'timestamp': timestamp,
            'coin_count': len(coins),
            'coins': coins,
            '_metadata': {
                'harvester_version': '6.0',
                'integrity_hash': self.calculate_hash(coins)
            }
        }
        
        # Atomic PUT to overwrite snapshot
        endpoint = f"{self.base_url}/orion_snapshots/{timestamp.replace(':', '-')}.json"
        
        try:
            response = requests.put(endpoint, json=payload, timeout=10)
            response.raise_for_status()
            logger.info(f"🚀 Pushed {len(coins)} coins to Firebase")
            logger.info(f"📍 Endpoint: {endpoint}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Firebase push failed: {e}")
            return False


# ============================================================================
# MAIN HARVESTER
# ============================================================================

class OrionHarvester:
    """Main orchestrator for data collection."""
    
    def __init__(self):
        self.session = requests.Session()
        self.firebase = FirebaseClient(FIREBASE_DB_URL)
        self.start_time = time.time()
        self._setup_session()
    
    def _setup_session(self):
        """Configure session with cookies and headers."""
        # Parse cookies from environment
        try:
            cookies = json.loads(ORION_COOKIES_JSON)
            for name, value in cookies.items():
                self.session.cookies.set(name, value)
            logger.info(f"🍪 Loaded {len(cookies)} cookies")
        except json.JSONDecodeError:
            logger.warning("⚠️ No valid cookies found, proceeding without authentication")
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://app.orionprotocol.io/',
            'Origin': 'https://app.orionprotocol.io'
        })
    
    def fetch_data(self) -> Optional[Any]:
        """Fetch raw data from Orion API."""
        try:
            logger.info("📡 Fetching data from Orion...")
            response = self.session.get(ORION_API_URL, timeout=15)
            
            # Check for auth failures
            if response.status_code == 401:
                logger.error("❌ Authentication failed - cookies expired")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"✅ Received response: {len(str(data))} bytes")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request failed: {e}")
            return None
    
    def should_continue(self) -> bool:
        """Check if we should continue the loop."""
        elapsed = time.time() - self.start_time
        remaining = RUNTIME_LIMIT_SECONDS - elapsed
        
        if remaining <= 0:
            logger.info(f"⏰ Runtime limit reached ({RUNTIME_LIMIT_SECONDS}s)")
            return False
        
        logger.info(f"⏱️ Remaining time: {int(remaining)}s")
        return True
    
    def harvest_cycle(self) -> bool:
        """
        Single harvest cycle.
        Returns True if data was successfully pushed.
        """
        # Fetch raw data
        raw_data = self.fetch_data()
        if raw_data is None:
            return False
        
        # Parse with universal processor
        coins = DataProcessor.process(raw_data)
        
        # Validate
        if len(coins) < MIN_COINS_THRESHOLD:
            logger.warning(f"⚠️ Only {len(coins)} coins parsed (threshold: {MIN_COINS_THRESHOLD})")
            logger.warning("⚠️ Skipping Firebase push - data may be incomplete")
            return False
        
        # Push to Firebase
        success = self.firebase.push_snapshot(coins)
        return success
    
    def run(self):
        """Main execution loop with GitHub Actions lifecycle."""
        logger.info("=" * 70)
        logger.info("🚀 Orion Harvester v6.0 STARTING")
        logger.info(f"⏰ Runtime limit: {RUNTIME_LIMIT_SECONDS}s")
        logger.info(f"🔥 Firebase URL: {FIREBASE_DB_URL}")
        logger.info("=" * 70)
        
        cycle_count = 0
        success_count = 0
        
        # Main watchdog loop
        while self.should_continue():
            cycle_count += 1
            logger.info(f"\n{'='*70}")
            logger.info(f"🔄 Cycle {cycle_count} starting...")
            logger.info(f"{'='*70}")
            
            success = self.harvest_cycle()
            if success:
                success_count += 1
            
            # Rate limiting
            if self.should_continue():
                logger.info(f"💤 Sleeping {REQUEST_DELAY}s (rate limit)...")
                time.sleep(REQUEST_DELAY)
        
        # Graceful shutdown
        logger.info("\n" + "=" * 70)
        logger.info("🏁 GRACEFUL SHUTDOWN INITIATED")
        logger.info(f"📊 Total cycles: {cycle_count}")
        logger.info(f"✅ Successful pushes: {success_count}")
        logger.info(f"💤 Cooldown period: {COOLDOWN_SECONDS}s")
        logger.info("=" * 70)
        
        time.sleep(COOLDOWN_SECONDS)
        logger.info("👋 Harvester exiting cleanly")
        sys.exit(0)


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Entry point with error handling."""
    try:
        harvester = OrionHarvester()
        harvester.run()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
