import logging
import asyncio
import websockets
import json
from datetime import datetime
import time
from database import QuantDB

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BinanceIngestion:
    def __init__(self, symbols=['btcusdt', 'ethusdt'], db_path='market_data.db'):
        self.symbols = [s.lower() for s in symbols]
        self.db = QuantDB(db_path)
        self.base_url = "wss://fstream.binance.com/stream?streams="
        # streams string: btcusdt@trade/ethusdt@trade
        self.stream_url = self.base_url + "/".join([f"{s}@trade" for s in self.symbols])
        self.running = False
        self.buffer = []
        self.last_flush = time.time()
        self.flush_interval = 1.0  # seconds

    async def connect(self):
        self.running = True
        logger.info(f"Connecting to {self.stream_url}...")
        async with websockets.connect(self.stream_url) as websocket:
            logger.info("Connected.")
            while self.running:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    # Data format: data['data'] contains the payload
                    # Payload: e: trade, s: symbol, p: price, q: quantity, T: timestamp
                    if 'data' in data:
                        trade = data['data']
                        self.process_trade(trade)
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("Connection closed. Retrying...")
                    break
                except Exception as e:
                    logger.error(f"Error: {e}")
                    await asyncio.sleep(1)

            # Reconnect loop if running is true
            if self.running:
                await asyncio.sleep(1)
                await self.connect()

    def process_trade(self, trade):
        # normalize
        record = {
            'timestamp': datetime.fromtimestamp(trade['T'] / 1000.0),
            'symbol': trade['s'],
            'price': float(trade['p']),
            'quantity': float(trade['q'])
        }
        self.buffer.append(record)
        
        # Flush if buffer full or time elapsed
        current_time = time.time()
        if len(self.buffer) >= 100 or (current_time - self.last_flush) > self.flush_interval:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        
        # print(f"Flushing {len(self.buffer)} trades...")
        self.db.insert_trades(self.buffer)
        self.buffer = []
        self.last_flush = time.time()

    def stop(self):
        self.running = False

def run_ingestion(symbols):
    ingestion = BinanceIngestion(symbols)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(ingestion.connect())
    except KeyboardInterrupt:
        pass
    finally:
        ingestion.flush()
        loop.close()

if __name__ == "__main__":
    run_ingestion(['btcusdt', 'ethusdt'])
