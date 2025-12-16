import sqlite3
import pandas as pd
from datetime import datetime

class QuantDB:
    def __init__(self, db_path='market_data.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                symbol TEXT,
                price REAL,
                quantity REAL
            )
        ''')
        
        # Indexes for faster querying
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_sym ON trades(symbol)')
        
        conn.commit()
        conn.close()

    def insert_trades(self, trades_data):
        """
        Insert a batch of trades.
        trades_data: list of tuples/dicts or pandas DataFrame
        """
        if not trades_data:
            return
            
        conn = sqlite3.connect(self.db_path)
        if isinstance(trades_data, pd.DataFrame):
            trades_data.to_sql('trades', conn, if_exists='append', index=False)
        else:
            # Assume list of dicts
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT INTO trades (timestamp, symbol, price, quantity)
                VALUES (:timestamp, :symbol, :price, :quantity)
            ''', trades_data)
            conn.commit()
        conn.close()

    def get_trades(self, symbol, lookback_minutes=60):
        """Fetch trades for a symbol within the last N minutes."""
        conn = sqlite3.connect(self.db_path)
        # Use Python's datetime to ensure consistency with insertion (local time)
        cutoff_time = datetime.now() - pd.Timedelta(minutes=lookback_minutes)
        query = '''
            SELECT timestamp, price, quantity 
            FROM trades 
            WHERE symbol = ? 
            AND timestamp >= ?
            ORDER BY timestamp ASC
        '''
        df = pd.read_sql_query(query, conn, params=(symbol, cutoff_time), parse_dates=['timestamp'])
        conn.close()
        return df

    def get_latest_price(self, symbol):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT price FROM trades WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1', (symbol,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
