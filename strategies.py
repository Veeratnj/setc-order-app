import pandas as pd
import yfinance as yf
from datetime import datetime
from services import get_auth, get_historical_data
from creds import *


class TripleEMAStrategyOptimized:
    def __init__(self, short=9, medium=21, long=55):
        self.short = short
        self.medium = medium
        self.long = long

        self.df = pd.DataFrame()
        self.last_ema_short = None
        self.last_ema_medium = None
        self.last_ema_long = None
        self.last_signal = None
        self.last_position = None 

    def load_historical_data(self, raw_data):
        # print(raw_data.columns)
        """
        Load OHLCV data and compute EMAs.
        """
        self.df = pd.DataFrame(raw_data)
        print(self.df.columns)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.sort_values(by='timestamp', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        # Calculate EMAs
        self.df['EMA_short'] = self.df['close'].ewm(span=self.short, adjust=False).mean()
        self.df['EMA_medium'] = self.df['close'].ewm(span=self.medium, adjust=False).mean()
        self.df['EMA_long'] = self.df['close'].ewm(span=self.long, adjust=False).mean()

        # Pick last EMA values to continue from
        last_row = self.df.iloc[-1]
        self.last_ema_short = last_row['EMA_short']
        self.last_ema_medium = last_row['EMA_medium']
        self.last_ema_long = last_row['EMA_long']

    def add_live_price(self, timestamp, ltp):
        """
        Add new candle and calculate EMAs incrementally.
        """
        print(self.df.columns)
        if self.last_ema_short is None:
            return None  # historical data not loaded

        # EMA calculation
        alpha_short = 2 / (self.short + 1)
        alpha_medium = 2 / (self.medium + 1)
        alpha_long = 2 / (self.long + 1)

        ema_short = (ltp * alpha_short) + (self.last_ema_short * (1 - alpha_short))
        ema_medium = (ltp * alpha_medium) + (self.last_ema_medium * (1 - alpha_medium))
        ema_long = (ltp * alpha_long) + (self.last_ema_long * (1 - alpha_long))

        self.last_ema_short = ema_short
        self.last_ema_medium = ema_medium
        self.last_ema_long = ema_long

        new_row = {
            'timestamp': pd.to_datetime(timestamp),
            'close': ltp,
            'open': ltp,
            'high': ltp,
            'low': ltp,
            'EMA_short': ema_short,
            'EMA_medium': ema_medium,
            'EMA_long': ema_long
        }

        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        return self.generate_signal()

    def generate_signal(self):
        if len(self.df) < 2:
            return None

        last = self.df.iloc[-1]
        prev = self.df.iloc[-2]

        # BUY signal
        if (
            prev['EMA_short'] < prev['EMA_medium'] < prev['EMA_long'] and
            last['EMA_short'] > last['EMA_medium'] > last['EMA_long']
        ):
            if self.last_signal != 'BUY':
                self.last_signal = 'BUY'
                self.last_position = 'LONG'
                return 'BUY_ENTRY'

        # SELL signal
        if (
            prev['EMA_short'] > prev['EMA_medium'] > prev['EMA_long'] and
            last['EMA_short'] < last['EMA_medium'] < last['EMA_long']
        ):
            if self.last_signal != 'SELL':
                self.last_signal = 'SELL'
                self.last_position = 'SHORT'
                return 'SELL_ENTRY'

        # EXIT conditions
        if self.last_position == 'LONG' and last['EMA_short'] < last['EMA_medium']:
            self.last_position = None
            self.last_signal = None
            return 'BUY_EXIT'

        if self.last_position == 'SHORT' and last['EMA_short'] > last['EMA_medium']:
            self.last_position = None
            self.last_signal = None
            return 'SELL_EXIT'

        return None



if __name__ == "__main__":
    smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
    historical_data=get_historical_data(smart_api_obj=smart_api_obj,symboltoken='3045',exchange="NSE", interval="FIVE_MINUTE", fromdate='2025-05-18 00:00', todate='2025-05-19 14:30')

    strategy = TripleEMAStrategyOptimized()
    strategy.load_historical_data(historical_data)
    ltp_timestamp = '2025-05-19 14:35'
    ltp_price = 795.0

    signal = strategy.add_live_price(ltp_timestamp, ltp_price)
    strategy.df.to_csv('hist.csv',index=False)

    if signal:
        print(f"📈 Signal generated: {signal}")
    else:
        print("📉 No signal generated.")



