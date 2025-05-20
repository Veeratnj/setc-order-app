import time
import pandas as pd
import yfinance as yf
from datetime import datetime
from services import get_auth, get_historical_data
from creds import *


class TripleEMAStrategyOptimized1:
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
        # print(self.df.columns)
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
        # print(self.df.columns)
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

    def generate_signal1(self):
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

    def generate_signal(self):
        if len(self.df) < 2:
            return None

        last = self.df.iloc[-1]

        short_temp = last['EMA_short']
        middle_temp = last['EMA_medium']
        long_temp = last['EMA_long']

        # print('short_temp:', short_temp)
        # print('middle_temp:', middle_temp)  
        # print('long_temp:', long_temp)

        # ENTRY: Not in any position
        if self.last_position is None:
            # Condition: SHORT entry (Sell first)
            if middle_temp < long_temp and short_temp < middle_temp:
                self.last_position = 'SHORT'
                self.last_signal = 'SELL'
                print(f">> 📉 SELL_ENTRY triggered at {last.name}")
                return 'SELL_ENTRY'

            # Condition: LONG entry (Buy first)
            elif middle_temp < long_temp and short_temp > middle_temp:
                self.last_position = 'LONG'
                self.last_signal = 'BUY'
                print(f">> 📈 BUY_ENTRY triggered at {last.name}")
                return 'BUY_ENTRY'

        # EXIT: From SHORT position
        elif self.last_position == 'SHORT':
            if short_temp > middle_temp:
                self.last_position = None
                self.last_signal = None
                print(f">> 📈 BUY_EXIT from SHORT at {last.name}")
                return 'BUY_EXIT'

        # EXIT: From LONG position
        elif self.last_position == 'LONG':
            if short_temp < middle_temp:
                self.last_position = None
                self.last_signal = None
                print(f">> 📉 SELL_EXIT from LONG at {last.name}")
                return 'SELL_EXIT'

        # No Signal
        return None





class TripleEMAStrategyOptimized:
    def __init__(self, short=5, medium=21, long=63):
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
        self.df = pd.DataFrame(raw_data)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.sort_values(by='timestamp', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        self.df['EMA_short'] = self.df['close'].ewm(span=self.short, adjust=False).mean()
        self.df['EMA_medium'] = self.df['close'].ewm(span=self.medium, adjust=False).mean()
        self.df['EMA_long'] = self.df['close'].ewm(span=self.long, adjust=False).mean()

        last_row = self.df.iloc[-1]
        self.last_ema_short = last_row['EMA_short']
        self.last_ema_medium = last_row['EMA_medium']
        self.last_ema_long = last_row['EMA_long']

    def generate_signals1(self):
        signals = []
        flag_long = False
        flag_short = False

        for i in range(len(self.df)):
            row = self.df.iloc[i]
            short_temp = row['EMA_short']
            middle_temp = row['EMA_medium']
            long_temp = row['EMA_long']
            close_price = row['close']
            timestamp = self.df['timestamp'].iloc[i]

            # SELL Entry
            if not flag_long and not flag_short and middle_temp < long_temp and short_temp < middle_temp:
                signals.append("SELL_ENTRY")
                flag_short = True
                print(f"📉 SELL_ENTRY at {timestamp} | Price: {close_price}")

            # BUY Exit (Close Short)
            elif flag_short and short_temp > middle_temp:
                signals.append("BUY_EXIT")
                flag_short = False
                print(f"📈 BUY_EXIT at {timestamp} | Price: {close_price}")

            # BUY Entry
            elif not flag_long and not flag_short and middle_temp < long_temp and short_temp > middle_temp:
                signals.append("BUY_ENTRY")
                flag_long = True
                print(f"📈 BUY_ENTRY at {timestamp} | Price: {close_price}")

            # SELL Exit (Close Long)
            elif flag_long and short_temp < middle_temp:
                signals.append("SELL_EXIT")
                flag_long = False
                print(f"📉 SELL_EXIT at {timestamp} | Price: {close_price}")

            # No Signal
            else:
                signals.append(None)

        self.df['Signal'] = signals
        return self.df

    def generate_signals(self):
        if len(self.df) < 2:
            return None

        prev = self.df.iloc[-2]
        last = self.df.iloc[-1]

        short_temp = last['EMA_short']
        middle_temp = last['EMA_medium']
        long_temp = last['EMA_long']

        # ENTRY: Not in any position
        if self.last_position is None:
            if middle_temp < long_temp and short_temp < middle_temp:
                self.last_position = 'SHORT'
                self.last_signal = 'SELL'
                print(f"📉 SELL_ENTRY at {last['timestamp']} | Price: {last['close']} short {short_temp} middle {middle_temp} long {long_temp}")
                return 'SELL_ENTRY'

            elif middle_temp < long_temp and short_temp > middle_temp:
                self.last_position = 'LONG'
                self.last_signal = 'BUY'
                print(f"📈 BUY_ENTRY at {last['timestamp']} | Price: {last['close']} short {short_temp} middle {middle_temp} long {long_temp}")
                return 'BUY_ENTRY'

        elif self.last_position == 'SHORT' and short_temp > middle_temp:
            self.last_position = None
            self.last_signal = None
            print(f"📈 BUY_EXIT at {last['timestamp']} | Price: {last['close']} short {short_temp} middle {middle_temp} long {long_temp}")
            return 'BUY_EXIT'

        elif self.last_position == 'LONG' and short_temp < middle_temp:
            self.last_position = None
            self.last_signal = None
            print(f"📉 SELL_EXIT at {last['timestamp']} | Price: {last['close']} short {short_temp} middle {middle_temp} long {long_temp}")
            return 'SELL_EXIT'

        return None


   
    def add_live_price(self, timestamp, ltp):
        if self.last_ema_short is None:
            return None

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
        return self.generate_signals()  # Optional real-time signal




if __name__ == "__main__":
    # smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
    # historical_data=get_historical_data(smart_api_obj=smart_api_obj,symboltoken='3045',exchange="NSE", interval="FIVE_MINUTE", fromdate='2025-05-18 00:00', todate='2025-05-19 14:30')
    
    historical_data = pd.read_csv('historical_data15.csv')
    live_data = pd.read_csv('live.csv')
    strategy = TripleEMAStrategyOptimized()
    strategy.load_historical_data(historical_data)
    
    for i in range(len(live_data)):
        ltp_timestamp = live_data['timestamp'].iloc[i]
        ltp_price = live_data['close'].iloc[i]
        # print(f"Processing live data for timestamp: {ltp_timestamp} with price: {ltp_price}")
        
        signal = strategy.add_live_price(ltp_timestamp, ltp_price)
        # print(f"Signal: {signal}")
        # time.sleep(1)
        



