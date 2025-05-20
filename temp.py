import pandas as pd
from datetime import datetime



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
            self.df.to_csv('t1.csv', index=False)

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
        # print(new_row)
        return self.generate_signal()

    def generate_signal(self):
        if len(self.df) < 2:
            return None

        last = self.df.iloc[-1]

        short_temp = last['EMA_short']
        middle_temp = last['EMA_medium']
        long_temp = last['EMA_long']

        # timestamp = last.name  # assuming your df index is datetime

        # ENTRY: Not in any position
        if self.last_position is None:
            # SHORT entry
            # if middle_temp < long_temp and short_temp < middle_temp:
            if middle_temp < long_temp and short_temp < middle_temp and short_temp < long_temp:
                self.last_position = 'SHORT'
                self.last_signal = 'SELL_ENTRY'
                print(f">> 📉 SELL_ENTRY triggered at time  {ltp_timestamp} long {long_temp} middle {middle_temp} short {short_temp}")
                return 'SELL_ENTRY'

            # LONG entry
            # elif middle_temp < long_temp and short_temp > middle_temp:
            elif middle_temp > long_temp and short_temp > middle_temp and short_temp > long_temp:
                self.last_position = 'LONG'
                self.last_signal = 'BUY_ENTRY'
                print(f">> 📈 BUY_ENTRY triggered at {ltp_timestamp} long {long_temp} middle {middle_temp} short {short_temp}")
                return 'BUY_ENTRY'

        # EXIT from SHORT
        elif self.last_position == 'SHORT':
            # short_temp > middle_temp

            if short_temp > middle_temp:
                self.last_position = None
                self.last_signal = 'SELL_EXIT'
                print(f">> 📈 SELL_EXIT from SHORT at {ltp_timestamp} long {long_temp} middle {middle_temp} short {short_temp}")
                return 'SELL_EXIT'

        # EXIT from LONG
        elif self.last_position == 'LONG':
            # short_temp < middle_temp
            if short_temp < middle_temp:
                self.last_position = None
                self.last_signal = 'BUY_EXIT'
                print(f">> 📉 BUY_EXIT from LONG at {ltp_timestamp} long {long_temp} middle {middle_temp} short {short_temp}")
                return 'BUY_EXIT'

        # No signal
        return None


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
        # print(ltp_price)
        signal = strategy.add_live_price(ltp_timestamp, ltp_price)
        if signal:
            print(ltp_price)
        # print(f"Signal: {signal}")
        # time.sleep(1)
        







