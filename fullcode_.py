import pandas as pd
import pandas_ta as ta
from creds import *  

class TripleEMAStrategyOptimized:
    def __init__(self,
                 ema_fast_len=5, ema_med_len=20, ema_long_len=63, ema_macro_len=120,ema_ultra_len=233,
                 rsi_len=14, rsi_thresh=50,
                 atr_len=14, atr_mult=2.0,
                 risk_percent=1.0, reward_rr=2.0,
                 swing_lookback=30,
                 rsi_trend_tf='30min',):
        self.ema_fast_len = ema_fast_len
        self.ema_med_len = ema_med_len
        self.ema_long_len = ema_long_len
        self.ema_macro_len = ema_macro_len
        self.ema_ultra_len=ema_ultra_len
        self.rsi_len = rsi_len
        self.rsi_thresh = rsi_thresh
        self.atr_len = atr_len
        self.atr_mult = atr_mult
        self.risk_percent = risk_percent
        self.reward_rr = reward_rr
        self.swing_lookback = swing_lookback
        self.rsi_trend_tf = rsi_trend_tf

        self.df = pd.DataFrame()
        self.last_signal = None
        self.last_position = None

    def load_historical_data(self, raw_data):
        self.df = pd.DataFrame(raw_data)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.sort_values(by='timestamp', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        # Calculate indicators
        self.df['ema_fast'] = ta.ema(self.df['close'], length=self.ema_fast_len)
        self.df['ema_med'] = ta.ema(self.df['close'], length=self.ema_med_len)
        self.df['ema_long'] = ta.ema(self.df['close'], length=self.ema_long_len)
        self.df['ema_macro'] = ta.ema(self.df['close'], length=self.ema_macro_len)
        self.df['ema_ultra_len'] = ta.ema(self.df['close'], length=self.ema_ultra_len)
        self.df['rsi'] = ta.rsi(self.df['close'], length=self.rsi_len)
        self.df['atr'] = ta.atr(self.df['high'], self.df['low'], self.df['close'], length=self.atr_len)
        self.df['swing_high'] = self.df['high'].rolling(self.swing_lookback).max()
        self.df['swing_low'] = self.df['low'].rolling(self.swing_lookback).min()
        columns_to_round = [
            'ema_fast', 'ema_med', 'ema_long', 'ema_macro', 'ema_ultra_len',
            'rsi', 'atr', 'swing_high', 'swing_low'
        ]
        self.df[columns_to_round] = self.df[columns_to_round].round(2)
        # First, ensure 'timestamp' is parsed as datetime (if not already)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])

        # If the timestamp is already tz-aware (e.g. +05:30), use only tz_convert
        self.df['timestamp'] = self.df['timestamp'].dt.tz_convert('Asia/Kolkata')

        # Set timezone to Asia/Kolkata
        # self.df['timestamp'] = self.df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')

        # Session: 9:15 AM to 1:15 PM IST
        self.df['in_session'] = self.df['timestamp'].dt.time.between(
            pd.to_datetime("09:15").time(),
            pd.to_datetime("22:15").time()
        )
        df_rsi = self.df.set_index('timestamp')
        rsi_1h = df_rsi['close'].resample(self.rsi_trend_tf).last()
        rsi_1h = ta.rsi(rsi_1h, length=self.rsi_len).round(2)
        # Forward fill to align with original df
        rsi_1h = rsi_1h.reindex(self.df['timestamp'], method='ffill').reset_index(drop=True)
        self.df['rsi_trend'] = rsi_1h

        # Trend filter columns
        self.df['isLongTrend'] = self.df['rsi_trend'] > self.rsi_thresh
        self.df['isShortTrend'] = self.df['rsi_trend'] < self.rsi_thresh


        # Exit time: 14:25 IST
        self.df['exit_time'] = self.df['timestamp'].dt.time == pd.to_datetime("22:25").time()

        # Save processed data
        self.df.to_csv('processed_historical_data.csv', index=False)


    def load_historical_data1(self, raw_data):
        self.df = pd.DataFrame(raw_data)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.sort_values(by='timestamp', inplace=True)
        self.df.reset_index(drop=True, inplace=True)

        # Calculate indicators
        self.df['ema_fast'] = ta.ema(self.df['close'], length=self.ema_fast_len)
        self.df['ema_med'] = ta.ema(self.df['close'], length=self.ema_med_len)
        self.df['ema_long'] = ta.ema(self.df['close'], length=self.ema_long_len)
        self.df['ema_macro'] = ta.ema(self.df['close'], length=self.ema_macro_len)
        self.df['ema_ultra_len'] = ta.ema(self.df['close'], length=self.ema_ultra_len)
        self.df['rsi'] = ta.rsi(self.df['close'], length=self.rsi_len)
        self.df['atr'] = ta.atr(self.df['high'], self.df['low'], self.df['close'], length=self.atr_len)
        self.df['swing_high'] = self.df['high'].rolling(self.swing_lookback).max()
        self.df['swing_low'] = self.df['low'].rolling(self.swing_lookback).min()

        # Round indicator values to 2 decimal places
        round_cols = [
            'ema_fast', 'ema_med', 'ema_long', 'ema_macro', 'ema_ultra_len',
            'rsi', 'atr', 'swing_high', 'swing_low'
        ]
        self.df[round_cols] = self.df[round_cols].round(2)

        # Set timezone to Asia/Kolkata (ensure timestamp is tz-aware)
        # self.df['timestamp'] = self.df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        if self.df['timestamp'].dt.tz is None:
            self.df['timestamp'] = self.df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
        else:
            self.df['timestamp'] = self.df['timestamp'].dt.tz_convert('Asia/Kolkata')

        # Session: 9:15 AM to 1:15 PM IST
        self.df['in_session'] = self.df['timestamp'].dt.time.between(
            pd.to_datetime("09:15").time(),
            pd.to_datetime("13:15").time()
        )

        # RSI trend from higher timeframe
        df_rsi = self.df.set_index('timestamp')
        rsi_1h = df_rsi['close'].resample(self.rsi_trend_tf.replace('m', 'min')).last()
        rsi_1h = ta.rsi(rsi_1h, length=self.rsi_len)
        rsi_1h = rsi_1h.reindex(self.df['timestamp'], method='ffill').reset_index(drop=True)
        self.df['rsi_trend'] = rsi_1h.round(2)

        # Trend filters
        self.df['isLongTrend'] = self.df['rsi_trend'] > self.rsi_thresh
        self.df['isShortTrend'] = self.df['rsi_trend'] < self.rsi_thresh

        # Exit time: 14:25 IST
        self.df['exit_time'] = self.df['timestamp'].dt.time == pd.to_datetime("14:25").time()

        # Save to CSV
        self.df.to_csv('processed_historical_data.csv', index=False)


    def add_live_data(self, timestamp, open_, high, low, close, volume):
        # Parse the timestamp with timezone awareness
        ts = pd.to_datetime(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize('Asia/Kolkata')  # If naive, localize to IST
        else:
            ts = ts.tz_convert('Asia/Kolkata')   # If already tz-aware, convert to IST

        new_row = {
            'timestamp': ts,
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        }

        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        idx = self.df.index[-1]

        # Calculate indicators for the new row only
        for name, length in [
            ('ema_fast', self.ema_fast_len),
            ('ema_med', self.ema_med_len),
            ('ema_long', self.ema_long_len),
            ('ema_macro', self.ema_macro_len)
        ]:
            ema_series = ta.ema(self.df['close'], length=length).round(2)
            self.df.at[idx, name] = ema_series.iloc[-1] if ema_series is not None else None

        rsi_series = ta.rsi(self.df['close'], length=self.rsi_len).round(2)
        self.df.at[idx, 'rsi'] = rsi_series.iloc[-1] if rsi_series is not None else None

        atr_series = ta.atr(self.df['high'], self.df['low'], self.df['close'], length=self.atr_len).round(2)
        self.df.at[idx, 'atr'] = atr_series.iloc[-1] if atr_series is not None else None

        self.df.at[idx, 'swing_high'] = self.df['high'].rolling(self.swing_lookback).max().iloc[-1]
        self.df.at[idx, 'swing_low'] = self.df['low'].rolling(self.swing_lookback).min().iloc[-1]

        # Filter by session (IST) — session 09:15 to 22:15
        ts_time = self.df.at[idx, 'timestamp'].timetz()  # Includes tzinfo, safe for time comparison
        in_session = pd.to_datetime("09:15:00").time() <= ts_time <= pd.to_datetime("22:15:00").time()
        self.df.at[idx, 'in_session'] = in_session

        # --- RSI Trend Filter for new row ---
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        if self.df['timestamp'].dt.tz is None:
            self.df['timestamp'] = self.df['timestamp'].dt.tz_localize('Asia/Kolkata')

        df_rsi = self.df.set_index('timestamp')
        if not isinstance(df_rsi.index, pd.DatetimeIndex):
            df_rsi.index = pd.to_datetime(df_rsi.index)

        df_rsi = df_rsi.sort_index()  # Ensure the index is sorted before resampling
        rsi_1h = df_rsi['close'].resample(self.rsi_trend_tf.lower()).last()
        rsi_1h = ta.rsi(rsi_1h, length=self.rsi_len).round(2)

        # Get latest RSI trend value for current timestamp
        rsi_trend_val = rsi_1h.reindex([ts], method='ffill').iloc[0]
        self.df.at[idx, 'rsi_trend'] = rsi_trend_val
        self.df.at[idx, 'isLongTrend'] = rsi_trend_val > self.rsi_thresh
        self.df.at[idx, 'isShortTrend'] = rsi_trend_val < self.rsi_thresh

    def generate_signal(self):
        if len(self.df) < 2:
            return None

        last = self.df.iloc[-1]
        prev = self.df.iloc[-2]
        # print(last['ema_long'])
        # print(last.to_dict())
        required_cols = [
        'ema_fast', 'ema_med', 'ema_long', 'ema_macro', 'rsi',
    ]
        if any(pd.isna(last[col]) or pd.isna(prev[col]) for col in required_cols):
            print("Insufficient data for signal generation.")
            return None

        # Entry conditions
        long_cond = (
            prev['ema_fast'] <= prev['ema_med'] and
            last['ema_fast'] > last['ema_med'] and
            last['ema_med'] > last['ema_long'] and
            last['ema_long'] > last['ema_macro'] and
            # last['ema_ultra_len'] > last['ema_macro'] and
            last['rsi'] > self.rsi_thresh and
            last['in_session'] 
            and last['isLongTrend']
        )
        short_cond = (
            prev['ema_fast'] >= prev['ema_med'] and
            last['ema_fast'] < last['ema_med'] and
            last['ema_med'] < last['ema_long'] and
            last['ema_long'] < last['ema_macro'] and
            # last['ema_ultra_len'] < last['ema_macro'] and
            last['rsi'] < self.rsi_thresh and
            last['in_session'] 
            and last['isShortTrend']
        )

        # Risk calculation
        stop_loss_long = last['close'] - last['atr'] * self.atr_mult
        stop_loss_short = last['close'] + last['atr'] * self.atr_mult
        risk_amt = 100000 * (self.risk_percent / 100)  # Example: 100k capital
        qty_long = risk_amt / (last['close'] - stop_loss_long) if (last['close'] - stop_loss_long) != 0 else 0
        qty_short = risk_amt / (stop_loss_short - last['close']) if (stop_loss_short - last['close']) != 0 else 0
        take_profit_long = last['close'] + (last['close'] - stop_loss_long) * self.reward_rr
        take_profit_short = last['close'] - (stop_loss_short - last['close']) * self.reward_rr

        # Signal logic
        if self.last_position is None:
            print(f"""
            --- Condition Evaluation ---
            Previous EMA Fast: {prev['ema_fast']}
            Previous EMA Med:  {prev['ema_med']}
            Last EMA Fast:     {last['ema_fast']}
            Last EMA Med:      {last['ema_med']}
            Last EMA Long:     {last['ema_long']}
            Last EMA Macro:    {last['ema_macro']}
            Last RSI:          {last['rsi']}
            RSI Threshold:     {self.rsi_thresh}
            In Session:        {last['in_session']}

            Long Condition:
            (prev['ema_fast'] <= prev['ema_med']): {prev['ema_fast'] <= prev['ema_med']}
            (last['ema_fast'] > last['ema_med']): {last['ema_fast'] > last['ema_med']}
            (last['ema_med'] > last['ema_long']): {last['ema_med'] > last['ema_long']}
            (last['ema_long'] > last['ema_macro']): {last['ema_long'] > last['ema_macro']}
            (last['rsi'] > self.rsi_thresh): {last['rsi'] > self.rsi_thresh}
            (last['in_session']): {last['in_session']}

            Result: long_cond = {long_cond}

            Short Condition:
            (prev['ema_fast'] >= prev['ema_med']): {prev['ema_fast'] >= prev['ema_med']}
            (last['ema_fast'] < last['ema_med']): {last['ema_fast'] < last['ema_med']}
            (last['ema_med'] < last['ema_long']): {last['ema_med'] < last['ema_long']}
            (last['ema_long'] < last['ema_macro']): {last['ema_long'] < last['ema_macro']}
            (last['rsi'] < self.rsi_thresh): {last['rsi'] < self.rsi_thresh}
            (last['in_session']): {last['in_session']}

            Result: short_cond = {short_cond}
            time {last['timestamp']}
            Long Trend: {last['isLongTrend']}
            Short Trend: {last['isShortTrend']}
            -----------------------------
            """)
            input("Press Enter to continue...")

            if long_cond:
                self.last_position = 'LONG'
                self.last_signal = {
                    'signal': 'BUY_ENTRY',
                    'qty': qty_long,
                    'stop_loss': stop_loss_long,
                    'take_profit': min(take_profit_long, last['swing_high'])
                }
                return self.last_signal
            elif short_cond:
                self.last_position = 'SHORT'
                self.last_signal = {
                    'signal': 'SELL_ENTRY',
                    'qty': qty_short,
                    'stop_loss': stop_loss_short,
                    'take_profit': max(take_profit_short, last['swing_low'])
                }
                return self.last_signal
        elif self.last_position == 'LONG':
            # Exit if price hits stop loss or take profit
            if last['low'] <= stop_loss_long or last['high'] >= min(take_profit_long, last['swing_high']):
                self.last_position = None
                self.last_signal = {'signal': 'BUY_EXIT'}
                return self.last_signal
        elif self.last_position == 'SHORT':
            if last['high'] >= stop_loss_short or last['low'] <= max(take_profit_short, last['swing_low']):
                self.last_position = None
                self.last_signal = {'signal': 'SELL_EXIT'}
                return self.last_signal

        return None


if __name__ == "__main__":

    historical_data = pd.read_csv('hind_petro_historical_data.csv')
    live_data = pd.read_csv('hind_petro_live_data.csv')
    strategy = TripleEMAStrategyOptimized()
    strategy.load_historical_data(historical_data)
    
    for i in range(len(live_data)):
        ltp_timestamp = live_data['timestamp'].iloc[i]
        ltp_open = live_data['open'].iloc[i]
        ltp_high = live_data['high'].iloc[i]
        ltp_low = live_data['low'].iloc[i]
        ltp_close = live_data['close'].iloc[i]
        ltp_volume = live_data['volume'].iloc[i]
 
       
        strategy.add_live_data(timestamp=ltp_timestamp, open_=ltp_open, high=ltp_high, low=ltp_low, close=ltp_close, volume=ltp_volume)
        signal = strategy.generate_signal()
      
        if signal:
            print('signal ',signal)
    strategy.df.to_csv('hind_petro_processed_live_data.csv', index=False)