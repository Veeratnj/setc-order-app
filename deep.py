import pandas as pd
from datetime import datetime

class TripleEMAStrategyOptimized:
    def __init__(self, short=5, medium=21, long=63):
        """
        Triple EMA Crossover Strategy with optimized calculations.
        
        Args:
            short (int): Fast EMA period (default: 5)
            medium (int): Medium EMA period (default: 21)
            long (int): Slow EMA period (default: 63)
        """
        self.short = short
        self.medium = medium
        self.long = long
        
        self.df = pd.DataFrame()
        self.last_ema_short = None
        self.last_ema_medium = None
        self.last_ema_long = None
        self.last_signal = None
        self.last_position = None  # Tracks current position (None, 'LONG', 'SHORT')

    def load_historical_data(self, raw_data):
        """
        Load historical OHLCV data and calculate initial EMAs.
        
        Args:
            raw_data (pd.DataFrame): DataFrame containing OHLCV data with 'timestamp' column
        """
        self.df = raw_data.copy()
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df.sort_values('timestamp', inplace=True)
        self.df.reset_index(drop=True, inplace=True)
        
        # Calculate EMAs
        self.df['EMA_short'] = self.df['close'].ewm(span=self.short, adjust=False).mean()
        self.df['EMA_medium'] = self.df['close'].ewm(span=self.medium, adjust=False).mean()
        self.df['EMA_long'] = self.df['close'].ewm(span=self.long, adjust=False).mean()
        
        # Initialize last EMA values
        self.last_ema_short = self.df['EMA_short'].iloc[-1]
        self.last_ema_medium = self.df['EMA_medium'].iloc[-1]
        self.last_ema_long = self.df['EMA_long'].iloc[-1]

    def add_live_price(self, timestamp, ltp):
        """
        Process live price tick and update EMAs incrementally.
        
        Args:
            timestamp (str/datetime): Timestamp of the new price
            ltp (float): Last traded price
            
        Returns:
            str: Generated trading signal or None
        """
        if self.last_ema_short is None:
            return None  # Historical data not loaded yet

        # Calculate new EMAs incrementally
        alpha_short = 2 / (self.short + 1)
        alpha_medium = 2 / (self.medium + 1)
        alpha_long = 2 / (self.long + 1)
        
        ema_short = (ltp * alpha_short) + (self.last_ema_short * (1 - alpha_short))
        ema_medium = (ltp * alpha_medium) + (self.last_ema_medium * (1 - alpha_medium))
        ema_long = (ltp * alpha_long) + (self.last_ema_long * (1 - alpha_long))
        
        # Update EMA values
        self.last_ema_short = ema_short
        self.last_ema_medium = ema_medium
        self.last_ema_long = ema_long
        
        # Append new data point
        new_row = {
            'timestamp': pd.to_datetime(timestamp),
            'close': ltp,
            'EMA_short': ema_short,
            'EMA_medium': ema_medium,
            'EMA_long': ema_long
        }
        
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        return self.generate_signal(timestamp)

    def generate_signal(self, current_timestamp=None):
        """
        Generate trading signals based on EMA crossovers.
        
        Args:
            current_timestamp (datetime): Timestamp for signal generation
            
        Returns:
            str: Trading signal or None
        """
        if len(self.df) < 2:
            return None

        last = self.df.iloc[-1]
        short, medium, long = last['EMA_short'], last['EMA_medium'], last['EMA_long']
        timestamp = current_timestamp if current_timestamp else last['timestamp']

        # ENTRY LOGIC
        if self.last_position is None:
            # LONG ENTRY: Short > Medium > Long (bullish trend)
            if short > medium > long:
                self.last_position = 'LONG'
                self.last_signal = 'BUY_ENTRY'
                print(f">> 📈 BUY_ENTRY at {timestamp} | EMAs: {short:.2f} > {medium:.2f} > {long:.2f}")
                return 'BUY_ENTRY'
            
            # SHORT ENTRY: Short < Medium < Long (bearish trend)
            elif short < medium < long:
                self.last_position = 'SHORT'
                self.last_signal = 'SELL_ENTRY'
                print(f">> 📉 SELL_ENTRY at {timestamp} | EMAs: {short:.2f} < {medium:.2f} < {long:.2f}")
                return 'SELL_ENTRY'

        # EXIT LOGIC
        elif self.last_position == 'LONG':
            # LONG EXIT: Short crosses below Medium
            if short < medium:
                self.last_position = None
                self.last_signal = 'BUY_EXIT'
                print(f">> 📉 BUY_EXIT (Close LONG) at {timestamp} | EMAs: {short:.2f} < {medium:.2f}")
                return 'BUY_EXIT'
                
        elif self.last_position == 'SHORT':
            # SHORT EXIT: Short crosses above Medium
            if short > medium:
                self.last_position = None
                self.last_signal = 'SELL_EXIT'
                print(f">> 📈 SELL_EXIT (Close SHORT) at {timestamp} | EMAs: {short:.2f} > {medium:.2f}")
                return 'SELL_EXIT'

        return None


if __name__ == "__main__":
    # Example usage
    historical_data = pd.read_csv('historical_data15.csv')
    live_data = pd.read_csv('live.csv')
    
    strategy = TripleEMAStrategyOptimized()
    strategy.load_historical_data(historical_data)
    
    for i in range(len(live_data)):
        current_timestamp = live_data['timestamp'].iloc[i]
        current_price = live_data['close'].iloc[i]
        
        signal = strategy.add_live_price(current_timestamp, current_price)
        if signal:
            print(f"Price: {current_price:.2f} | Signal: {signal}")