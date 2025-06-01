import numpy as np
import pandas as pd
import pandas_ta as ta

def calculate_atr_trailing_stop(self, atr_length: int = 14, atr_mult: float = 2.5):
    """
    Calculate dynamic ATR-based trailing stop-loss for long and short positions.

    Parameters:
    - df: DataFrame containing 'High', 'Low', and 'Close' columns
    - entry_candle: Entry candle number (1-based index)
    - atr_length: ATR period length (default=14)
    - atr_mult: ATR multiplier for SL distance (default=2.5)

    Returns:
    - DataFrame with 'TrailStopLong' and 'TrailStopShort' columns added
    """
    if not {'High', 'Low', 'Close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns.")

    # df = df.copy()
    self.df['ATR'] = ta.atr(high=self.df['High'], low=self.df['Low'], close=self.df['Close'], length=atr_length)

    entry_index = len(self.df) - 1
    if entry_index >= len(self.df) or entry_index < 0:
        raise ValueError("Entry candle index is out of range.")

    trail_stop_long = [np.nan] * len(self.df)
    trail_stop_short = [np.nan] * len(self.df)

    for i in range(entry_index, len(self.df)):
        atr = self.df.at[i, 'ATR']
        if np.isnan(atr):
            continue

        high = self.df.at[i, 'High']
        low = self.df.at[i, 'Low']

        curr_trail_long = high - atr * atr_mult
        curr_trail_short = low + atr * atr_mult

        if i == entry_index:
            trail_stop_long[i] = curr_trail_long
            trail_stop_short[i] = curr_trail_short
        else:
            trail_stop_long[i] = max(trail_stop_long[i - 1], curr_trail_long)
            trail_stop_short[i] = min(trail_stop_short[i - 1], curr_trail_short)

    self.df['TrailStopLong'] = trail_stop_long
    self.df['TrailStopShort'] = trail_stop_short


