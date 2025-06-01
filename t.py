import pandas as pd

# Load your data
# df = pd.read_csv("rel.csv", parse_dates=['timestamp'])



def calculate_to_60_minute(self):
    rolling = self.df.rolling(window=12, min_periods=12)
    self.df['60_min_open']   = rolling['open'].apply(lambda x: x.iloc[0])
    self.df['60_min_high']   = rolling['high'].max()
    self.df['60_min_low']    = rolling['low'].min()
    self.df['60_min_close']  = rolling['close'].apply(lambda x: x.iloc[-1])
    self.df['60_min_volume'] = rolling['volume'].sum()

    delta = self.df['60_min_close'].diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=14, min_periods=14).mean()
    avg_loss = loss.rolling(window=14, min_periods=14).mean()

    rs = avg_gain / avg_loss
    self.df['60_min_rsi_14'] = 100 - (100 / (1 + rs))

