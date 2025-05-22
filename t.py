import time
from datetime import datetime, timedelta
import psql  # assuming this is your custom DB interface

def get_latest_ltp(stock_token: str):
    """
    Fetch the latest LTP and timestamp for the given stock_token from the database.
    Returns (timestamp, ltp)
    """
    try:
        query = """
            SELECT last_update, ltp
            FROM stock_details
            WHERE token = :stock_token
            ORDER BY last_update DESC
            LIMIT 1
        """
        params = {"stock_token": stock_token}
        result = psql.execute_query(query, params=params)
        if not result:
            raise ValueError(f"No LTP found for stock_token: {stock_token}")
        row = result[0]
        return row['last_update'], row['ltp']
    except Exception as e:
        raise

def candles_builder(token: str, interval: int):
    """
    Builds OHLC candle for the given token over the specified interval (in seconds),
    using Python's current time instead of database timestamps.
    """
    ltps = []
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=interval)

    while datetime.now() < end_time:
        try:
            _, ltp_price = get_latest_ltp(token)  # Ignore DB timestamp
            ltps.append(ltp_price)
            time.sleep(1)  # Fetch every second (you can adjust this)
        except Exception as e:
            print(f"Error fetching LTP: {e}")
            time.sleep(1)
            continue

    open_ = ltps[0] if ltps else None
    high = max(ltps) if ltps else None
    low = min(ltps) if ltps else None
    close = ltps[-1] if ltps else None

    return open_, high, low, close, end_time

if __name__ == "__main__":
    stock_token = "10"
    interval = 60  # 1 minute
    open_, high, low, close, end_time = candles_builder(stock_token, interval)
    print(f"Open: {open_}, High: {high}, Low: {low}, Close: {close}, End Time: {end_time}")
