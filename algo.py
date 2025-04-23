def get_latest_ltp_from_db(token: str):
    sql = """
    SELECT last_update, ltp
    FROM stock_details
    WHERE token = :token
    LIMIT 1
    """
    try:
        session = SessionLocal()
        result = session.execute(text(sql), {"token": token})
        row = result.fetchone()
        session.close()
        if row:
            return {"timestamp": row[0], "close": row[1]}
        else:
            return None
    except SQLAlchemyError as e:
        print(f"DB Error: {e}")
        return None

def combine_historical_with_live(historical_df: pd.DataFrame, token: str):
    latest = get_latest_ltp_from_db(token)
    if latest:
        # Create 1-row DataFrame in the same structure
        new_row = pd.DataFrame([{
            "timestamp": pd.to_datetime(latest["timestamp"]),
            "open": np.nan,
            "high": np.nan,
            "low": np.nan,
            "close": float(latest["close"]),
            "volume": np.nan
        }])

        # Combine with historical
        combined_df = pd.concat([historical_df, new_row], ignore_index=True)
        combined_df.sort_values("timestamp", inplace=True)

        # Recalculate EMAs
        combined_df['short'] = combined_df['close'].ewm(span=5, adjust=False).mean()
        combined_df['middle'] = combined_df['close'].ewm(span=21, adjust=False).mean()
        combined_df['long'] = combined_df['close'].ewm(span=63, adjust=False).mean()

        return combined_df
    else:
        print("No latest price found.")
        return historical_df
