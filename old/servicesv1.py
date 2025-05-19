import json
from SmartApi import SmartConnect
import pandas as pd
import pyotp
import ast
from creds import * 
import numpy as np
import psql


def get_auth(api_key, username, pwd, token):
    obj = SmartConnect(api_key=api_key)
    data = obj.generateSession(username, pwd, pyotp.TOTP(token).now())

    AUTH_TOKEN = data['data']['jwtToken']
    FEED_TOKEN = data['data']['feedToken']
    refresh_token = data['data']['refreshToken']

    res = obj.getProfile(refresh_token)
    return obj

def get_profile():
    return {
        'auth_token': AUTH_TOKEN,
        'feed_token': FEED_TOKEN,
        'refresh_token': refresh_token,
        'res': res,
    }


def place_angelone_order(smart_api_obj, order_details):
    """
    Places an order using the Angel One SmartAPI.

    Args:
        smart_api_obj (SmartConnect): The authenticated SmartConnect object.
        order_details (dict): A dictionary containing the order parameters.
                              See example below for required keys.

    Returns:
        dict: The response from the placeOrder API call, or None if an error occurs.
    """
    try:
        print("\nPlacing Order with details:")
        print(json.dumps(order_details, indent=2))
        order_response = smart_api_obj.placeOrder(order_details)

        print("\nOrder Placement Response:")
        if order_response:
            print(json.dumps(order_response, indent=2))
            # if order_response.get("status") == True and order_response.get("data") and order_response["data"].get("orderid"):
                
            #      print(f"--- Order placed successfully! Order ID: {order_response['data']['orderid']} ---")
            # else:
                
            #      print(f"--- Order placement failed or status unknown. Message: {order_response.get('message')} ---")
            #      print(f"--- Error Code: {order_response.get('errorcode')} ---")

        else:
            print("--- Received empty response from placeOrder API. ---")


        return order_response
    except Exception as e:
        print(f"An error occurred while placing the order: {e}")
        # logging.exception("Exception during order placement:") # Log detailed exception
        return None



def get_historical_data(smart_api_obj,exchange,symboltoken,interval,fromdate,todate):
    try:
        historicParam={
        "exchange": exchange,
        "symboltoken":symboltoken,
        "interval": interval,
        "fromdate": fromdate, 
        "todate": todate,
        }
        # historicParam={
        # "exchange": "NSE",
        # "symboltoken": "3045",
        # "interval": "FIVE_MINUTE",
        # "fromdate": "2025-04-21 09:00", 
        # "todate": "2025-04-21 15:16"
        # }
        # print(smart_api_obj.getCandleData(historicParam))
        df = pd.DataFrame(smart_api_obj.getCandleData(historicParam))
        # print(df.head())

        # If 'data' is already a list of lists, no need for ast.literal_eval
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        data_df = pd.DataFrame(df['data'].tolist(), columns=columns)

        # Optional: convert timestamp to datetime
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])

        # (Optional) Convert timestamp to datetime
        data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
        #short/fast
        short_ema=data_df.close.ewm(span=5,adjust=False).mean()
        #middle/medium
        middle_ema=data_df.close.ewm(span=21,adjust=False).mean()
        #long/slow 
        long_ema=data_df.close.ewm(span=63,adjust=False).mean()

        data_df['short']=short_ema
        data_df['middle']=middle_ema
        data_df['long']=long_ema

        data_df['buy'] = np.nan
        data_df['sell'] = np.nan
        data_df['buy_exit'] = np.nan
        data_df['sell_exit'] = np.nan
        data_df.to_csv('historical_data.csv', index=False)
        print(df.head())
        return data_df
    except Exception as e:
        print(f"An error occurred while fetching historical data: {e}")
        return None

def buy_sell_function(data):
    buy_list = []
    sell_list = []
    buy_exit_list = []
    sell_exit_list = []
    flag_long = False  # Track long (buy) position
    flag_short = False  # Track short (sell) position
    buy_enter_condition= True # Track buy enter condition
    sell_enter_condition= True # Track sell enter condition
    buy_exit_condition= True # Track buy exit condition
    sell_exit_condition= True # Track sell exit condition
    print(data)
    print(data.columns)
    print(data['short'].iloc[0])
    print(data['middle'])
    print(data['long'])
    print('ok')
    for i in range(len(data)):
        # print(f"Processing row {i} | Timestamp: {data.index[i]}")
        short_temp = data['short'].iloc[i]
        middle_temp = data['middle'].iloc[i]
        long_temp = data['long'].iloc[i]
        # short_temp = data['short'][i]
        # middle_temp = data['middle'][i]
        # long_temp = data['long'][i]
        # Condition 1: Enter a Short Trade (Sell Entry)
        if not flag_long and not flag_short and middle_temp < long_temp and short_temp < middle_temp:
            sell_list.append(1)  # Short Trade Entry (Sell first)
            buy_list.append(np.nan)
            sell_exit_list.append(np.nan)
            buy_exit_list.append(np.nan)
            flag_short = True
            # print(f"📉 Sell Entry (Short Trade) at {data.index[i]} | Price: {data['Close'][i]}")

        # Condition 2: Exit Short Trade (sell Exit)
        elif flag_short and short_temp > middle_temp:
            buy_exit_list.append(np.nan)  # Short Trade Exit (Buy to close)
            buy_list.append(np.nan)
            sell_list.append(np.nan)
            sell_exit_list.append(1)
            flag_short = False
            # print(f"📈 Buy Exit (Close Short Trade) at {data.index[i]} | Price: {data['Close'][i]}")

        # Condition 3: Enter a Long Trade (Buy Entry)
        elif not flag_long and not flag_short and middle_temp < long_temp and short_temp > middle_temp:
            buy_list.append(1)  # Long Trade Entry (Buy first)
            sell_list.append(np.nan)
            buy_exit_list.append(np.nan)
            sell_exit_list.append(np.nan)
            flag_long = True
            # print(f"📈 Buy Entry (Long Trade) at {data.index[i]} | Price: {data['Close'][i]}")

        # Condition 4: Exit Long Trade (Sell Exit)
        elif flag_long and short_temp < middle_temp:
            sell_exit_list.append(np.nan)  # Long Trade Exit (Sell to close)
            sell_list.append(np.nan)
            buy_list.append(np.nan)
            buy_exit_list.append(1)
            flag_long = False
            # print(f"📉 Sell Exit (Close Long Trade) at {data.index[i]} | Price: {data['Close'][i]}")

        # No Trade
        else:
            buy_list.append(np.nan)
            sell_list.append(np.nan)
            buy_exit_list.append(np.nan)
            sell_exit_list.append(np.nan)

    return buy_list, sell_list, buy_exit_list, sell_exit_list


def get_latest_ltp_from_db(token: str):
    sql = """
    SELECT last_update, ltp
    FROM stock_details
    WHERE token = :token
    LIMIT 1
    """
    try:
        row = psql.execute_query(raw_sql=sql, params={"token": token})
        
        if row:
            return {"timestamp": row[0]['last_update'], "close": row[0]['ltp']}
        else:
            return None
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def combine_historical_with_live_algo(historical_df: pd.DataFrame, token: str):
    latest = get_latest_ltp_from_db(token)
    print("Latest LTP from DB:", latest)


    if latest:
        # Create 1-row DataFrame in the same structure
        new_row = pd.DataFrame([{
            "timestamp": pd.to_datetime(latest["timestamp"]),
            "open": np.nan,
            "high": np.nan,
            "low": np.nan,
            "close": float(latest["close"]),
            "volume": np.nan,
            "buy": np.nan,
            "sell": np.nan,
            "buy_exit": np.nan,
            "sell_exit": np.nan,
        }])

        # Combine with historical
        combined_df = pd.concat([historical_df, new_row], ignore_index=True)
        # combined_df.sort_values("timestamp", inplace=True)
        print("Combined DataFrame:")

        # Recalculate EMAs
        combined_df['short'] = combined_df['close'].ewm(span=5, adjust=False).mean()
        combined_df['middle'] = combined_df['close'].ewm(span=21, adjust=False).mean()
        combined_df['long'] = combined_df['close'].ewm(span=63, adjust=False).mean()
        # print('Combined DataFrame with EMAs:',combined_df.tail(1))
        buy_list, sell_list, buy_exit_list, sell_exit_list = buy_sell_function(combined_df.tail(1))
        print("Buy/Sell signals for the latest row:")
        # combined_df['buy'] = buy_list
        # combined_df['sell'] = sell_list
        # combined_df['buy_exit'] = buy_exit_list
        # combined_df['sell_exit'] = sell_exit_list
        combined_df.loc[combined_df.index[-1], 'buy'] = buy_list[0]
        combined_df.loc[combined_df.index[-1], 'sell'] = sell_list[0]
        combined_df.loc[combined_df.index[-1], 'buy_exit'] = buy_exit_list[0]
        combined_df.loc[combined_df.index[-1], 'sell_exit'] = sell_exit_list[0]
        print("Combined DataFrame with latest LTP:")
        print(combined_df.tail(1))

        return combined_df
    else:
        print("No latest price found.")
        return historical_df





order_params = {
    "variety": "NORMAL", # NORMAL for regular orders. Others: STOPLOSS, AMO, ROBO (Bracket Order)
    "tradingsymbol": "SBIN-EQ", # The trading symbol (e.g., SBIN-EQ for NSE equity)
    "symboltoken": "3045",     # The unique instrument token (Find this via API or Angel platform)
    "transactiontype": "BUY",  # Or "SELL"
    "exchange": "NSE",         # Or "BSE", "NFO", "MCX", "CDS"
    "ordertype": "MARKET",     # Or "LIMIT", "STOPLOSS_LIMIT", "STOPLOSS_MARKET"
    "producttype": "DELIVERY", # Or "INTRADAY", "CARRYFORWARD", "MARGIN"
    "duration": "DAY",         # Or "IOC" (Immediate or Cancel)
    "price": "0",              # Set to "0" for MARKET orders. Enter specific price for LIMIT orders.
    "squareoff": "0",          # Target price for profit booking (mainly for ROBO) - Set to "0" for Normal orders
    "stoploss": "0",           # Stoploss price (mainly for ROBO/Stoploss orders) - Set to "0" for Normal orders
    "quantity": "1"            # The number of shares/lots
    # "triggerprice": "0"      # Required for STOPLOSS_LIMIT and STOPLOSS_MARKET orders
}


if __name__ == "__main__":
    print("Placing order...")
    # Call the function to place the order
    obj = get_auth(api_key, username, pwd, token)
    result_df=get_historical_data(smart_api_obj=obj,exchange="NSE",symboltoken="3045",interval="FIVE_MINUTE",fromdate="2025-04-01 09:00",todate="2025-04-24 15:30")
    # print(result.to_csv('historical_data.csv', index=False))
    # place_angel_order(obj, order_params)

    # if 'obj' in locals() and obj: 
    #     place_angel_order(obj, order_params)
    # else:
    #     print("SmartAPI object not initialized. Cannot place order.")




