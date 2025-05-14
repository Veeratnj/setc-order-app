import logging
from threading import Thread
from sqlalchemy import text, bindparam
import psql
from services import get_profile, place_angelone_order,get_auth,get_historical_data,combine_historical_with_live_algo
from creds import *
from datetime import datetime, time, timedelta

# Configure logging
logging.basicConfig(
    filename='trading.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def trade_function(row):
    try:
        logging.info(f"Entering trade_function with row: {row}")
        
        # Extracting values from row
        logging.info(f"Extracting values from row")
        quantity = row['quantity']  # number of stocks to buy
        logging.info(f"Extracted quantity: {quantity}")
        stock_token = row['stock_token']  # stock token from the database
        logging.info(f"Extracted stock_token: {stock_token}")
        trade_count = row['trade_count']  # number of trades to be executed
        logging.info(f"Extracted trade_count: {trade_count}")
        user_id = row['user_id']  # user ID from the database
        logging.info(f"Extracted user_id: {user_id}")
        strategy_id = row['strategy_id']  # strategy uuid from the database
        logging.info(f"Extracted strategy_id: {strategy_id}")
        
        logging.info(f"Processing trade for user_id={user_id}, strategy_id={strategy_id}, stock_token={stock_token}")
        
        # Fetching user details
        logging.info(f"Fetching user details for user_id: {user_id}")
        user_details = psql.execute_query(
            'select * from "user" where id = :user_id',
            params={'user_id': user_id}
        )
        logging.info(f"Fetched user details: {user_details}")
        if not user_details:
            logging.error(f"User details not found for user_id: {user_id}")
            raise ValueError(f"User details not found for user_id: {user_id}")
        user_details = user_details[0]
        
        # Fetching strategy details
        logging.info(f"Fetching strategy details for strategy_id: {strategy_id}")
        strategy_details = psql.execute_query(
            'select * from strategy where uuid = :strategy_id',
            params={'strategy_id': strategy_id}
        )
        logging.info(f"Fetched strategy details: {strategy_details}")
        if not strategy_details:
            logging.error(f"Strategy details not found for strategy_id: {strategy_id}")
            raise ValueError(f"Strategy details not found for strategy_id: {strategy_id}")
        strategy_details = strategy_details[0]
        
        # Fetching stock details
        logging.info(f"Fetching stock details for stock_token: {stock_token}")
        stock_details = psql.execute_query(
            "select * from stock_details where token = :stock_token",
            params={'stock_token': str(stock_token)}
        )
        logging.info(f"Fetched stock details: {stock_details}")
        # if not stock_details or 'stock_name' not in stock_details:
        #     logging.error(f"Stock details not found or incomplete for token: {stock_token}")
        #     raise ValueError(f"Stock details not found or incomplete for token: {stock_token}")
        stock_details = stock_details[0]
        
        # Fetching historical data
        logging.info(f"Fetching historical data for stock_token: {stock_token}")
        now = datetime.now()
        fromdate = (now - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)

        ovr_data = get_historical_data(
            smart_api_obj=get_auth(api_key=api_key, username=username, pwd=pwd, token=token),
            exchange='NSE',
            symboltoken=stock_token, 
            interval='FIVE_MINUTE', 
            # fromdate='2025-04-21 09:00',
            fromdate=fromdate.strftime('%Y-%m-%d %H:%M'),
            # todate='2025-04-21 09:00',
            todate= now.strftime('%Y-%m-%d %H:%M'),
        )
        logging.info(f"Fetched historical data: {ovr_data}")

        def is_five_minute_window():
            now = datetime.now()
            return now.minute % 5 == 0 and now.second == 1

        while True:
            if not is_five_minute_window():
                # logging.info(f"Waiting for the next 5-minute window")
                # time.sleep(1)
                continue
            logging.info(f"Entering while loop with trade_count: {trade_count}")
            if trade_count <= 0:
                logging.info(f"Exiting while loop as trade_count is less than or equal to 0")
                break
            ovr_data = combine_historical_with_live_algo(historical_df=ovr_data, token=stock_token)
            # logging.info(f"Combined historical with live algo: {ovr_data}")
            
            final_row = ovr_data.tail(1).to_dict(orient='records')[0]
            logging.info(f"Final row: {final_row}")
            
            if final_row['buy'] == 1:
                logging.info(f"Buy signal received")
                trade_count -= 1
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": stock_details['stock_name'],
                    "symboltoken": stock_token,
                    "transactiontype": "BUY",
                    "exchange": "NSE",
                    "ordertype": "MARKET",
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "price": "0",
                    "squareoff": "0",
                    "stoploss": "0",
                    "quantity": quantity
                }
                logging.info(f"Order params for buy: {order_params}")
                place_angelone_order(smart_api_obj=get_auth(api_key=api_key, username=username, pwd=pwd, token=token), order_details=order_params)
                logging.info(f"Order placed for user_id={user_id}, stock_token={stock_token}")
            elif final_row['sell'] == 1:
                logging.info(f"Sell signal received")
                trade_count -= 1
                order_params = {
                    "variety": "NORMAL",
                    "tradingsymbol": stock_details['stock_name'],
                    "symboltoken": stock_token,
                    "transactiontype": "SELL",
                    "exchange": "NSE",
                    "ordertype": "MARKET",
                    "producttype": "INTRADAY",
                    "duration": "DAY",
                    "price": "0",
                    "squareoff": "0",
                    "stoploss": "0",
                    "quantity": quantity
                }
                logging.info(f"Order params for sell: {order_params}")
            
            # logging.info(f"Placing order with order_params: {order_params}")
                place_angelone_order(smart_api_obj=get_auth(api_key=api_key, username=username, pwd=pwd, token=token), order_details=order_params)
                logging.info(f"Order placed for user_id={user_id}, stock_token={stock_token}")
            
            if final_row['buy_exit']:
                logging.info(f"Buy exit signal received")
                # 'logic to exit buy position'
            if final_row['sell_exit']:
                logging.info(f"Sell exit signal received")
                # 'logic to exit sell position'

    except Exception as e:
        logging.error(f"Error processing trade for user_id={row.get('user_id')} - {str(e)}", exc_info=True)


def main():
    try:
        data = psql.execute_query("SELECT * FROM user_active_strategy WHERE is_started = false")
        if not data:
            logging.info("No new strategies to start.")
            return
        
        ids_to_update = [row['id'] for row in data]

        sql = text("UPDATE user_active_strategy SET is_started = true WHERE id IN :ids")
        sql = sql.bindparams(bindparam("ids", expanding=True))

        # Update the DB to avoid re-processing
        # psql.execute_query(sql, params={"ids": ids_to_update})
        logging.info(f"Updated is_started=true for IDs: {ids_to_update}")

        for row in data:
            # print(row)
            trade_function(row)
            break
            # thread = Thread(target=trade_function, args=(row,))
            # thread.start()

    except Exception as e:
        logging.error("Error in main function", exc_info=True)

# if __name__ == "__main__":
print("Starting trading process...")
main()
