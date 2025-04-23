import logging
from threading import Thread
from sqlalchemy import text, bindparam
import psql
from services import get_profile, place_angelone_order,get_auth,get_historical_data,combine_historical_with_live_algo
from creds import *

# Configure logging
logging.basicConfig(
    filename='trading.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def trade_function(row):
    try:
        quantity = row['quantity']  # number of stocks to buy
        stock_token = row['stock_token'] # stock token from the database
        trade_count = row['trade_count'] # number of trades to be executed
        user_id = row['user_id'] # user ID from the database
        strategy_id = row['strategy_id'] # strategy uuid from the database
        
        logging.info(f"Processing trade for user_id={user_id}, strategy_id={strategy_id}, stock_token={stock_token}")
        
        user_details = psql.execute_query(
            'select * from "user" where id = :user_id',
            params={'user_id': user_id}
        )[0]
        
        strategy_details = psql.execute_query(
            'select * from strategy where uuid = :strategy_id',
            params={'strategy_id': strategy_id}
        )[0]
     
        stock_details = psql.execute_query(
            "select * from stock_details where token = :stock_token",
            params={'stock_token': str(stock_token)}
        )[0]
        
        if not stock_details or 'stock_name' not in stock_details:
            raise ValueError(f"Stock details not found or incomplete for token: {stock_token}")

        ovr_data=get_historical_data(
            smart_api_obj=get_auth(api_key=api_key,username=username,pwd=pwd,token=token),
            exchange='NSE',
            symboltoken=stock_token, 
            interval='ONE_MINUTE', 
            fromdate='2025-04-21 09:00',
            todate='2025-04-21 09:00',)
        
        while True:
            if trade_count<=0:
                break
            ovr_data=combine_historical_with_live_algo(historical_df=ovr_data,token=stock_token)
            
            final_row=ovr_data.tail(1).to_dict(orient='records')[0]
            # print(final_row)
            
            if final_row['buy'] == 1:
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
            if final_row['sell'] == 1:
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
                place_angelone_order(smart_api_obj=get_auth(api_key=api_key,username=username,pwd=pwd,token=token), order_details=order_params)
                logging.info(f"Order placed for user_id={user_id}, stock_token={stock_token}")
            
            if final_row['buy_exit']:
                'logic to exit buy position'
            if final_row['sell_exit']:
                'logic to exit sell position'

            

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
            # break
            # thread = Thread(target=trade_function, args=(row,))
            # thread.start()

    except Exception as e:
        logging.error("Error in main function", exc_info=True)

# if __name__ == "__main__":
print("Starting trading process...")
main()
