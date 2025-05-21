import logging
import time
from threading import Thread
import pandas as pd
from uuid import uuid4
from sqlalchemy import text, bindparam
import psql
from services import (
    get_profile,
    place_angelone_order,
    get_auth,
    get_historical_data,
    combine_historical_with_live_algo,
    buy_sell_function12  # Make sure this is imported if you use it directly
)
from creds import *
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(
    filename='trading.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def fetch_from_db(query: str, params: Dict[str, Any], error_message: str) -> Dict[str, Any]:
    """Helper function to fetch data from the database."""
    try:
        result = psql.execute_query(query, params=params)
        if not result:
            logging.error(error_message)
            raise ValueError(error_message)
        return result[0]
    except Exception as e:
        logging.error(f"Database query failed: {error_message} - {str(e)}", exc_info=True)
        raise

def place_order(order_params: Dict[str, Any], user_id: int, stock_token: str) -> None:
    """Helper function to place an order."""
    try:
        logging.info(f"Placing order with params: {order_params}")
        result = place_angelone_order(
            smart_api_obj=get_auth(api_key=api_key, username=username, pwd=pwd, token=token),
            order_details=order_params
        )
        logging.info(f"Order placed successfully for user_id={user_id}, stock_token={stock_token}")
        return result
    except Exception as e:
        logging.error(f"Failed to place order for user_id={user_id}, stock_token={stock_token} - {str(e)}", exc_info=True)
        return None

def trade_function(row: Dict[str, Any]) -> None:
    """Processes a single trade based on the provided row data."""
    try:
        logging.info(f"Entering trade_function with row: {row}")

        # Extracting values from row
        quantity: int = row['quantity']
        stock_token: str = row['stock_token']
        trade_count: int = row['trade_count']
        user_id: int = row['user_id']
        strategy_id: str = row['strategy_id']
        user_strategy_id: int = row['id']

        logging.info(f"Processing trade for user_id={user_id}, strategy_id={strategy_id}, stock_token={stock_token}")
        stock_details: Dict[str, Any] = fetch_from_db(
            "SELECT * FROM stock_details WHERE token = :stock_token",
            {'stock_token': str(stock_token)},
            f"Stock details not found for stock_token: {stock_token}"
        )

        order_manager_uuid = str(uuid4())

        # Insert into order_manager 
        psql.execute_query(
            raw_sql="""
            INSERT INTO order_manager (
                order_id, completed_order_count, buy_count, sell_count, is_active, created_at, updated_at, user_active_strategy_id
            ) VALUES (
                :order_id, :completed_order_count, :buy_count, :sell_count, :is_active, :created_at, :updated_at, :user_active_strategy_id
            )
            ON CONFLICT (order_id) DO NOTHING;
            """,
            params={
                "order_id": order_manager_uuid,
                "completed_order_count": 0,
                "buy_count": 0,
                "sell_count": 0,
                "is_active": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "user_active_strategy_id": user_strategy_id,
            }
        )
        logging.info(f"Order created with order_id={order_manager_uuid} for strategy_id={strategy_id}")

        # --- Initialize historical data ---
        smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
        today = datetime.now()
        fromdate = (today - timedelta(days=10)).strftime("%Y-%m-%d %H:%M")
        todate = today.strftime("%Y-%m-%d %H:%M")
        historical_df = get_historical_data(
            smart_api_obj=smart_api_obj,
            exchange="NSE",
            symboltoken=stock_token,
            interval="FIVE_MINUTE",
            fromdate=fromdate,
            todate=todate
        )
        if historical_df is None or historical_df.empty:
            logging.error("No historical data found, aborting trade_function.")
            return

        ovr_data = historical_df.copy()

        def is_five_minute_window() -> bool:
            now: datetime = datetime.now()
            return now.minute % 5 == 0 and now.second == 1

        while trade_count > 0:
            # Wait for the next 5-minute window
            while not is_five_minute_window():
                time.sleep(0.5)

            logging.info(f"Entering while loop with trade_count: {trade_count}")

            # Combine historical with latest LTP and recalculate signals
            ovr_data = combine_historical_with_live_algo(historical_df=ovr_data, token=stock_token)
            final_row: Dict[str, Any] = ovr_data.tail(1).to_dict(orient='records')[0]
            with open("signals123.txt", "a") as f:
                f.write(f"final_row: Dict[str, Any] signals = {final_row}\n")

            if final_row.get('buy') == 1:
                if True:
                # if current_position is None:
                    trade_count -= 1
                    # current_position = "buy"
                    order_params: Dict[str, Any] = {
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
                    angelone_response = place_order(order_params, user_id, stock_token)
                    with open("signals.txt", "a") as f:
                        f.write(f"final_row: Dict[str, Any] buy = {final_row}\n {angelone_response}\n")
                    psql.execute_query(
                        raw_sql="""
                        insert into 
                            equity_trade_history (order_id, stock_token, trade_type, quantity, price, entry_ltp, exit_ltp, total_price, trade_entry_time, trade_exit_time
                            )
                            values
                            (:order_id, :stock_token, :trade_type, :quantity, :price, :entry_ltp, :exit_ltp, :total_price, :trade_entry_time, :trade_exit_time)
                        """,
                        params={
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "BUY",
                            "quantity": quantity,
                            "price": 0,
                            "entry_ltp": final_row['close'],
                            "exit_ltp": 0,
                            "total_price": 0,
                            "trade_entry_time": datetime.now(),
                            "trade_exit_time": None
                        }
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE order_manager
                        SET buy_count = buy_count + 1
                        WHERE order_id = :order_id;
                        """,
                        params={
                            "order_id": order_manager_uuid
                        }
                    )
                else:
                    logging.info(f"Cannot place buy order. Current position: ")

            elif final_row.get('sell') == 1:
                # if current_position is None:
                if True:
                    trade_count -= 1
                    # current_position = "sell"
                    order_params: Dict[str, Any] = {
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
                    angelone_response = place_order(order_params, user_id, stock_token)
                    with open("signals.txt", "a") as f:
                        f.write(f"final_row: Dict[str, Any] sell = {final_row}\n {angelone_response}\n")
                    psql.execute_query(
                        raw_sql="""
                        insert into 
                            equity_trade_history (order_id, stock_token, trade_type, quantity, price, entry_ltp, exit_ltp, total_price, trade_entry_time, trade_exit_time
                            )
                            values
                            (:order_id, :stock_token, :trade_type, :quantity, :price, :entry_ltp, :exit_ltp, :total_price, :trade_entry_time, :trade_exit_time)
                        """,
                        params={
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "SELL",
                            "quantity": quantity,
                            "price": 0,
                            "entry_ltp": final_row['close'],
                            "exit_ltp": 0,
                            "total_price": 0,
                            "trade_entry_time": datetime.now(),
                            "trade_exit_time": None
                        }
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE order_manager
                        SET sell_count = sell_count + 1
                        WHERE order_id = :order_id;
                        """,
                        params={
                            "order_id": order_manager_uuid
                        }
                    )
                else:
                    logging.info(f"Cannot place sell order. Current position: ")

            elif final_row.get('buy_exit') == 1:
                if True:
                # if current_position == "buy":
                    psql.execute_query("""
                    UPDATE equity_trade_history
                    SET exit_ltp = :exit_ltp, 
                    trade_exit_time = :trade_exit_time
                    WHERE order_id = :order_id;
                    """, params={
                        "exit_ltp": final_row['close'],
                        "trade_exit_time": datetime.now(),
                        "order_id": order_manager_uuid
                    })
                    # current_position = None
                    logging.info(f"Buy exit executed for stock_token={stock_token}")
                else:
                    logging.info(f"Cannot execute buy exit. Current position: ")

            elif final_row.get('sell_exit') == 1:
                if True:
                # if current_position == "sell":
                    psql.execute_query("""
                    UPDATE equity_trade_history
                    SET exit_ltp = :exit_ltp, 
                    trade_exit_time = :trade_exit_time
                    WHERE order_id = :order_id;
                    """, params={
                        "exit_ltp": final_row['close'],
                        "trade_exit_time": datetime.now(),
                        "order_id": order_manager_uuid
                    })
                    # current_position = None
                    logging.info(f"Sell exit executed for stock_token={stock_token}")
                else:
                    logging.info(f"Cannot execute sell exit. Current position: {current_position}")

            # Sleep to avoid double execution in the same minute
            time.sleep(2)

    except Exception as e:
        logging.error(f"Error processing trade for user_id={row.get('user_id')} - {str(e)}", exc_info=True)

def main() -> None:
    """Main function to start the trading process."""
    try:
        data: List[Dict[str, Any]] = psql.execute_query(text("SELECT * FROM user_active_strategy WHERE is_started = false"))
        if not data:
            logging.info("No new strategies to start.")
            return

        ids_to_update: List[int] = [row['id'] for row in data]
        logging.info(f"Updated is_started=true for IDs: {ids_to_update}")
        threads = []
        for row in data:
            sql = text("UPDATE user_active_strategy SET is_started = true, status='active' WHERE id = :id")
            psql.execute_query(sql, params={"id": row['id']})
            # trade_function(row)
            t = Thread(target=trade_function, args=(row,))
            t.start()
            threads.append(t)
        logging.info(f"total threads {len(threads)}", exc_info=True)
    except Exception as e:
        logging.error("Error in main function", exc_info=True)

if __name__ == "__main__":
    logging.info("Starting trading process...")
    main()