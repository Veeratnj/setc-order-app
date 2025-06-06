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
    buy_sell_function12
)
from creds import *
from datetime import datetime, timedelta
from typing import Dict, Any, List

# from strategies import TripleEMAStrategyOptimized
from fullcode_ import TripleEMAStrategyOptimized
from temp2 import candles_builder

class StrategyTrader:
    def __init__(self):
        self.smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
        pass

    def place_order(self,order_params: Dict[str, Any], user_id: int, stock_token: str) -> None:
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

    def fetch_from_db(self,query: str, params: Dict[str, Any], error_message: str) -> Dict[str, Any]:
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

    def get_latest_ltp(self, stock_token: str):
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
                error_message = f"No LTP found for stock_token: {stock_token}"
                logging.error(error_message)
                raise ValueError(error_message)
            row = result[0]
            # Ensure timestamp is in datetime format
            ltp_timestamp = row['last_update']
            ltp_price = row['ltp']
            return ltp_timestamp, ltp_price
        except Exception as e:
            logging.error(f"Database query failed for LTP: {str(e)}", exc_info=True)
            raise

    
    def is_market_open(self):
        """Returns True if current time is within trading hours (e.g., 9:15 to 15:30). Adjust as needed."""
        now = datetime.now()
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=25, second=0, microsecond=0)
        # return True
        return market_open <= now <= market_close

    def trade_function(self, row: Dict[str, Any]) -> None:
        try:
            logging.info(f"Starting trade_function for row: {row}")

            quantity = row['quantity']
            stock_token = row['stock_token']
            trade_count = row['trade_count']
            user_id = row['user_id']
            strategy_id = row['strategy_id']
            user_strategy_id = row['id']

            stock_details = self.fetch_from_db(
                "SELECT * FROM stock_details WHERE token = :stock_token",
                {'stock_token': str(stock_token)},
                f"Stock details not found for stock_token: {stock_token}"
            )

            order_manager_uuid = str(uuid4())
            print(f"Order manager UUID: {order_manager_uuid}")
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
            print(f"Order manager UUID: {order_manager_uuid}")

            # --- Initialize historical data and strategy ---
            # smart_api_obj = get_auth(api_key=api_key, username=username, pwd=pwd, token=token)
            today = datetime.now()
            fromdate = (today - timedelta(days=10)).strftime("%Y-%m-%d %H:%M")
            todate = today.strftime("%Y-%m-%d %H:%M")
            print(f"Fetching historical data from {fromdate} to {todate}")
            # 5/0
            historical_df = get_historical_data(
                smart_api_obj=self.smart_api_obj,
                exchange="NSE",
                symboltoken=stock_token,
                interval="FIVE_MINUTE",
                # fromdate='2025-05-15 08:11',
                fromdate=fromdate,
                # todate='2025-05-26 08:11'
                todate=todate
            )
            if historical_df is None or historical_df.empty:
                logging.error("No historical data found, aborting trade_function.")
                return

            strategy = TripleEMAStrategyOptimized(token=stock_token,)
            strategy.load_historical_data(historical_df)

            def is_time_window():
                now = datetime.now()
                # Adjust this logic for your timeframe (5min, 1min, 30sec, etc.)
                return now.minute % 1 == 0 and now.second < 3
            open_order=False
            while (trade_count > 0 and self.is_market_open()) or open_order:
                # while not is_time_window() and self.is_market_open():
                #     time.sleep(0.5)
                if not self.is_market_open():
                    logging.info("Market closed. Exiting trade_function.")
                    break

                try:
                    ltp_timestamp, ltp_price = self.get_latest_ltp(stock_token)
                except Exception as e:
                    logging.error(f"Failed to fetch latest LTP: {e}")
                    time.sleep(2)
                    continue

                # signal = strategy.add_live_price(ltp_timestamp, ltp_price)
                open_, high, low, close, end_time = candles_builder(stock_token, )
                strategy.add_live_data(open_=open_,close=close, high=high, low=low, volume=0, timestamp=end_time)
                signal,stop_loss = strategy.generate_signal()
                logging.info(f"Signal generated: {signal}")

                if signal == 'BUY_ENTRY':
                    print('BUY_ENTRY signal received')
                    print({
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "buy",
                            "quantity": quantity,
                            "price": 0,
                            "entry_ltp": ltp_price,
                            "exit_ltp": 0,
                            "total_price": 0,
                            "trade_entry_time": datetime.now(),
                            "trade_exit_time": None
                        })
                    open_order=True
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
                    # angelone_response = self.place_order(order_params, user_id, stock_token)
                    psql.execute_query(
                        raw_sql="""
                        INSERT INTO equity_trade_history (
                            order_id, stock_token, trade_type, quantity, price, entry_ltp, exit_ltp, total_price, trade_entry_time, trade_exit_time
                        ) VALUES (
                            :order_id, :stock_token, :trade_type, :quantity, :price, :entry_ltp, :exit_ltp, :total_price, :trade_entry_time, :trade_exit_time
                        )
                        """,
                        params={
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "buy",
                            "quantity": quantity,
                            "price": 0,
                            "entry_ltp": ltp_price,
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
                        params={"order_id": order_manager_uuid}
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE user_active_strategy
                        SET status = 'active'
                        WHERE id = :id;
                        """,
                        params={"id": row['id']}
                    )

                elif signal == 'SELL_ENTRY':
                    print('SELL_ENTRY signal received')
                    print({
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "sell",
                            "quantity": quantity,
                            "price": quantity*ltp_price,
                            "entry_ltp": ltp_price,
                            "exit_ltp": 0,
                            "total_price": 0,
                            "trade_entry_time": datetime.now(),
                            "trade_exit_time": None
                        })
                    # print((trade_count > 0 and self.is_market_open()) or open_order)
                    # print(trade_count)
                    # print(self.is_market_open())
                    # print(open_order)
                    open_order=True
                    
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
                    # angelone_response = self.place_order(order_params, user_id, stock_token)
                    psql.execute_query(
                        raw_sql="""
                        INSERT INTO equity_trade_history (
                            order_id, stock_token, trade_type, quantity, price, entry_ltp, exit_ltp, total_price, trade_entry_time, trade_exit_time
                        ) VALUES (
                            :order_id, :stock_token, :trade_type, :quantity, :price, :entry_ltp, :exit_ltp, :total_price, :trade_entry_time, :trade_exit_time
                        )
                        """,
                        params={
                            "order_id": order_manager_uuid,
                            "stock_token": stock_token,
                            "trade_type": "sell",
                            "quantity": quantity,
                            "price": quantity*ltp_price,
                            "entry_ltp": ltp_price,
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
                        params={"order_id": order_manager_uuid}
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE user_active_strategy
                        SET status = 'active'
                        WHERE id = :id;
                        """,
                        params={"id": row['id']}
                    )

                elif signal == 'BUY_EXIT':
                    print
                    open_order=False
                    psql.execute_query(
                        """
                        UPDATE equity_trade_history
                        SET exit_ltp = :exit_ltp, 
                            trade_exit_time = :trade_exit_time,
                            total_price = :total_price
                        WHERE order_id = :order_id AND trade_type = 'buy' AND exit_ltp = 0;
                        """,
                        params={
                            "exit_ltp": ltp_price,
                            "trade_exit_time": datetime.now(),
                            "order_id": order_manager_uuid,
                            "total_price": quantity * ltp_price
                        }
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE user_active_strategy
                        SET status = 'close'
                        WHERE id = :id;
                        """,
                        params={"id": row['id']}
                    )
                    logging.info(f"Buy exit executed for stock_token={stock_token}")

                elif signal == 'SELL_EXIT':
                    print('sell exit signal received')
                    open_order=False
                    psql.execute_query(
                        """
                        UPDATE equity_trade_history
                        SET exit_ltp = :exit_ltp, 
                            trade_exit_time = :trade_exit_time,
                            total_price = :total_price
                        WHERE order_id = :order_id AND trade_type = 'sell' AND exit_ltp = 0;
                        """,
                        params={
                            "exit_ltp": ltp_price,
                            "trade_exit_time": datetime.now(),
                            "order_id": order_manager_uuid,
                            "total_price": quantity * ltp_price
                        }
                    )
                    psql.execute_query(
                        raw_sql="""
                        UPDATE user_active_strategy
                        SET status = 'close'
                        WHERE id = :id;
                        """,
                        params={"id": row['id']}
                    )
                    logging.info(f"Sell exit executed for stock_token={stock_token}")

                # Sleep to avoid double execution in the same minute
                time.sleep(2)


            print((trade_count > 0 and self.is_market_open()) or open_order)
            print(trade_count)
            print(self.is_market_open())
            print(open_order)
            if trade_count > 0:
                logging.info("Trading day ended before all trades could be completed.")

        except Exception as e:
            logging.error(f"Error processing trade for user_id={row.get('user_id')} - {str(e)}", exc_info=True)

    def main(self) -> None:
        """Main function to start the trading process (no threading, for testing)."""
        try:
            data: List[Dict[str, Any]] = psql.execute_query(
                text("SELECT * FROM user_active_strategy WHERE is_started = false")
            )
            if not data:
                logging.info("No new strategies to start.")
                return

            ids_to_update: List[int] = [row['id'] for row in data]
            logging.info(f"Updated is_started=true for IDs: {ids_to_update}")
            print(len(data))
            time.sleep(2)

            for row in data:
                sql = text("UPDATE user_active_strategy SET is_started = true WHERE id = :id")
                psql.execute_query(sql, params={"id": row['id']})
                print(f"Updated is_started=true for ID: {row['id']}")
                # self.trade_function(row)
                t = Thread(target=self.trade_function, args=(row,))
                t.start()
                print(f"Starting thread for user_id={row['user_id']}, strategy_id={row['strategy_id']}, token={row['stock_token']}")

        except Exception as e:
            logging.error("Error in run method", exc_info=True)


if __name__ == "__main__":
    trader = StrategyTrader()
    trader.main()