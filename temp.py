from triple_ema_strategy import TripleEMAStrategyOptimized  # Make sure this import path is correct

class StrategyTrader:
    def __init__(self):
        pass

    def get_latest_ltp(self, stock_token):
        # Implement this function to fetch the latest price for the stock_token
        # For example, from your broker API or a market data service
        # Return (timestamp, ltp)
        raise NotImplementedError("Implement get_latest_ltp to fetch live price.")

    def trade_function(self, row: Dict[str, Any]) -> None:
        try:
            logging.info(f"Starting trade_function for row: {row}")

            quantity = row['quantity']
            stock_token = row['stock_token']
            trade_count = row['trade_count']
            user_id = row['user_id']
            strategy_id = row['strategy_id']
            user_strategy_id = row['id']

            stock_details = fetch_from_db(
                "SELECT * FROM stock_details WHERE token = :stock_token",
                {'stock_token': str(stock_token)},
                f"Stock details not found for stock_token: {stock_token}"
            )

            order_manager_uuid = str(uuid4())
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

            # --- Initialize historical data and strategy ---
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

            strategy = TripleEMAStrategyOptimized()
            strategy.load_historical_data(historical_df)

            def is_five_minute_window():
                now = datetime.now()
                return now.minute % 5 == 0 and now.second < 3

            max_iterations = 100
            iterations = 0

            while trade_count > 0 and iterations < max_iterations:
                while not is_five_minute_window():
                    time.sleep(0.5)
                iterations += 1

                # Fetch latest price (implement this function as per your infra)
                try:
                    ltp_timestamp, ltp_price = self.get_latest_ltp(stock_token)
                except Exception as e:
                    logging.error(f"Failed to fetch latest LTP: {e}")
                    time.sleep(2)
                    continue

                signal = strategy.add_live_price(ltp_timestamp, ltp_price)
                logging.info(f"Signal generated: {signal}")

                if signal == 'BUY_ENTRY':
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
                    angelone_response = place_order(order_params, user_id, stock_token)
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
                            "trade_type": "BUY",
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

                elif signal == 'SELL_ENTRY':
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
                    angelone_response = place_order(order_params, user_id, stock_token)
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
                            "trade_type": "SELL",
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
                        SET sell_count = sell_count + 1
                        WHERE order_id = :order_id;
                        """,
                        params={"order_id": order_manager_uuid}
                    )

                elif signal == 'BUY_EXIT':
                    psql.execute_query(
                        """
                        UPDATE equity_trade_history
                        SET exit_ltp = :exit_ltp, 
                            trade_exit_time = :trade_exit_time
                        WHERE order_id = :order_id AND trade_type = 'BUY' AND exit_ltp = 0;
                        """,
                        params={
                            "exit_ltp": ltp_price,
                            "trade_exit_time": datetime.now(),
                            "order_id": order_manager_uuid
                        }
                    )
                    logging.info(f"Buy exit executed for stock_token={stock_token}")

                elif signal == 'SELL_EXIT':
                    psql.execute_query(
                        """
                        UPDATE equity_trade_history
                        SET exit_ltp = :exit_ltp, 
                            trade_exit_time = :trade_exit_time
                        WHERE order_id = :order_id AND trade_type = 'SELL' AND exit_ltp = 0;
                        """,
                        params={
                            "exit_ltp": ltp_price,
                            "trade_exit_time": datetime.now(),
                            "order_id": order_manager_uuid
                        }
                    )
                    logging.info(f"Sell exit executed for stock_token={stock_token}")

                # Sleep to avoid double execution in the same minute
                time.sleep(2)

            if iterations >= max_iterations:
                logging.warning("Max iterations reached, exiting trade_function.")

        except Exception as e:
            logging.error(f"Error processing trade for user_id={row.get('user_id')} - {str(e)}", exc_info=True)

