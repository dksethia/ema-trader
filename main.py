import json
import time
import argparse
import pandas as pd
import pandas_ta as ta

from bitget.consts import CONTRACT_WS_URL
from bitget.ws.bitget_ws_client import BitgetWsClient, SubscribeReq
import bitget.mix.order_api as order

from config import API_KEY, SECRET_KEY, PASSPHRASE


def handle_candles(message):
    # start = time.perf_counter()

    # Convert message from string to dict
    data = json.loads(message)["data"]

    # Create dataframe from message data
    columns = ["Timestamp", "O", "H", "L", "C", "Vol"]
    df = pd.DataFrame(data=data, columns=columns)

    # Chenge dataframe index to datetime value using the 'Timestamp' column
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")
    df.set_index("Timestamp", inplace=True)

    # Change data type from string to float64
    df = df.apply(pd.to_numeric)

    # end = time.perf_counter()
    # print(f"{end - start} seconds elapsed")

    return df


def handle_position(message):
    print(f"positions: {message}")


def handle_error(message):
    print(f"Error: {message}")


if __name__ == "__main__":
    # Parse command line arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--symbol', default="BTCUSDT", help="ticker for asset to trade with")
    arg_parser.add_argument('--timeframe', default="1m", help="time frame to trade on (1m, 5m, 15m, 30m, 1H, 4H, 12H, 1D, 1W) ")
    arg_parser.add_argument('--ema1', default=10, type=int, help="Period of first EMA")
    arg_parser.add_argument('--ema2', default=50, type=int, help="Period of second EMA")
    arg_parser.add_argument('--ema3', default=100, type=int, help="Period of third EMA")
    arg_parser.add_argument('--ema4', default=200, type=int, help="Period of fourth EMA")
    arg_parser.add_argument('--tenkan', default=9, type=int, help="Period of Tenkan-sen (Conversion Line)")
    arg_parser.add_argument('--kijun', default=26, type=int, help="Period of Kijun-sen (Base Line)")
    arg_parser.add_argument('--senkou', default=52, type=int, help="Period of Senkou Span B")
    args = arg_parser.parse_args()

    # symbol = args.symbol
    symbol = "SBTCSUSDT"
    timeframe = f"candle{args.timeframe}"
    # order_symbol = f"{symbol}_UMCBL"
    order_symbol = "SBTCSUSDT_SUMCBL"
    margin_coin = "SUSDT"
    # order_size = "0.001"
    order_size = "0.1"

    leverage = 20

    # Open Websocket connection
    client = BitgetWsClient(CONTRACT_WS_URL, need_login=True) \
        .api_key(API_KEY) \
        .api_secret_key(SECRET_KEY) \
        .passphrase(PASSPHRASE) \
        .error_listener(handle_error) \
        .build()

    orderApi = order.OrderApi(API_KEY, SECRET_KEY, PASSPHRASE, use_server_time=False, first=False)

    # Subscribe to candle feed
    channels = [SubscribeReq("mc", timeframe, symbol)]
    client.subscribe(channels, handle_candles)

    # Subscribe to positions
    # channels = [SubscribeReq("UMCBL", "positions", "default")]
    # client.subscribe(channels, handle_position)

    open_position = None

    # Trading loop
    while True:
        # time.sleep(1) # For testing

        if client.df is None:
            continue

        # (re)calculate ichimoku and emas
        ema4 = ta.sma(client.df["C"], length=args.ema4)
        ichimoku, forward_spans = ta.ichimoku(client.df["H"], client.df["L"], client.df["C"], include_chikou=False, tenkan=args.tenkan, kijun=args.kijun, senkou=args.senkou)

        tenkan = ichimoku.iloc[:, 2]
        kijun = ichimoku.iloc[:, 3]

        # If not already in a position
        if open_position is None:
            """ Strategy:

            1. Check if conversion line has crossed above base line
            2. If true, check if price is above ema
            3. If true, open long and close when price closes below conversion line

            (Opposite for shorts)
            """

            # Check if tenkan has crossed above kijun in the last close
            if (tenkan[-3] <= kijun[-3]) and (tenkan[-2] > kijun[-2]):
                print("bullish cross")
                # Check if last close was above ema
                if (client.df["C"][-2] >= ema4[-2]) and (client.df["C"][-2] >= tenkan[-2]):
                    print("price above ema and tenkan, opening long...")
                    # Set hard stop to 10% loss (arbitrary number)
                    hard_stop = str(round(client.df["C"][-2] * (1 - (0.1 / leverage))))

                    res = orderApi.place_order(order_symbol, margin_coin, order_size, "open_long", "market", presetStopLossPrice=hard_stop)

                    if res.get("msg") == "success":
                        open_position = {
                            "order_symbol": order_symbol,
                            "margin_coin": margin_coin,
                            "position_size": order_size,
                            "side": "long"
                        }
                    else:
                        print("order failed, exiting loop...")
                        break
                else:
                    print("price below ema, no action taken")

            # Check if tenkan has crossed below kijun in the last close
            elif (tenkan[-3] >= kijun[-3]) and (tenkan[-2] < kijun[-2]):
                print("bearish cross")
                # Check if last close was below ema
                if (client.df["C"][-2] <= ema4[-2]) and (client.df["C"][-2] <= tenkan[-2]):
                    print("price below ema and tenkan, opening short...")
                    # Set hard stop to 10% loss (arbitrary number)
                    hard_stop = str(round(client.df["C"][-2] * (1 + (0.1 / leverage))))

                    res = orderApi.place_order(order_symbol, margin_coin, order_size, "open_short", "market", presetStopLossPrice=hard_stop)

                    if res.get("msg") == "success":
                        open_position = {
                            "order_symbol": order_symbol,
                            "margin_coin": margin_coin,
                            "position_size": order_size,
                            "side": "short"
                        }
                    else:
                        print("order failed, exiting loop...")
                        break
                else:
                    print("price above ema, no action taken")

            else:
                pass
                # print("nothing to see here...")

        # If already in a position
        else:
            """ Position management

            1. Open an order with hard stop, check success and set open_position to position details.
            2. Keep tracking price, checking if price closes below conversion line
            3. If true, then send an close position order, check success
            """

            if open_position["side"] == "long":
                if client.df["C"][-2] < tenkan[-2]:
                    print("close: ", client.df["C"][-2], ", tenkan: ", tenkan[-2])
                    res = orderApi.place_order(order_symbol, open_position["margin_coin"], open_position["position_size"], "close_long", "market")

                    if res.get("msg") == "success":
                        print("position closed")
                        open_position = None
                    else:
                        print("ERROR: position was not closed")

            elif open_position["side"] == "short":
                if client.df["C"][-2] > tenkan[-2]:
                    print("close: ", client.df["C"][-2], ", tenkan: ", tenkan[-2])
                    res = orderApi.place_order(order_symbol, open_position["margin_coin"], open_position["position_size"], "close_short", "market")

                    if res.get("msg") == "success":
                        print("position closed")
                        open_position = None
                    else:
                        print("ERROR: position was not closed")

            else:
                print("invalid position details")
