import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from objects.action import Action
from objects.order_book import OrderBook
from objects.portfolio import Portfolio
from objects.trade import Trade
from objects.trader import SpreadTrader
from objects.action_stream import merge_sorted_actions
from __init__ import *

from datetime import time

cny_initial = 10_000_000
portfolio = Portfolio(initial_cny=cny_initial, initial_rub=0)
portfolio.last_update_ts_dt = None

empty_order_books = {
    "spot": OrderBook(None, "spot"),
    "itrf": OrderBook(None, "itrf"),
    "perp": OrderBook(None, "perp")
}

for ob in empty_order_books.values():
    ob.asks = {}
    ob.bids = {}

trader = SpreadTrader(empty_order_books, portfolio)

c = 0
previous_timestamp = None
flag = 0

paths = {
    'spot': 'data/preprocessed_data/actions/spot_actions.parquet',
    'perp': 'data/preprocessed_data/actions/perp_actions.parquet',
    'itrf': 'data/preprocessed_data/actions/itrf_actions.parquet'
}

for action in tqdm(merge_sorted_actions(paths)):
    for instrument in empty_order_books.keys():
        empty_order_books[instrument].ts_dt = action.ts_dt
    
    if portfolio.last_update_ts_dt == 0 or portfolio.last_update_ts_dt == None:
        portfolio.last_update_ts_dt = action.ts_dt
    
    action = Action(*action)
    action.apply_ob(empty_order_books)

    for instrument in ['spot', 'perp', 'itrf']:
        empty_order_books[instrument].timestamp = action.ts_dt

    if previous_timestamp is None or action.ts_dt != previous_timestamp:
        if action.ts_dt.time() >= time(11, 0):
            flag = 1
            flag2 = trader.unwind(cny_initial=cny_initial)
            # break  # Stop processing further actions
        else:
            flag = trader.find_trade_opportunity()
        c += flag
        previous_timestamp = action.ts_dt
        
        
    if c % 5_000 == 0 and c > 0:
        print('______________________________________________')
        
        print(portfolio)
        print(previous_timestamp)
        print(f'Approximate PnL (No Liquidity Constraints): {portfolio.approximate_pnl(empty_order_books, cny_initial):,.2f} RUB')
        print(f'Sharpe Ratio: {portfolio.calculate_sharpe():.4f}')
        print(f'Max Drawdown: {portfolio.calculate_max_drawdown():.2%}')
        print('______________________________________________')
    
print("\nFinal Portfolio State:")
print(portfolio)
print(f'Trades executed: {c}')
print(f'Final Approximate PnL: {portfolio.approximate_pnl(empty_order_books, cny_initial):,.2f} RUB')
print(f'Final Sharpe Ratio: {portfolio.calculate_sharpe():.4f}')
print(f'Final Max Drawdown: {portfolio.calculate_max_drawdown():.2%}')


