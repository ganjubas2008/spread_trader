import sys
import os
from datetime import time
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from objects.action import Action
from objects.order_book import OrderBook
from objects.portfolio import Portfolio
from objects.trader import SpreadTrader
from objects.action_stream import merge_sorted_actions
from __init__ import *


# --- CONFIGURATION ---
CNY_INITIAL = 10_000_000
UNWIND_TIME = time(11, 0)
PRINT_INTERVAL = 5_000

# --- INITIALIZATION ---
portfolio = Portfolio(initial_cny=CNY_INITIAL, initial_rub=0)
portfolio.last_update_ts_dt = None

order_books = {inst: OrderBook(None, inst) for inst in ["spot", "itrf", "perp"]}
for ob in order_books.values():
    ob.asks, ob.bids = {}, {}

trader = SpreadTrader(order_books, portfolio)

paths = {
    "spot": "data/preprocessed_data/actions/spot_actions.parquet",
    "perp": "data/preprocessed_data/actions/perp_actions.parquet",
    "itrf": "data/preprocessed_data/actions/itrf_actions.parquet",
}

# --- HELPER FUNCTIONS ---
def print_portfolio_summary(trade_count, timestamp):
    """Prints the current portfolio state and key metrics."""
    print("\n" + "=" * 60)
    print(f"  Trades Executed: {trade_count:,} | Timestamp: {timestamp}")
    print("-" * 60)
    print(f"  {'Asset':<10} | {'Balance':>15}")
    print("-" * 60)
    print(f"  {'CNY':<10} | {portfolio.cny_balance:>15,.2f}")
    print(f"  {'RUB':<10} | {portfolio.rub_balance:>15,.2f}")
    print(f"  {'PERP':<10} | {portfolio.perp_balance:>15,.2f}")
    print(f"  {'ITRF':<10} | {portfolio.itrf_balance:>15,.2f}")
    print("-" * 60)
    print(f"  Approx. PnL (No Liquidity Constraints): {portfolio.approximate_pnl(order_books, CNY_INITIAL):>15,.2f} RUB")
    print(f"  Sharpe Ratio: {portfolio.calculate_sharpe():>15.4f}")
    print(f"  Max Drawdown: {portfolio.calculate_max_drawdown():>15.2%}")
    print("=" * 60 + "\n")


# --- TRADING LOOP ---
trade_count = 0
previous_timestamp = None

for action in tqdm(merge_sorted_actions(paths)):
    action = Action(*action)
    action.apply_ob(order_books)

    for inst in order_books:
        order_books[inst].ts_dt = action.ts_dt

    if portfolio.last_update_ts_dt in [None, 0]:
        portfolio.last_update_ts_dt = action.ts_dt

    if previous_timestamp is None or action.ts_dt != previous_timestamp:
        if action.ts_dt.time() >= UNWIND_TIME:
            trader.unwind(cny_initial=CNY_INITIAL)
        else:
            trade_count += trader.find_trade_opportunity()

        previous_timestamp = action.ts_dt

    if trade_count % PRINT_INTERVAL == 0 and trade_count > 0:
        print_portfolio_summary(trade_count, previous_timestamp)

# --- FINAL SUMMARY ---
print("\nFINAL PORTFOLIO STATE:")
print_portfolio_summary(trade_count, previous_timestamp)
