from objects.action import Action
from objects.order_book import OrderBook
from objects.portfolio import Portfolio
from objects.trade import Trade
from objects.trader import SpreadTrader

from __init__ import *

class UnwindManager:
    """Gradually unwinds open positions before market close."""
    
    def __init__(self, trader, portfolio, start_time, end_time):
        self.trader = trader
        self.portfolio = portfolio
        self.start_time = start_time
        self.end_time = end_time
        self.unwinding = False

    def check_unwind(self, current_time):
        """Decides when to start unwinding and executes liquidation trades."""
        if self.unwinding or current_time < self.start_time:
            return

        self.unwinding = True
        print("\n⚠️ Market close approaching, starting gradual unwinding.")

    def execute_unwind_trade(self):
        """Executes unwind trades at the best available liquidity."""
        ob = self.trader.order_books

        # Unwind SPOT → sell excess CNY
        if self.portfolio.cny_balance > 0:
            best_bid = ob["spot"].get_best_bid_ask()[0]  # Best bid (to sell)
            if best_bid:
                self.trader.execute_trade("spot", "perp", best_bid, best_bid)  # Sell CNY

        # Unwind ITRF → sell excess ITRF
        if self.portfolio.itrf_balance > 0:
            best_bid = ob["itrf"].get_best_bid_ask()[0]
            if best_bid:
                self.trader.execute_trade("itrf", "spot", best_bid, best_bid)

        # Unwind PERP → sell excess PERP
        if self.portfolio.perp_balance > 0:
            best_bid = ob["perp"].get_best_bid_ask()[0]
            if best_bid:
                self.trader.execute_trade("perp", "spot", best_bid, best_bid)

        # If nothing left to unwind, stop
        if self.portfolio.cny_balance == 0 and self.portfolio.itrf_balance == 0 and self.portfolio.perp_balance == 0:
            print("\n✅ All positions successfully unwound.")
            self.unwinding = False
        print(self.trader.trades[-1])
