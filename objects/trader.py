from objects.action import Action
from objects.order_book import OrderBook
from objects.portfolio import Portfolio
from objects.trade import Trade

from __init__ import *

from datetime import time

class SpreadTrader:
    """Executes taker spread trades with dynamic spread-based logic and supports unwinding positions."""

    def __init__(self, order_books, portfolio, delta_1=0.002, delta_2=0.001):
        assert delta_1 > 0 and delta_2 > 0
        self.order_books = order_books
        self.portfolio = portfolio
        self.trades = []
        self.delta_1 = delta_1
        self.delta_2 = delta_2

    def find_spread_opportunity(self):
        """Checks for spread opportunities and executes trades."""
        spot_ob, itrf_ob, perp_ob = self.order_books["spot"], self.order_books["itrf"], self.order_books["perp"]
        spot_bid, spot_ask = spot_ob.get_best_bid_ask()
        itrf_bid, itrf_ask = itrf_ob.get_best_bid_ask()
        perp_bid, perp_ask = perp_ob.get_best_bid_ask()

        flag = False

        # Perp-Spot Trading Logic
        spread_perp_spot = perp_bid - spot_ask if perp_bid and spot_ask else None
        if spread_perp_spot is not None:
            if spread_perp_spot > self.delta_1:
                self.execute_trade("spot", "perp", spot_ask, perp_bid)  # Buy Spot, Sell Perp
                flag = True
            elif spread_perp_spot < self.delta_2:
                self.execute_trade("perp", "spot", perp_ask, spot_bid)  # Buy Perp, Sell Spot
                flag = True

        # Itrf-Spot Trading Logic
        spread_itrf_spot = itrf_bid - spot_ask if itrf_bid and spot_ask else None
        if spread_itrf_spot is not None:
            if spread_itrf_spot > self.delta_1:
                self.execute_trade("spot", "itrf", spot_ask, itrf_bid)  # Buy Spot, Sell Itrf
                flag = True
            elif spread_itrf_spot < self.delta_2:
                self.execute_trade("itrf", "spot", itrf_ask, spot_bid)  # Buy Itrf, Sell Spot
                flag = True

        return flag

    def execute_trade(self, buy_market, sell_market, buy_price, sell_price, trade_type="taker"):
        """Executes a spread trade with leverage and commission checks."""
        available_size = min(
            self.order_books[buy_market].asks.get(buy_price, 0),
            self.order_books[sell_market].bids.get(sell_price, 0)
        )
    
        if available_size == 0:
            return
    
        trade = Trade(
            ts_dt=self.order_books[buy_market].ts_dt,
            buy_market=buy_market,
            sell_market=sell_market,
            buy_price=buy_price,
            sell_price=sell_price,
            size=available_size,
            trade_type=trade_type
        )
    
        safe_trade_size = self.portfolio.can_trade(trade)
    
        if safe_trade_size == 0:
            return
    
        if safe_trade_size < available_size:
            print(f"⚠️ Trade partially executed: {safe_trade_size:.2f}/{available_size:.2f}")
    
        trade.size = safe_trade_size
        trade.apply(self.portfolio)
    
        self.order_books[buy_market].update_liquidity("ask", buy_price, safe_trade_size)
        self.order_books[sell_market].update_liquidity("bid", sell_price, safe_trade_size)
    
        self.trades.append(trade)
        
        self.portfolio.last_update_ts_dt = trade.ts_dt
        
        # print(trade)

    def unwind(self, cny_initial = 10_000_000):
        """Checks for spread opportunities and executes trades."""
        spot_ob, itrf_ob, perp_ob = self.order_books["spot"], self.order_books["itrf"], self.order_books["perp"]
        spot_bid, spot_ask = spot_ob.get_best_bid_ask()
        itrf_bid, itrf_ask = itrf_ob.get_best_bid_ask()
        perp_bid, perp_ask = perp_ob.get_best_bid_ask()
        
        
        open_positions = {
            'spot': self.portfolio.cny_balance - cny_initial,
            'perp': self.portfolio.perp_balance,
            'itrf': self.portfolio.itrf_balance
        }
        
        flag = False

        spread_perp_spot = perp_bid - spot_ask if perp_bid and spot_ask else None
        if spread_perp_spot is not None:
            if open_positions['spot'] < 0 and open_positions['perp'] > 0:
                self.execute_trade("spot", "perp", spot_ask, perp_bid)  # Buy Spot, Sell Perp
                flag = True
            elif open_positions['spot'] > 0 and open_positions['perp'] < 0:
                self.execute_trade("perp", "spot", perp_ask, spot_bid)  # Buy Perp, Sell Spot
                flag = True
                
        open_positions = {
            'spot': self.portfolio.cny_balance - cny_initial,
            'perp': self.portfolio.perp_balance,
            'itrf': self.portfolio.itrf_balance
        }

        # Itrf-Spot Trading Logic
        spread_itrf_spot = itrf_bid - spot_ask if itrf_bid and spot_ask else None
        if spread_itrf_spot is not None:
            if open_positions['spot'] < 0 and open_positions['itrf'] > 0:
                self.execute_trade("spot", "itrf", spot_ask, itrf_bid)  # Buy Spot, Sell Itrf
                flag = True
            elif open_positions['spot'] > 0 and open_positions['itrf'] < 0:
                self.execute_trade("itrf", "spot", itrf_ask, spot_bid)  # Buy Itrf, Sell Spot
                flag = True
        
        # Itrf-Perp Trading Logic
        spread_itrf_perp = itrf_bid - perp_ask if itrf_bid and perp_ask else None
        if spread_itrf_perp is not None:
            if open_positions['perp'] < 0 and open_positions['itrf'] > 0:
                self.execute_trade("perp", "itrf", perp_ask, itrf_bid)  # Buy Perp, Sell Itrf
                flag = True
            elif open_positions['perp'] > 0 and open_positions['itrf'] < 0:
                self.execute_trade("itrf", "perp", itrf_ask, perp_bid)  # Buy Itrf, Sell Perp
                flag = True

        return flag


