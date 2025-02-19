from objects.action import Action
from objects.order_book import OrderBook
from objects.portfolio import Portfolio
from objects.trade import Trade

from __init__ import *

from datetime import time

class SpreadTrader:
    """Executes taker spread trades using Order Book Imbalance (OBI), with specific thresholds per pair."""

    def __init__(self, order_books, portfolio, obi_thresholds=None):
        self.order_books = order_books
        self.portfolio = portfolio
        self.trades = []
        
        self.obi_thresholds = obi_thresholds or {
            "spot_perp": 0.1,
            "spot_itrf": 0.1,
            "perp_itrf": 0.1
        }

    def get_obi(self, instrument):
        """Calculates Order Book Imbalance for an instrument using first 10 levels."""
        ob = self.order_books[instrument]
        total_bid_vol = sum(ob.bids.values())
        total_ask_vol = sum(ob.asks.values())
        
        if total_bid_vol + total_ask_vol == 0:
            return None  # Avoid division by zero
        
        return (total_bid_vol - total_ask_vol) / (total_bid_vol + total_ask_vol)

    def find_trade_opportunity(self):
        """Checks for trading opportunities based on OBI per pair."""
        spot_ob, itrf_ob, perp_ob = self.order_books["spot"], self.order_books["itrf"], self.order_books["perp"]
        spot_bid, spot_ask = spot_ob.get_best_bid_ask()
        itrf_bid, itrf_ask = itrf_ob.get_best_bid_ask()
        perp_bid, perp_ask = perp_ob.get_best_bid_ask()

        obi_spot = self.get_obi("spot")
        obi_perp = self.get_obi("perp")
        obi_itrf = self.get_obi("itrf")

        flag = False

        if obi_spot and obi_perp and obi_itrf:
            if obi_spot > self.obi_thresholds["spot_perp"] and obi_perp < -self.obi_thresholds["spot_perp"]:
                self.execute_trade("spot", "perp", spot_ask, perp_bid)
                flag = True
            elif obi_perp > self.obi_thresholds["spot_perp"] and obi_spot < -self.obi_thresholds["spot_perp"]:
                self.execute_trade("perp", "spot", perp_ask, spot_bid)
                flag = True

            if obi_spot > self.obi_thresholds["spot_itrf"] and obi_itrf < -self.obi_thresholds["spot_itrf"]:
                self.execute_trade("spot", "itrf", spot_ask, itrf_bid)
                flag = True
            elif obi_itrf > self.obi_thresholds["spot_itrf"] and obi_spot < -self.obi_thresholds["spot_itrf"]:
                self.execute_trade("itrf", "spot", itrf_ask, spot_bid)
                flag = True

            if obi_perp > self.obi_thresholds["perp_itrf"] and obi_itrf < -self.obi_thresholds["perp_itrf"]:
                self.execute_trade("perp", "itrf", perp_ask, itrf_bid)
                flag = True
            elif obi_itrf > self.obi_thresholds["perp_itrf"] and obi_perp < -self.obi_thresholds["perp_itrf"]:
                self.execute_trade("itrf", "perp", itrf_ask, perp_bid)
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
    
        trade.size = safe_trade_size
        trade.apply(self.portfolio)
    
        self.order_books[buy_market].update_liquidity("ask", buy_price, safe_trade_size)
        self.order_books[sell_market].update_liquidity("bid", sell_price, safe_trade_size)
    
        self.trades.append(trade)
        self.portfolio.last_update_ts_dt = trade.ts_dt

    def unwind(self, cny_initial=10_000_000):
        """Unwinds open positions based on OBI, ensuring minimal market impact."""
        # print("🔄 Unwinding positions...")

        spot_ob, itrf_ob, perp_ob = self.order_books["spot"], self.order_books["itrf"], self.order_books["perp"]
        spot_bid, spot_ask = spot_ob.get_best_bid_ask()
        itrf_bid, itrf_ask = itrf_ob.get_best_bid_ask()
        perp_bid, perp_ask = perp_ob.get_best_bid_ask()

        open_positions = {
            'spot': self.portfolio.cny_balance - cny_initial,
            'perp': self.portfolio.perp_balance,
            'itrf': self.portfolio.itrf_balance
        }

        obi_spot = self.get_obi("spot")
        obi_perp = self.get_obi("perp")
        obi_itrf = self.get_obi("itrf")

        flag = False

        if obi_spot and obi_perp and obi_itrf:
            if open_positions['spot'] < 0 and open_positions['perp'] > 0 and obi_perp > self.obi_thresholds["spot_perp"]:
                self.execute_trade("spot", "perp", spot_ask, perp_bid)
                flag = True
            elif open_positions['spot'] > 0 and open_positions['perp'] < 0 and obi_spot > self.obi_thresholds["spot_perp"]:
                self.execute_trade("perp", "spot", perp_ask, spot_bid)
                flag = True

            if open_positions['spot'] < 0 and open_positions['itrf'] > 0 and obi_itrf > self.obi_thresholds["spot_itrf"]:
                self.execute_trade("spot", "itrf", spot_ask, itrf_bid)
                flag = True
            elif open_positions['spot'] > 0 and open_positions['itrf'] < 0 and obi_spot > self.obi_thresholds["spot_itrf"]:
                self.execute_trade("itrf", "spot", itrf_ask, spot_bid)
                flag = True

            if open_positions['perp'] < 0 and open_positions['itrf'] > 0 and obi_itrf > self.obi_thresholds["perp_itrf"]:
                self.execute_trade("perp", "itrf", perp_ask, itrf_bid)
                flag = True
            elif open_positions['perp'] > 0 and open_positions['itrf'] < 0 and obi_perp > self.obi_thresholds["perp_itrf"]:
                self.execute_trade("itrf", "perp", itrf_ask, perp_bid)
                flag = True

        #print("✅ Unwinding attempt completed.")
        return flag
