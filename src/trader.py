from src.reading import *
from src.objects import *

class SpreadTrader:
    """Executes taker spread trades with dynamic spread-based logic."""

    def __init__(self, order_books, portfolio, delta_1=0.002, delta_2=0.001):
        """
        - `delta_1`: Upper threshold for divergence (e.g., if spread > delta_1, we trade).
        - `delta_2`: Lower threshold for convergence (e.g., if spread < delta_2, we trade).
        """
        assert delta_1 > 0 and delta_2 > 0, "Both delta_1 and delta_2 must be positive."
        
        self.order_books = order_books
        self.portfolio = portfolio  # Track portfolio
        self.trades = []
        self.delta_1 = delta_1  # Upper bound for divergence
        self.delta_2 = delta_2  # Lower bound for convergence

    def find_spread_opportunity(self):
        """Checks for spread opportunities based on delta_1 and delta_2."""
        spot_ob = self.order_books["spot"]
        itrf_ob = self.order_books["itrf"]
        perp_ob = self.order_books["perp"]

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
        
        # Get available liquidity at given price levels
        available_size = min(
            self.order_books[buy_market].asks.get(buy_price, 0),
            self.order_books[sell_market].bids.get(sell_price, 0)
        )
    
        if available_size == 0:
            # print("❌ Trade blocked: No available liquidity!")
            return
    
        # Create a trade object with max possible size
        trade = Trade(
            timestamp=self.order_books[buy_market].timestamp,
            buy_market=buy_market,
            sell_market=sell_market,
            buy_price=buy_price,
            sell_price=sell_price,
            size=available_size,
            trade_type=trade_type
        )
    
        # Determine the safe trade size using binary search
        safe_trade_size = self.portfolio.can_trade(trade)
    
        if safe_trade_size == 0:
            # print("❌ Trade blocked: Not enough unleveraged capital!")
            return
    
        if safe_trade_size < available_size:
            print(f"⚠️ Trade partially executed: {safe_trade_size:.2f}/{available_size:.2f}")
    
        # Finalize trade with adjusted size
        trade.size = safe_trade_size
        trade.apply(self.portfolio)
    
        # Remove executed volume from order books
        self.order_books[buy_market].update_liquidity("ask", buy_price, safe_trade_size)
        self.order_books[sell_market].update_liquidity("bid", sell_price, safe_trade_size)
    
        # Store trade
        self.trades.append(trade)
        # print(trade)

