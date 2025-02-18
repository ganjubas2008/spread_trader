from objects.portfolio import Portfolio

from __init__ import *

class Trade:
    """Represents a completed spread trade, supporting maker/taker fees."""

    def __init__(self, ts_dt, buy_market, sell_market, buy_price, sell_price, size, trade_type):
        assert trade_type in {"maker", "taker"}, "Invalid trade type"

        self.ts_dt = ts_dt
        self.buy_market = buy_market
        self.sell_market = sell_market
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.size = size
        self.trade_type = trade_type  # "maker" or "taker"
        self.taker_fee = 0.55 / 10**4  # 0.55 bps taker fee
        self.maker_fee = 0  # 0 bps maker rebate

    def apply(self, portfolio):
        """Applies the trade's impact on the portfolio balances, considering maker/taker fees."""
        portfolio.apply_interest(self.ts_dt)

        # Compute commission rate
        fee_rate = self.taker_fee if self.trade_type == "taker" else self.maker_fee

        # Buy side impact
        buy_cost = self.size * self.buy_price
        fee = buy_cost * fee_rate

        if self.buy_market == "spot":
            portfolio.cny_balance += self.size
            portfolio.rub_balance -= buy_cost + fee
        elif self.buy_market == "perp":
            portfolio.perp_balance += self.size
            portfolio.rub_balance -= buy_cost + fee
        elif self.buy_market == "itrf":
            portfolio.itrf_balance += self.size
            portfolio.rub_balance -= buy_cost + fee

        # Sell side impact
        sell_revenue = self.size * self.sell_price
        fee = sell_revenue * fee_rate  # Apply fee on sell side too

        if self.sell_market == "spot":
            portfolio.cny_balance -= self.size
            portfolio.rub_balance += sell_revenue - fee
        elif self.sell_market == "perp":
            portfolio.perp_balance -= self.size
            portfolio.rub_balance += sell_revenue - fee
        elif self.sell_market == "itrf":
            portfolio.itrf_balance -= self.size
            portfolio.rub_balance += sell_revenue - fee

    def __repr__(self):
        return (f"Trade({self.ts_dt}, {self.trade_type.upper()}, "
                f"BUY {self.size} {self.buy_market} @ {self.buy_price}, "
                f"SELL {self.size} {self.sell_market} @ {self.sell_price})")
        
        