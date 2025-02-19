from objects.action import Action
from objects.order_book import OrderBook

from __init__ import *

class Portfolio:
    """Tracks the user's portfolio and applies interest rates."""
    
    def __init__(self, initial_cny=0, initial_rub=0, initial_itrf=0, initial_perp=0, leverage_limit=5):
        self.cny_balance = initial_cny
        self.rub_balance = initial_rub
        self.itrf_balance = initial_itrf
        self.perp_balance = initial_perp
        self.leverage_limit = leverage_limit
        self.last_update_ts_dt = None  # Track last interest update
        self.value_history = []  # Store portfolio value over time

        self.interest_rates = {
            "CNY": 0.05,  # 5% annual rate
            "RUB": 0.21   # 21% annual rate
        }
        
        self.last_pnl = 0

    def apply_interest(self, current_ts_dt):
        """Accrues interest on balances based on time elapsed."""
        if self.last_update_ts_dt is None:
            self.last_update_ts_dt = current_ts_dt
            return

        time_diff = (current_ts_dt.value - self.last_update_ts_dt.value) / (365 * 24 * 60 * 60 * 10**9)
        self.cny_balance *= (1 + self.interest_rates["CNY"] * time_diff)
        self.rub_balance *= (1 + self.interest_rates["RUB"] * time_diff)
        self.last_update_ts_dt = current_ts_dt

    def can_trade(self, trade):
        """Uses binary search to find the maximum safe trade size within leverage constraints."""
        unleveraged_balance = (
            (self.cny_balance + self.itrf_balance + self.perp_balance) * 14 + self.rub_balance
        )
    
        if unleveraged_balance <= 0:
            return 0
    
        left, right = 0, trade.size
        best_size = 0  
    
        while left <= right:
            mid = (left + right) / 2  
            temp_trade = deepcopy(trade)
            temp_trade.size = mid  
            
            temp_portfolio = deepcopy(self)
            temp_trade.apply(temp_portfolio)
    
            leveraged_balance = (
                (abs(temp_portfolio.cny_balance) + abs(temp_portfolio.itrf_balance) + abs(temp_portfolio.perp_balance)) * 14
                + abs(temp_portfolio.rub_balance)
            )
    
            if leveraged_balance / unleveraged_balance <= self.leverage_limit:
                best_size = mid  
                left = mid + 1e-3  
            else:
                right = mid - 1e-3  
    
        return round(best_size)  

    def update_balances(self, trade):
        """Updates the portfolio after a spread trade."""
        self.apply_interest(trade.ts_dt)

        if trade.buy_market == 'spot':
            self.cny_balance += trade.size
            self.rub_balance -= trade.size * trade.buy_price
        if trade.buy_market == 'perp':
            self.perp_balance += trade.size
            self.rub_balance -= trade.size * trade.buy_price
        if trade.buy_market == 'itrf':
            self.itrf_balance += trade.size
            self.rub_balance -= trade.size * trade.buy_price

        if trade.sell_market == 'spot':
            self.cny_balance -= trade.size
            self.rub_balance += trade.size * trade.sell_price
        if trade.sell_market == 'perp':
            self.perp_balance -= trade.size
            self.rub_balance += trade.size * trade.sell_price
        if trade.sell_market == 'itrf':
            self.itrf_balance -= trade.size
            self.rub_balance += trade.size * trade.sell_price

    def approximate_pnl(self, order_books, cny_initial):
        """Computes PnL assuming infinite liquidity for quick estimation."""
        spot_bid, _ = order_books["spot"].get_best_bid_ask()
        perp_bid, _ = order_books["perp"].get_best_bid_ask()
        itrf_bid, _ = order_books["itrf"].get_best_bid_ask()

        spot_value = self.cny_balance * (spot_bid if spot_bid else 0)
        perp_value = self.perp_balance * (perp_bid if perp_bid else 0)
        itrf_value = self.itrf_balance * (itrf_bid if itrf_bid else 0)
        
        if (not spot_bid) or (not perp_bid) or (not itrf_bid): # Can't calculate PnL at the moment, so assume it is zero
            return self.last_pnl

        total_value = self.rub_balance + spot_value + perp_value + itrf_value
        initial_value = cny_initial * (spot_bid if spot_bid else 0)

        self.value_history.append(total_value)  
        self.last_pnl = total_value - initial_value
        return self.last_pnl

    def calculate_sharpe(self, risk_free_rate=0.02):
        """Calculates the Sharpe ratio for portfolio performance."""
        if len(self.value_history) < 2:
            return 0

        returns = np.diff(self.value_history) / self.value_history[:-1]
        excess_returns = returns - risk_free_rate / 252  

        return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252) if np.std(excess_returns) > 0 else 0

    def calculate_max_drawdown(self):
        """Calculates the maximum drawdown experienced by the portfolio."""
        if not self.value_history:
            return 0

        cumulative_max = np.maximum.accumulate(self.value_history)
        drawdowns = (self.value_history - cumulative_max) / cumulative_max
        return np.min(drawdowns)  

    def __repr__(self):
        return (
            f"Portfolio:\n"
            f"CNY: {self.cny_balance:.2f}, RUB: {self.rub_balance:.2f}\n"
            f"PERP: {self.perp_balance:.2f}, ITRF: {self.itrf_balance:.2f}\n"
            f"Last Update: {self.last_update_ts_dt}"
        )
