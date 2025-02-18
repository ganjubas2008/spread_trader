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

        self.leverge_limit = leverage_limit
        
        self.last_update_ts_dt = None  # Track last interest update

        self.interest_rates = {
            "CNY": 0.05,  # 5% annual rate
            "RUB": 0.21   # 21% annual rate
        }

    def apply_interest(self, current_ts_dt):
        """Accrues interest on balances based on time elapsed."""
        if self.last_update_ts_dt is None:
            self.last_update_ts_dt = current_ts_dt
            return

        #print(f'__!!!{(current_ts_dt.value, self.last_update_ts_dt.value)}___')
        time_diff = (current_ts_dt.value - self.last_update_ts_dt.value) / (365 * 24 * 60 * 60 * 10**9)
        # print(time_diff * 60 * 10**9, 'sec')
        self.cny_balance *= (1 + self.interest_rates["CNY"] * time_diff) # Assuming that cash is stored in the form of debt obligations
        self.rub_balance *= (1 + self.interest_rates["RUB"] * time_diff)
        self.last_update_ts_dt = current_ts_dt

    def can_trade(self, trade):
        """Uses binary search to find the maximum safe trade size within leverage constraints."""
        
        # Compute initial unleveraged balance
        unleveraged_balance = ((self.cny_balance) + (self.itrf_balance) + (self.perp_balance)) * 14 + (self.rub_balance)
    
        # If unleveraged balance is zero, block trading to avoid division errors
        if unleveraged_balance <= 0:
            return 0
    
        # Set binary search boundaries
        left, right = 0, trade.size
        best_size = 0  # Store the best valid trade size
    
        while left <= right:
            mid = (left + right) / 2  # Test midpoint trade size
            temp_trade = deepcopy(trade)
            temp_trade.size = mid  # Adjust trade size
            
            temp_portfolio = deepcopy(self)
            temp_trade.apply(temp_portfolio)
    
            # Compute leveraged balance after applying trade
            leveraged_balance = (abs(temp_portfolio.cny_balance) + abs(temp_portfolio.itrf_balance) + abs(temp_portfolio.perp_balance)) * 14 + abs(temp_portfolio.rub_balance)
    
            # Check leverage constraint (5x max leverage)
            if leveraged_balance / unleveraged_balance <= 5:
                best_size = mid  # Valid size, try increasing it
                left = mid + 1e-3  # Increase search space (small step for precision)
            else:
                right = mid - 1e-3  # Reduce search space
    
        return round(best_size)  # Return the largest possible trade size

        

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

    def __repr__(self):
        return f"Portfolio:\nCNY: {self.cny_balance:.2f}, RUB: {self.rub_balance:.2f})\nPERP: {self.perp_balance}, ITRF: {self.itrf_balance}\nLast Update: {self.last_update_ts_dt}"