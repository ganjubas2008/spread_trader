from src.reading import *

class Action:
    """Represents an order book change (either add or remove) with a timestamp and instrument type."""

    def __init__(self, action_type, side, price, volume, timestamp, instrument):
        assert action_type in {"add", "remove"}, "Invalid action type"
        assert side in {"ask", "bid"}, "Invalid side"
        assert volume > 0, "Volume must be positive"
        assert instrument in {"spot", "itrf", "perp"}, "Invalid instrument type"

        self.action_type = action_type  # "add" or "remove"
        self.side = side  # "ask" or "bid"
        self.price = price  # Price level
        self.volume = volume  # Amount of volume changed
        self.timestamp = timestamp  # MarketTimestamp
        self.instrument = instrument  # "spot", "itrf", or "perp"

    def apply(self, order_books):
        """Applies this action to the correct OrderBook instance."""
        ob = order_books[self.instrument]  # Select the correct order book
        book = ob.asks if self.side == "ask" else ob.bids

        if self.action_type == "remove":
            if self.price in book:
                book[self.price] -= self.volume
                if book[self.price] <= 0:  # If volume reaches 0, remove price level
                    del book[self.price]
        elif self.action_type == "add":
            book[self.price] = book.get(self.price, 0) + self.volume

    def __repr__(self):
        return f"Action({self.timestamp}, {self.instrument.upper()}, {self.action_type.upper()} {self.side.upper()} @ {self.price}: {self.volume})"
    
class OrderBook:
    def __init__(self, row, instrument_type):
        """Initialize order book from a row."""
        self.instrument = instrument_type
        self.timestamp = pd.to_datetime(row[3])  # MarketTimestamp (for sorting)
        self.asks = self._parse_levels(row[5:15])  # Ask side (sorted ascending)
        self.bids = self._parse_levels(row[16:26])  # Bid side (sorted descending)

    def _parse_levels(self, levels):
        """Parse order book levels into a structured format."""
        book = {}
        for level in levels:
            if level != "[;;]":  # Ignore empty levels
                level = level.replace(';', ',')
                price, volume, _ = ast.literal_eval(level)  # Convert "[price;qty;1]" to tuple
                book[float(price)] = int(volume)  # Store price → volume mapping
        return book

    def compute_differences(self, new_ob):
        """Compute actions needed to transform this order book into new_ob."""
        actions = []

        # Compare Ask Side
        actions.extend(self._compare_books(self.asks, new_ob.asks, "ask", new_ob.timestamp, new_ob.instrument))

        # Compare Bid Side
        actions.extend(self._compare_books(self.bids, new_ob.bids, "bid", new_ob.timestamp, new_ob.instrument))

        return actions

    def _compare_books(self, old_book, new_book, side, timestamp, instrument):
        """Compute ADD and REMOVE actions to transform old_book into new_book."""
        actions = []

        # Remove old volume or entire price level
        for price, old_volume in old_book.items():
            new_volume = new_book.get(price, 0)
            if new_volume < old_volume:  # Volume decreased
                actions.append(Action("remove", side, price, old_volume - new_volume, timestamp, instrument))
            elif new_volume == 0:  # Entire price level disappeared
                actions.append(Action("remove", side, price, old_volume, timestamp, instrument))

        # Add new volume or new price level
        for price, new_volume in new_book.items():
            old_volume = old_book.get(price, 0)
            if new_volume > old_volume:  # Volume increased or new price appeared
                actions.append(Action("add", side, price, new_volume - old_volume, timestamp, instrument))

        return actions

    def update_liquidity(self, side, price, volume):
        """Removes executed volume from the order book."""
        book = self.asks if side == "ask" else self.bids
        if price in book:
            book[price] -= volume
            if book[price] <= 0:
                del book[price]  # Remove empty levels

    def get_best_bid_ask(self):
        """Returns the best available bid and ask prices."""
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return best_bid, best_ask

    def __repr__(self):
        return f"OrderBook({self.timestamp}, Asks: {list(self.asks.items())[:3]}, Bids: {list(self.bids.items())[:3]})"
    
class Trade:
    """Represents a completed spread trade, supporting maker/taker fees."""

    def __init__(self, timestamp, buy_market, sell_market, buy_price, sell_price, size, trade_type):
        assert trade_type in {"maker", "taker"}, "Invalid trade type"

        self.timestamp = timestamp
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
        portfolio.apply_interest(self.timestamp)

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
        return (f"Trade({self.timestamp}, {self.trade_type.upper()}, "
                f"BUY {self.size} {self.buy_market} @ {self.buy_price}, "
                f"SELL {self.size} {self.sell_market} @ {self.sell_price})")
    
class Portfolio:
    """Tracks the user's portfolio and applies interest rates."""
    
    def __init__(self, initial_cny=0, initial_rub=0, initial_itrf=0, initial_perp=0, leverage_limit=5):
        self.cny_balance = initial_cny
        self.rub_balance = initial_rub

        self.itrf_balance = initial_itrf
        self.perp_balance = initial_perp

        self.leverge_limit = leverage_limit
        
        self.last_update_timestamp = None  # Track last interest update

        self.interest_rates = {
            "CNY": 0.05,  # 5% annual rate
            "RUB": 0.21   # 21% annual rate
        }

    def apply_interest(self, current_timestamp):
        """Accrues interest on balances based on time elapsed."""
        if self.last_update_timestamp is None:
            self.last_update_timestamp = current_timestamp
            return

        time_diff = (current_timestamp - self.last_update_timestamp).total_seconds() / (365 * 24 * 60 * 60)
        self.cny_balance *= (1 + self.interest_rates["CNY"] * time_diff) # Assuming that cash is stored in the form of debt obligations
        self.rub_balance *= (1 + self.interest_rates["RUB"] * time_diff)
        self.last_update_timestamp = current_timestamp

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
        self.apply_interest(trade.timestamp)

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
        return f"Portfolio:\nCNY: {self.cny_balance:.2f}, RUB: {self.rub_balance:.2f})\nPERP: {self.perp_balance}, ITRF: {self.itrf_balance}"