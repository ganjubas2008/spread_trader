from objects.action import Action

from __init__ import *

class OrderBook:
    def __init__(self, row, instrument):
        """Initialize order book from a row."""
        
        
        self.instrument = instrument
        
        if type(row) != type(None):
            self.ts_ns = row['ts_ns']
            self.ts_dt = row['ts_dt']
            
            self.asks = self._parse_levels(row, 'ask')
            self.bids = self._parse_levels(row, 'bid')
        else:
            self.ts_ns = 0
            self.ts_dt = 0
            
            self.asks = {}
            self.bids = {}

    def _parse_levels(self, row, side):
        """Parse order book levels into a structured format."""
        book = {}
        n_levels = 10
        for i in range(1, n_levels + 1):
            price = row[f'{side}_price_{i}']
            volume = row[f'{side}_volume_{i}']

            if volume > 0:
                book[price] = volume
        return book

    def compute_differences(self, new_ob):
        """Compute actions needed to transform this order book into new_ob."""
        actions = []
        actions.extend(self._compare_books(self.asks, new_ob.asks, "ask", new_ob.ts_dt, new_ob.instrument))
        actions.extend(self._compare_books(self.bids, new_ob.bids, "bid", new_ob.ts_dt, new_ob.instrument))
        return actions

    def _compare_books(self, old_book, new_book, side, ts_dt, instrument):
        """Compute ADD and REMOVE actions to transform old_book into new_book."""
        actions = []

        # Remove old volume or entire price level
        for price, old_volume in old_book.items():
            new_volume = new_book.get(price, 0)
            if new_volume < old_volume:  # Volume decreased
                actions.append(Action("remove", side, price, old_volume - new_volume, ts_dt, instrument))

        # Add new volume or new price level
        for price, new_volume in new_book.items():
            old_volume = old_book.get(price, 0)
            if new_volume > old_volume:  # Volume increased or new price appeared
                actions.append(Action("add", side, price, new_volume - old_volume, ts_dt, instrument))

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
        return f"OrderBook({self.ts_dt}, Asks: {list(self.asks.items())[:3]}, Bids: {list(self.bids.items())[:3]})"