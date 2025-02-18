from __init__ import *

class Action:
    def __init__(self, action_type, side, price, volume, ts_dt, instrument):
        assert action_type in {"add", "remove"}
        assert side in {"ask", "bid"}
        assert volume > 0
        assert instrument in {"spot", "itrf", "perp"}

        self.action_type = action_type
        self.side = side
        self.price = price
        self.volume = volume
        self.ts_dt = ts_dt
        self.instrument = instrument

    def apply_ob(self, order_books):
        ob = order_books[self.instrument]
        book = ob.asks if self.side == "ask" else ob.bids

        if self.action_type == "remove":
            if self.price in book:
                book[self.price] -= self.volume
                if book[self.price] <= 0:
                    del book[self.price]
        elif self.action_type == "add":
            book[self.price] = book.get(self.price, 0) + self.volume

    def to_dict(self):
        return {
            "action_type": self.action_type,
            "side": self.side,
            "price": self.price,
            "volume": self.volume,
            "ts_dt": self.ts_dt,
            "instrument": self.instrument,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["action_type"], data["side"], data["price"],
            data["volume"], data["ts_dt"], data["instrument"]
        )

    @staticmethod
    def save_to_parquet(actions, filepath):
        df = pd.DataFrame([a.to_dict() for a in actions])
        df.to_parquet(filepath, engine='pyarrow')

    @staticmethod
    def load_from_parquet(filepath):
        df = pd.read_parquet(filepath, engine='pyarrow')
        return [Action.from_dict(row) for row in df.to_dict(orient='records')]

    def __repr__(self):
        return f"Action({self.ts_dt}, {self.instrument.upper()}, {self.action_type.upper()} {self.side.upper()} @ {self.price}: {self.volume})"