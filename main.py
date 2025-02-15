from src.trader import *
from src.reading import *
from src.objects import *


# FIRST PART: Read the data
paths = {
    'perp': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CNYRUBF.2024-12-04',
    'itrf': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CRZ4.2024-12-04',
    'spot': 'Local_FAST_CURR_MD_MOEX_CURR_CETS_CNYRUB_TOM.2024-12-04'
}


nrows = 100_000
df_spot = read_df('spot', paths, nrows)
df_perp = read_df('perp', paths, nrows)
df_itrf = read_df('itrf', paths, nrows)

# SECOND PART: Create action list

actions = []
order_books = {"spot": None, "itrf": None, "perp": None}

# Iterate over each dataset independently
for instrument, df in {"spot": df_spot, "itrf": df_itrf, "perp": df_perp}.items():
    prev_ob = None

    for _, row in tqdm(df.iterrows()):
        current_ob = OrderBook(row, instrument)

        if prev_ob:
            actions.extend(prev_ob.compute_differences(current_ob))

        prev_ob = current_ob  # Move to the next snapshot

actions.sort(key=lambda action: action.timestamp)  # Sort in chronological order


# THIRD PART: Running strategy

# Initialize empty order books
portfolio = Portfolio(initial_cny=1000, initial_rub=0)

empty_order_books = {
    "spot": OrderBook(df_spot.iloc[0], "spot"),
    "itrf": OrderBook(df_itrf.iloc[0], "itrf"),
    "perp": OrderBook(df_perp.iloc[0], "perp")
}

# Clear out levels to start from an empty state
for ob in empty_order_books.values():
    ob.asks = {}
    ob.bids = {}

# Initialize the trading strategy
trader = SpreadTrader(empty_order_books, portfolio)

c = 0

# Apply actions in order
previous_timestamp = None

for action in tqdm(actions):
    
    action.apply(empty_order_books)
    for instrument in ['spot', 'perp', 'itrf']:
        empty_order_books[instrument].timestamp = action.timestamp

    if previous_timestamp is None or action.timestamp != previous_timestamp:
        
        flag = trader.find_spread_opportunity()  # Execute taker trades
        c += flag
        
        previous_timestamp = action.timestamp

print("\nFinal Portfolio State:")
print(portfolio, c)