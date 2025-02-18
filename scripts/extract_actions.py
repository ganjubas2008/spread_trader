import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from objects.order_book import OrderBook
from objects.action import Action

from __init__ import *

def dump_actions_from_paths(df_paths, inst, chunk_size=100_000, output_path=None):
    """
    Parses dataframes from given paths chunk by chunk, processes actions, and dumps them to a parquet file.

    Args:
        df_paths (list): A list of paths to parquet files.
        inst (str): Instrument identifier.
        chunk_size (int): Number of actions to accumulate before writing to parquet.
        output_path (str): Path to the output parquet file.
    """
    if not output_path:
        output_path = f"actions_{inst}.parquet"

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    parquet_schema = pa.schema([
        ("action_type", pa.string()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("volume", pa.int64()),
        ("ts_dt", pa.timestamp('ns')),
        ("instrument", pa.string())
    ])

    writer = None
    actions_list = []  # Accumulate actions across dataframes
    first_orderbook = True #Set an event variable to check is the first dataframe is loaded.

    for path in df_paths:
        # Use chunked iteration to read large CSV/Parquet files
        
        if path.endswith('.parquet'):
            reader = pq.ParquetFile(path)
            for i in range(reader.num_row_groups):
                df = reader.read_row_group(i).to_pandas()
                for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Extracting actions from dataframe {path}; processing (Row Group {i+1}/{reader.num_row_groups})"):
                    if first_orderbook:
                        ob = OrderBook(row, inst)
                        first_orderbook = False #Now all next iterations will use row of dataframe, instead of the first row.
                    ob_new = OrderBook(row, inst)
                    actions = ob.compute_differences(ob_new)
                    ob = ob_new

                    actions_list.extend([(a.action_type, a.side, a.price, a.volume, pd.to_datetime(a.ts_dt), a.instrument) for a in actions])
                    if len(actions_list) >= chunk_size:
                        df_chunk = pd.DataFrame(actions_list, columns=parquet_schema.names)
                        table = pa.Table.from_pandas(df_chunk, schema=parquet_schema)
                        if writer is None:
                            writer = pq.ParquetWriter(output_path, parquet_schema)
                        writer.write_table(table)
                        actions_list = []
                
        elif path.endswith('.csv'):
            for df in pd.read_csv(path, chunksize=chunk_size):
                # Iterate over rows in the dataframe chunk
                for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {path} (Chunk)"):
                    if first_orderbook:
                        ob = OrderBook(row, inst)
                        first_orderbook = False #Now all next iterations will use row of dataframe, instead of the first row.
                    ob_new = OrderBook(row, inst)
                    actions = ob.compute_differences(ob_new)
                    ob = ob_new

                    actions_list.extend([(a.action_type, a.side, a.price, a.volume, pd.to_datetime(a.ts_dt), a.instrument) for a in actions])
                    if len(actions_list) >= chunk_size:
                        df_chunk = pd.DataFrame(actions_list, columns=parquet_schema.names)
                        table = pa.Table.from_pandas(df_chunk, schema=parquet_schema)
                        if writer is None:
                            writer = pq.ParquetWriter(output_path, parquet_schema)
                        writer.write_table(table)
                        actions_list = []
                        
        else:
            print(f"Unsupported file format for {path}. Supported formats: .parquet, .csv")

    # Write any remaining actions
    if actions_list:
        df_chunk = pd.DataFrame(actions_list, columns=parquet_schema.names)
        table = pa.Table.from_pandas(df_chunk, schema=parquet_schema)
        if writer is None:
            writer = pq.ParquetWriter(output_path, parquet_schema)
        writer.write_table(table)

    if writer:
        writer.close()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract actions from order book data.")
    parser.add_argument("days", type=str, help="Comma-separated list of days to process (e.g., 12-04,12-05)")
    parser.add_argument("folder", type=str, help="Folder containing the preprocessed Parquet files (e.g., data/preprocessed_data/pqt)")
    parser.add_argument("output_dir", type=str, help="Output directory for the actions Parquet files (e.g., data/preprocessed_data/actions)")
    
    args = parser.parse_args()
    
    days = args.days.split(',')
    instruments = ['spot']  # Adjust as needed

    paths = dict()

    for day in days:
        paths[day] = dict(
            [(instrument, f'{args.folder}/{day}/{instrument}_ob_data.parquet') for instrument in instruments]
        )

    for instrument in instruments:
        dump_actions_from_paths(
            df_paths=[paths[day][instrument] for day in days],
            inst=instrument,
            output_path=f'{args.output_dir}/{instrument}_actions.parquet',
            chunk_size=100_000
        )
