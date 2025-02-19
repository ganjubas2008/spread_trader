from __init__ import *

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from objects.order_book import OrderBook
from objects.action import Action


OB_PATTERN = re.compile(r"\[(.*?);(.*?);1\]")

def extract_gz(file_path):
    """Unzips a .gz file and removes the original compressed file."""
    extracted_path = file_path.with_suffix("")
    with gzip.open(file_path, "rb") as gz_file, open(extracted_path, "wb") as out_file:
        shutil.copyfileobj(gz_file, out_file)
    file_path.unlink()  # Remove the .gz file

def parse_order_book(entries, max_levels=10):
    """Extracts price and volume from OB entries, filling missing levels with (0,0)."""
    parsed = []
    
    for entry in entries:
        match = OB_PATTERN.match(str(entry).strip())  # Ensure string type
        if match:
            price, volume = match.groups()
            parsed.append((float(price), int(volume)))
        else:
            parsed.append((0.0, 0))  # Fill missing levels

    while len(parsed) < max_levels:
        parsed.append((0.0, 0))
    
    return parsed[:max_levels]  # Always return `max_levels` elements

def process_dataframe_chunk(df_chunk, max_levels=10):
    """Processes a single chunk of data, extracting full OB depth correctly."""
    df_chunk = df_chunk.rename(columns={col: str(i) for i, col in enumerate(df_chunk.columns)})

    # Filter only relevant order book updates
    df_chunk = df_chunk[df_chunk['31'] == 'MS']

    # Drop unnecessary columns
    df_chunk = df_chunk.drop(columns=[str(i) for i in range(26, 33)], errors='ignore')

    # Convert timestamps
    df_chunk["2"] = pd.to_datetime(df_chunk["2"], errors='coerce')

    # Apply order book parsing
    bid_data = df_chunk[[str(i) for i in range(5, 15)]].apply(lambda row: parse_order_book(row, max_levels), axis=1)
    ask_data = df_chunk[[str(i) for i in range(16, 26)]].apply(lambda row: parse_order_book(row, max_levels), axis=1)

    # Convert lists into separate columns
    for i in range(max_levels):
        df_chunk[f"bid_price_{max_levels-i}"] = bid_data.apply(lambda x: x[i][0])
        df_chunk[f"bid_volume_{max_levels-i}"] = bid_data.apply(lambda x: x[i][1])
        df_chunk[f"ask_price_{i+1}"] = ask_data.apply(lambda x: x[i][0])
        df_chunk[f"ask_volume_{i+1}"] = ask_data.apply(lambda x: x[i][1])

    df_chunk = df_chunk.drop(columns=[str(i) for i in range(5, 16)] + [str(i) for i in range(16, 26)], errors='ignore')

    df_chunk = df_chunk.drop(columns=["1", "3", "4", "6"], errors='ignore')

    return df_chunk.reset_index()[5:].reset_index().drop(['level_0', 'index'], axis=1).rename(columns={'0': 'ts_ns', '2': 'ts_dt'}).dropna()

def preprocess_and_save_to_parquet(input_csv_path, output_parquet_path, chunk_size=100_000, max_levels=10):
    """Reads a large CSV in chunks, processes each chunk, and appends to Parquet iteratively."""
    first_chunk = True

    for df_chunk in tqdm(pd.read_csv(input_csv_path, chunksize=chunk_size, low_memory=False), desc=f"Processing {input_csv_path}"):
        processed_chunk = process_dataframe_chunk(df_chunk, max_levels)

        if first_chunk:
            processed_chunk.to_parquet(output_parquet_path, engine='fastparquet', index=False, compression='snappy')
        else:
            processed_chunk.to_parquet(output_parquet_path, engine='fastparquet', index=False, compression='snappy', append=True)
        
        first_chunk = False

def get_data_paths(folder='data/raw_data', days=['12-04', '12-05', '12-06']):
    """Generates paths for given days and instruments."""
    instruments = ['spot', 'perp', 'itrf']
    extended_name = {
        'spot': 'Local_FAST_CURR_MD_MOEX_CURR_CETS_CNYRUB_TOM',
        'perp': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CNYRUBF',
        'itrf': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CRZ4'
    }

    data_paths = {day: {inst: f"{folder}/{day}/{extended_name[inst]}.2024-{day}" for inst in instruments} for day in days}
    
    return data_paths

def convert_csv_to_parquet(days, input_folder, output_dir):
    """Converts CSV files for specified days to Parquet format."""
    data_paths = get_data_paths(input_folder, days)
    instruments = ['spot', 'perp', 'itrf']

    for day, paths in data_paths.items():
        for instrument in instruments:
            if instrument in paths:
                input_path = paths[instrument]
                output_folder = Path(output_dir) / day
                output_path = output_folder / f"{instrument}_ob_data.parquet"

                output_folder.mkdir(parents=True, exist_ok=True)

                preprocess_and_save_to_parquet(input_path, output_path)
            else:
                print(f"⚠️ Skipping {instrument} for {day} (file not found)")

def process_order_book_actions(folder, output_dir, instrument, days, chunk_size=100_000):
    """Processes order book snapshots, extracts actions, and writes them to Parquet."""
    
    parquet_schema = pa.schema([
        ("action_type", pa.string()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("volume", pa.int64()),
        ("ts_dt", pa.timestamp('ns')),
        ("instrument", pa.string())
    ])

    output_path = Path(output_dir) / f"{instrument}_actions.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = None
    actions_list = []
    
    first_orderbook = True
    ob = None

    for day in days:
        input_path = Path(folder) / day / f"{instrument}_ob_data.parquet"
        if not input_path.exists():
            print(f"⚠️ Skipping {input_path} (File not found)")
            continue

        reader = pq.ParquetFile(input_path)
        for i in range(reader.num_row_groups):
            df = reader.read_row_group(i).to_pandas()

            for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {instrument} - {day} (Row Group {i+1}/{reader.num_row_groups})"):
                
                if first_orderbook:
                    ob = OrderBook(row, instrument)
                    first_orderbook = False
                    continue  # Skip action extraction on the first iteration
                
                ob_new = OrderBook(row, instrument)
                actions = ob.compute_differences(ob_new)
                ob = ob_new

                for action in actions:
                    actions_list.append({
                        "action_type": action.action_type,
                        "side": action.side,
                        "price": action.price,
                        "volume": action.volume,
                        "ts_dt": action.ts_dt,
                        "instrument": action.instrument
                    })

                if len(actions_list) >= chunk_size:
                    df_chunk = pd.DataFrame(actions_list, columns=parquet_schema.names)
                    table = pa.Table.from_pandas(df_chunk, schema=parquet_schema)
                    if writer is None:
                        writer = pq.ParquetWriter(output_path, parquet_schema)
                    writer.write_table(table)
                    actions_list = []

    if actions_list:
        df_chunk = pd.DataFrame(actions_list, columns=parquet_schema.names)
        table = pa.Table.from_pandas(df_chunk, schema=parquet_schema)
        if writer is None:
            writer = pq.ParquetWriter(output_path, parquet_schema)
        writer.write_table(table)

    if writer:
        writer.close()
    
    print(f"✅ Actions for {instrument} saved to {output_path}")
