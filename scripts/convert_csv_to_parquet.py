import pandas as pd
import os
import re
import argparse
from tqdm import tqdm

# Regex pattern to extract [price; volume; 1]
OB_PATTERN = re.compile(r"\[(.*?);(.*?);1\]")

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

    # Ensure exactly `max_levels` entries (fill missing levels)
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

    # Drop original OB columns
    df_chunk = df_chunk.drop(columns=[str(i) for i in range(5, 16)] + [str(i) for i in range(16, 26)], errors='ignore')

    # Drop other unnecessary columns
    df_chunk = df_chunk.drop(columns=["1", "3", "4", "6"], errors='ignore')

    return df_chunk.reset_index()[5:].reset_index().drop(['level_0', 'index'], axis=1).rename(columns={'0': 'ts_ns', '2': 'ts_dt'}).dropna()

def preprocess_and_save_to_parquet(input_csv_path, output_parquet_path, chunk_size=100_000, max_levels=10):
    """Reads a large CSV in chunks, processes each chunk, and appends to Parquet iteratively."""
    first_chunk = True

    for df_chunk in tqdm(pd.read_csv(input_csv_path, chunksize=chunk_size, low_memory=False), desc=f"Processing {input_csv_path}"):
        processed_chunk = process_dataframe_chunk(df_chunk, max_levels)

        # Append processed chunk to Parquet file
        processed_chunk.to_parquet(output_parquet_path, 
                                   engine='fastparquet', 
                                   index=False, 
                                   compression='snappy', 
                                   append=not first_chunk)
        
        first_chunk = False

def get_data_paths(folder = 'raw_data', days=['12-04', '12-05', '12-06']):
    instruments = ['spot', 'perp', 'itrf']
    extended_name = {
        'spot': 'Local_FAST_CURR_MD_MOEX_CURR_CETS_CNYRUB_TOM',
        'perp': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CNYRUBF',
        'itrf': 'Local_FAST_SPECTRA_MD_MOEX_SPECTRA_FUT_CRZ4'
    }

    

    data_paths = {}

    for day in days:
        data_paths[day] = dict(
            [(instrument, f'{folder}/{day}/{extended_name[instrument]}.2024-{day}') for instrument in instruments]
        )
        
    return data_paths

def convert_csv_to_parquet(days, folder, output_directory):
    data_paths = get_data_paths(folder=folder, days=days)
    instruments = ['spot', 'itrf', 'perp']

    for day, instrument_paths in data_paths.items():
        for instrument in instruments:
            if instrument in instrument_paths:
                # Define paths
                input_path = instrument_paths[instrument]
                output_folder = os.path.join(output_directory, day)
                output_path = os.path.join(output_folder, f"{instrument}_ob_data.parquet")

                # Ensure the output folder exists
                os.makedirs(output_folder, exist_ok=True)

                # Process and save
                preprocess_and_save_to_parquet(input_path, output_path)
            else:
                print(f"⚠️ Skipping {instrument} for {day} (file not found)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format.")
    parser.add_argument("days", type=str, help="Comma-separated list of days to process (e.g., 12-04,12-05)")
    parser.add_argument("folder", type=str, help="Folder containing the raw CSV files (e.g., data/raw_data)")
    parser.add_argument("output_dir", type=str, help="Output directory for the Parquet files (e.g., data/preprocessed_data/pqt)")
    
    args = parser.parse_args()
    
    days = args.days.split(',')
    folder = args.folder
    output_directory = args.output_dir
    
    convert_csv_to_parquet(days, folder, output_directory)