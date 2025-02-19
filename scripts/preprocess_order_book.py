import sys
import argparse
from pathlib import Path
from utils import preprocess_and_save_to_parquet, get_data_paths

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet format.")
    parser.add_argument("days", type=str, help="Comma-separated list of days to process (e.g., 12-04,12-05)")
    parser.add_argument("folder", type=str, help="Folder containing the raw CSV files (e.g., data/raw_data)")
    parser.add_argument("output_dir", type=str, help="Output directory for the Parquet files (e.g., data/preprocessed_data/pqt)")

    args = parser.parse_args()

    days = args.days.split(',')
    input_folder = Path(args.folder)
    output_dir = Path(args.output_dir)

    for day in days:
        for instrument in ['spot', 'perp', 'itrf']:
            input_csv_path = f"{input_folder}/{day}/{instrument}.csv"
            
            Path(f"{output_dir}/{day}").mkdir(parents=True, exist_ok=True)
            output_parquet_path = f"{output_dir}/{day}/{instrument}_ob_data.parquet"
            
            preprocess_and_save_to_parquet(input_csv_path, output_parquet_path)

