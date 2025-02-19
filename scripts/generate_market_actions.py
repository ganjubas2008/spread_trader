import sys
import argparse
from utils import process_order_book_actions

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract market actions from order book data.")
    parser.add_argument("days", type=str, help="Comma-separated list of days to process (e.g., 12-04,12-05)")
    parser.add_argument("folder", type=str, help="Path to preprocessed Parquet files (e.g., data/preprocessed_data/pqt)")
    parser.add_argument("output_dir", type=str, help="Output directory for the actions Parquet files (e.g., data/preprocessed_data/actions)")

    args = parser.parse_args()
    days = args.days.split(',')

    for instrument in ["spot", "perp", "itrf"]:
        print(f"🚀 Extracting actions for {instrument}...")
        process_order_book_actions(args.folder, args.output_dir, instrument, days)

    print("\n✅ Market actions successfully extracted.")
