#!/bin/bash

read -p "Enter the folder containing the raw CSV files (e.g., ~/raw_csv_data): " raw_csv_folder
read -p "Enter the days to process (comma-separated, e.g., 12-04,12-05,12-06): " days

pqt_output_dir="data/preprocessed_data/pqt"
actions_output_dir="data/preprocessed_data/actions"

python3 scripts/organize_raw_data.py "$raw_csv_folder" "$days"
echo "✅ Raw .csv files organized and extracted."

python3 scripts/preprocess_order_book.py "$days" "data/raw_data" "$pqt_output_dir"
echo "✅ CSV to Parquet conversion complete."

python3 scripts/generate_market_actions.py "$days" "$pqt_output_dir" "$actions_output_dir"
echo "✅ Market actions extracted."

echo "🎉 Data preprocessing completed!"
