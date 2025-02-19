#!/bin/bash

read -p "Enter the folder containing the raw CSV files (e.g., ~/raw_csv_data): " raw_csv_folder
read -p "Enter the days to process (comma-separated, e.g., 12-04,12-05,12-06): " days

pqt_output_dir="data/preprocessed_data/pqt"
actions_output_dir="data/preprocessed_data/actions"

python3 scripts/rename_and_copy_csv.py "$raw_csv_folder" "$days"

echo "Raw .csv data renamed, copied and restructured."

python3 scripts/convert_csv_to_parquet.py "$days" "data/raw_data" "$pqt_output_dir"

echo ".csv -> .pqt conversion complete."

python3 scripts/extract_actions.py "$days" "$pqt_output_dir" "$actions_output_dir"

echo ".pqt data -> Actions conversion complete."