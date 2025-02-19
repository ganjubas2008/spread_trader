#!/bin/bash

read -p "Enter the folder containing the raw CSV files (e.g., ~/znamenka_data): " raw_csv_folder
read -p "Enter the days to process (comma-separated, e.g., 12-04,12-05,12-06): " days

pqt_output_dir="data/preprocessed_data/pqt"
actions_output_dir="data/preprocessed_data/actions"
# days="12-04"
# raw_csv_folder="~/znamenka_data"

python3 scripts/convert_csv_to_parquet.py "$days" "$raw_csv_folder" "$pqt_output_dir"

echo ".csv -> .pqt conversion complete."

python3 scripts/extract_actions.py "$days" "$pqt_output_dir" "$actions_output_dir"

echo ".pqt data -> Actions conversion complete."