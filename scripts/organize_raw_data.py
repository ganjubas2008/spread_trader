from __init__ import *
import gzip

# Map raw file patterns to target instrument names
INSTRUMENT_MAP = {
    "CNYRUBF": "perp",
    "CNYRUB_TOM": "spot",
    "CRZ4": "itrf"
}

if len(sys.argv) != 3:
    print("Usage: python rename_and_copy_csv.py <source_directory> <comma_separated_days>")
    sys.exit(1)

source_dir = os.path.expanduser(sys.argv[1])
selected_dates = set(sys.argv[2].split(","))

destination_dir = "data/raw_data"
Path(destination_dir).mkdir(parents=True, exist_ok=True)

for file in os.listdir(source_dir):
    if file.endswith(".gz"):
        parts = file.split(".")
        date_part = parts[-2]  # Extract YYYY-MM-DD
        formatted_date = date_part[-5:]  # Get MM-DD format

        # Identify the instrument type
        instrument = None
        for key, name in INSTRUMENT_MAP.items():
            if key in file:
                instrument = name
                break
        
        if formatted_date in selected_dates and instrument:
            new_dir = os.path.join(destination_dir, formatted_date)
            Path(new_dir).mkdir(parents=True, exist_ok=True)

            source_path = os.path.join(source_dir, file)
            destination_path = os.path.join(new_dir, f"{instrument}.csv.gz")

            shutil.copy2(source_path, destination_path)

            # Unzip and save as `spot.csv`, `perp.csv`, or `itrf.csv`
            extracted_file_path = os.path.join(new_dir, f"{instrument}.csv")
            with gzip.open(destination_path, "rb") as gz_file, open(extracted_file_path, "wb") as out_file:
                shutil.copyfileobj(gz_file, out_file)

            os.remove(destination_path)  # Delete the .gz file after extraction
            print(f"✅ {file} -> {instrument}.csv")

print("All selected files copied, renamed, and extracted.")
