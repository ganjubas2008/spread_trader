from __init__ import *
import gzip

if len(sys.argv) != 3:
    print("Usage: python rename_and_copy_csv.py <source_directory> <comma_separated_days>")
    sys.exit(1)

source_dir = os.path.expanduser(sys.argv[1])
selected_dates = set(sys.argv[2].split(","))

destination_dir = "data/raw_data"
Path(destination_dir).mkdir(parents=True, exist_ok=True)

for file in os.listdir(source_dir):
    if file.endswith(".gz"):
        date_part = file.split(".")[-2]  # Extract YYYY-MM-DD
        formatted_date = date_part[-5:]  # Get MM-DD format

        if formatted_date in selected_dates:
            new_dir = os.path.join(destination_dir, formatted_date)
            Path(new_dir).mkdir(parents=True, exist_ok=True)

            source_path = os.path.join(source_dir, file)
            destination_path = os.path.join(new_dir, file.replace("#", "_"))

            shutil.copy2(source_path, destination_path)

            # Unzip the copied file
            extracted_file_path = destination_path[:-3]  # Remove `.gz` extension
            with gzip.open(destination_path, "rb") as gz_file, open(extracted_file_path, "wb") as out_file:
                shutil.copyfileobj(gz_file, out_file)

            os.remove(destination_path)  # Delete the .gz file after extraction

print("Selected files successfully copied, renamed, and extracted.")
