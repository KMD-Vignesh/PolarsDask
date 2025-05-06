import polars as pl
import tarfile
import tempfile
import os
from pathlib import Path

# Paths
tar_gz_path = "path/to/your/outer_archive.tar.gz"
output_parquet = "output.parquet"

print(f"Starting process with tar.gz file: {tar_gz_path}")

# Temporary directory for extracted files
with tempfile.TemporaryDirectory() as temp_dir:
    print(f"Created temporary directory: {temp_dir}")

    # Extract outer tar.gz
    with tarfile.open(tar_gz_path, "r:gz") as outer_tar:
        print("Extracting outer tar.gz archive...")
        outer_tar.extractall(temp_dir, filter="data")
        print("Outer tar.gz extracted successfully.")

    # Process inner tar files
    print("Scanning for inner tar files...")
    inner_tar_count = 0
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".tar"):
                inner_tar_count += 1
                inner_tar_path = os.path.join(root, file)
                print(f"Found inner tar file: {inner_tar_path}")
                with tarfile.open(inner_tar_path, "r") as inner_tar:
                    print(f"Extracting inner tar file: {file}...")
                    inner_tar.extractall(temp_dir, filter="data")
                    print(f"Inner tar file {file} extracted successfully.")
    print(f"Total inner tar files found and extracted: {inner_tar_count}")

    # Scan CSVs lazily
    print("Scanning for CSV files...")
    lazy_dfs = []
    csv_count = 0
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_count += 1
                csv_path = os.path.join(root, file)
                print(f"Found CSV file: {csv_path}")
                try:
                    # Use lazy scanning with minimal memory usage
                    lazy_df = pl.scan_csv(csv_path, low_memory=True)
                    lazy_dfs.append(lazy_df)
                except Exception as e:
                    print(f"Error scanning CSV {csv_path}: {e}. Skipping this file.")
    print(f"Total CSV files found: {csv_count}")

    # Concatenate and write to Parquet
    if lazy_dfs:
        print("Concatenating DataFrames and writing to Parquet...")
        try:
            # Concatenate lazily and optimize for memory and speed
            combined_df = pl.concat(lazy_dfs, how="vertical")
            # Use sink_parquet for streaming write
            combined_df.sink_parquet(
                output_parquet,
                compression="snappy",  # Faster compression
                row_group_size=100_000  # Optimize for large datasets
            )
            print(f"Parquet file successfully saved to: {output_parquet}")
        except Exception as e:
            print(f"Error during concatenation or writing Parquet: {e}")
    else:
        print("No CSV files found to process. Exiting without creating a Parquet file.")