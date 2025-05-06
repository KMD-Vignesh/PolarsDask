import polars as pl
import tarfile
import tempfile
import os
from pathlib import Path

# Paths
tar_gz_path = "path/to/your/outer_archive.tar.gz"
output_parquet = "output.parquet"

# Temporary directory for extracted files
with tempfile.TemporaryDirectory() as temp_dir:
    # Extract outer tar.gz
    with tarfile.open(tar_gz_path, "r:gz") as outer_tar:
        outer_tar.extractall(temp_dir)
    
    # List to store lazy DataFrames
    lazy_dfs = []
    
    # Process inner tar files
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".tar"):
                inner_tar_path = os.path.join(root, file)
                with tarfile.open(inner_tar_path, "r") as inner_tar:
                    inner_tar.extractall(temp_dir)
    
    # Scan CSVs lazily
    for root, _, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                lazy_dfs.append(pl.scan_csv(csv_path))
    
    # Concatenate and write to Parquet
    combined_df = pl.concat(lazy_dfs, how="vertical")
    combined_df.sink_parquet(output_parquet)