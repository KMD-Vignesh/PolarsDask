import tarfile
import polars as pl
from io import BytesIO

def tar_gz_to_single_parquet(tar_gz_path, output_parquet_path, batch_size=10):
    all_lazy_frames = []
    batch_lazy_frames = []

    with tarfile.open(tar_gz_path, 'r|gz') as tar:
        for member in tar:
            if member.isfile() and member.name.endswith('.csv'):
                file_obj = tar.extractfile(member)
                if file_obj:
                    buffer = BytesIO(file_obj.read())
                    lf = pl.read_csv(buffer).lazy()
                    batch_lazy_frames.append(lf)

                if len(batch_lazy_frames) == batch_size:
                    batch_combined = pl.concat(batch_lazy_frames)
                    all_lazy_frames.append(batch_combined)
                    batch_lazy_frames.clear()

        # Process leftover files
        if batch_lazy_frames:
            batch_combined = pl.concat(batch_lazy_frames)
            all_lazy_frames.append(batch_combined)

    # Combine all batches and write to single Parquet
    final_combined = pl.concat(all_lazy_frames)
    final_combined.sink_parquet(output_parquet_path, compression="zstd")
    print(f"Written final Parquet to: {output_parquet_path}")

# Example usage
tar_gz_to_single_parquet("your_file.tar.gz", "final_output.parquet", batch_size=10)