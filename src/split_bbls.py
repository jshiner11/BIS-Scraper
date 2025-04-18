import pandas as pd
import os
from pathlib import Path

def split_bbls_into_batches(input_file: str, batch_size: int = 1000, output_dir: str = 'data/input/batches'):
    """
    Split a large CSV file of BBLs into smaller batch files.
    
    Args:
        input_file (str): Path to the input CSV file containing BBLs
        batch_size (int): Number of BBLs per batch
        output_dir (str): Directory to save the batch files
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Read the input file
    df = pd.read_csv(input_file, dtype={'BBL': str})
    
    # Calculate number of batches
    total_bbls = len(df)
    num_batches = (total_bbls + batch_size - 1) // batch_size
    
    print(f"Splitting {total_bbls} BBLs into {num_batches} batches of {batch_size} BBLs each")
    
    # Split and save batches
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_bbls)
        batch_df = df.iloc[start_idx:end_idx]
        
        # Save batch to CSV
        batch_file = os.path.join(output_dir, f'batch_{i+1}.csv')
        batch_df.to_csv(batch_file, index=False)
        print(f"Saved batch {i+1} to {batch_file} ({len(batch_df)} BBLs)")

if __name__ == "__main__":
    # Path to your input file
    input_file = "data/input/input_bbls.csv"
    
    # Split into batches
    split_bbls_into_batches(input_file) 