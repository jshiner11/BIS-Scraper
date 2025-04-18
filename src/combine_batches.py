import os
import pandas as pd
from collections import defaultdict

def combine_batches():
    # Get all property data CSV files
    output_dir = os.path.join('data', 'output')
    csv_files = [f for f in os.listdir(output_dir) if f.startswith('property_data_batch_') and f.endswith('.csv')]
    
    # Dictionary to store all data, keyed by BBL
    all_data = {}
    
    # Process each file
    for csv_file in sorted(csv_files):
        file_path = os.path.join(output_dir, csv_file)
        print(f"Processing {csv_file}...")
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Process each row
        for _, row in df.iterrows():
            bbl = row['BBL']
            
            # If this BBL is new or has more complete data than what we have
            if bbl not in all_data or (
                pd.isna(all_data[bbl]['Primary Address']) and 
                not pd.isna(row['Primary Address'])
            ):
                all_data[bbl] = row
    
    # Convert dictionary to DataFrame
    combined_df = pd.DataFrame.from_dict(all_data, orient='index')
    
    # Sort by BBL
    combined_df = combined_df.sort_values('BBL')
    
    # Save to a new CSV file
    output_file = os.path.join(output_dir, 'property_data_combined.csv')
    combined_df.to_csv(output_file, index=False)
    
    # Print summary
    print(f"\nCombined {len(csv_files)} files into one master file:")
    print(f"Total unique BBLs: {len(combined_df)}")
    print(f"Output saved to: {output_file}")
    
    # Print column information
    print("\nColumns in the combined file:")
    for col in combined_df.columns:
        non_empty = combined_df[col].notna().sum()
        print(f"{col}: {non_empty} non-empty values")

if __name__ == "__main__":
    combine_batches() 