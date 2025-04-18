import os
import pandas as pd
from collections import defaultdict

def check_duplicates():
    # Get all property data CSV files
    output_dir = os.path.join('data', 'output')
    csv_files = [f for f in os.listdir(output_dir) if f.startswith('property_data_batch_') and f.endswith('.csv')]
    
    # Dictionary to track BBLs and their occurrences
    bbl_occurrences = defaultdict(list)
    
    # Check each file
    for csv_file in sorted(csv_files):
        file_path = os.path.join(output_dir, csv_file)
        print(f"\nChecking {csv_file}...")
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Check for duplicates within the file
        duplicates = df[df.duplicated(subset=['BBL'], keep=False)]
        if not duplicates.empty:
            print(f"Found {len(duplicates)} duplicates within {csv_file}:")
            print(duplicates[['BBL', 'Primary Address']].to_string())
        
        # Track BBLs across files
        for bbl in df['BBL']:
            bbl_occurrences[bbl].append(csv_file)
    
    # Check for BBLs that appear in multiple files
    print("\nChecking for BBLs that appear in multiple files...")
    for bbl, files in bbl_occurrences.items():
        if len(files) > 1:
            print(f"BBL {bbl} appears in {len(files)} files: {', '.join(files)}")

if __name__ == "__main__":
    check_duplicates() 