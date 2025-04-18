import pandas as pd

def clean_input_file(input_file: str = 'data/input/input_bbls.csv'):
    """
    Clean the input file to have just the BBL column.
    
    Args:
        input_file (str): Path to the input CSV file
    """
    # Read the input file
    df = pd.read_csv(input_file, dtype={'BBL': str})
    
    # Keep only the BBL column
    df = df[['BBL']]
    
    # Save the cleaned file
    df.to_csv(input_file, index=False)
    print(f"Cleaned input file saved to {input_file}")
    print(f"Total BBLs: {len(df)}")

if __name__ == "__main__":
    clean_input_file() 