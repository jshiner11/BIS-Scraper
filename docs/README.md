# DOB BIS Scraper

A web scraper for extracting property data from the NYC Department of Buildings Building Information System (BIS) website.

## Project Structure

```
BIS-Scraper/
├── src/                    # Source code
│   ├── scraper.py         # Main scraper implementation
│   ├── process_batches.py # Batch processing script
│   ├── combine_batches.py # Data combination script
│   ├── check_duplicates.py # Data validation script
│   ├── split_bbls.py     # Input preparation script
│   └── clean_input.py    # Input cleaning script
├── data/                  # Data files
│   ├── input/            # Input files
│   │   ├── input_bbls.csv
│   │   └── batches/     # Split batch files
│   └── output/          # Output files
│       ├── property_data_batch_*.csv  # Individual batch results
│       ├── property_data_combined.csv # Combined results
│       └── processed_bbls_batch_*.txt # Progress tracking
├── logs/                 # Log files
├── temp/                 # Temporary files
├── docs/                 # Documentation
│   └── README.md
├── tests/               # Test files
├── .gitignore
├── requirements.txt
└── venv/                # Virtual environment
```

## Why Batch Processing?

The scraper uses a batch processing approach for several important reasons:

1. **Rate Limiting**: The BIS website has rate limiting to prevent excessive requests. Processing in batches with delays between them helps avoid being blocked.

2. **Error Recovery**: If an error occurs, we only need to reprocess the affected batch rather than starting over from the beginning.

3. **Progress Tracking**: Each batch maintains its own progress file, making it easier to track and resume processing if interrupted.

4. **Memory Management**: Processing large numbers of BBLs in smaller batches is more memory-efficient.

5. **Parallel Processing**: The batch structure allows for potential parallel processing in the future if needed.

6. **Data Integrity**: Each batch's results are saved independently, reducing the risk of data loss if something goes wrong.

## Installation

1. Clone the repository:
```bash
git clone git@github.com:jshiner11/BIS-Scraper.git
cd BIS-Scraper
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### 1. Prepare Input Data

1. Place your BBLs in `data/input/input_bbls.csv`
2. Split the input into batches:
```bash
python src/split_bbls.py
```

### 2. Process Batches

Run the batch processor:
```bash
python src/process_batches.py
```

The script will:
- Process each batch sequentially
- Save results in `data/output/property_data_batch_*.csv`
- Track progress in `data/output/processed_bbls_batch_*.txt`
- Wait 5 minutes between batches to avoid rate limiting

### 3. Combine Results

After all batches are processed, combine the results:
```bash
python src/combine_batches.py
```

This will create `data/output/property_data_combined.csv` with all unique BBLs.

### 4. Check for Duplicates

To verify data integrity:
```bash
python src/check_duplicates.py
```

## Output Files

- `property_data_batch_*.csv`: Individual batch results
- `property_data_combined.csv`: Combined results from all batches
- `processed_bbls_batch_*.txt`: Progress tracking for each batch

## Dependencies

See `requirements.txt` for the list of required Python packages.

## License

[Add your license information here]
