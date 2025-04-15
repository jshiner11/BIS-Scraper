# DOB BIS Scraper

A web scraper for extracting property data from the NYC Department of Buildings Building Information System (BIS) website.

## Project Structure

```
BIS-Scraper/
├── src/                    # Source code
│   └── scraper.py         # Main scraper implementation
├── data/                  # Data files
│   ├── input/            # Input files
│   │   └── input_bbls.csv
│   └── output/           # Output files
│       ├── property_data.csv    # Scraped property data in CSV format
│       └── processed_bbls.txt   # Track processed BBLs
├── temp/                  # Temporary files
│   ├── detail_page.html
│   └── search_results.html
├── docs/                  # Documentation
│   └── README.md
├── tests/                # Test files
├── .gitignore
├── requirements.txt
└── venv/                 # Virtual environment
```

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

1. Prepare your input BBLs in `data/input/input_bbls.csv`
2. Run the scraper:
```bash
python src/scraper.py
```

The scraper will:
- Process each BBL from the input file
- Save detailed property information in `data/output/property_data.csv`
- Track processed BBLs in `data/output/processed_bbls.txt`

## Output Files

- `property_data.csv`: CSV file containing all scraped property data
- `processed_bbls.txt`: Text file tracking which BBLs have been processed

## Dependencies

See `requirements.txt` for the list of required Python packages.

## License

[Add your license information here]
