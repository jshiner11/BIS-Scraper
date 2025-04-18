import os
from scraper import BISScraper
import time
from datetime import datetime
import pandas as pd
import logging
import sys

# Get the absolute path to the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(PROJECT_ROOT, 'logs', 'batch_processing.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def process_batches(batch_dir: str = None, output_dir: str = None):
    """
    Process BBL batches sequentially.
    
    Args:
        batch_dir (str): Directory containing batch CSV files
        output_dir (str): Directory to save output files
    """
    # Set default directories if not provided
    if batch_dir is None:
        batch_dir = os.path.join(PROJECT_ROOT, 'data', 'input', 'batches')
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT, 'data', 'output')
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize scraper
    scraper = None
    max_retries = 3
    
    # Get list of batch files
    batch_files = sorted([f for f in os.listdir(batch_dir) if f.startswith('batch_') and f.endswith('.csv')])
    
    logger.info(f"Found {len(batch_files)} batch files to process")
    
    # Process each batch
    for batch_file in batch_files:
        batch_path = os.path.join(batch_dir, batch_file)
        batch_num = batch_file.split('_')[1].split('.')[0]
        
        logger.info(f"\nProcessing batch {batch_num} from {batch_file}")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if batch is already fully processed
        progress_file = os.path.join(output_dir, f'processed_bbls_batch_{batch_num}.txt')
        processed_bbls = set()
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                processed_bbls = {line.strip() for line in f}
        
        # Read BBLs from the batch file
        df = pd.read_csv(batch_path, dtype={'BBL': str})
        total_bbls = set(str(bbl).zfill(10) for bbl in df['BBL'])
        
        # Check if any BBLs need processing
        bbls_to_process = total_bbls - processed_bbls
        
        if not bbls_to_process:
            logger.info(f"Batch {batch_num} is already fully processed, continuing to next batch...")
            continue
        
        # Initialize or reinitialize scraper for each batch
        if scraper is None or (hasattr(scraper, 'error_count') and scraper.error_count > 10):
            if scraper is not None:
                logger.warning("Reinitializing scraper due to high error count")
            scraper = BISScraper()
        
        retry_count = 0
        while retry_count < max_retries:
            try:
                # Process the batch
                scraper.process_bbls_from_csv(
                    input_csv=batch_path,
                    output_csv=os.path.join(output_dir, f'property_data_batch_{batch_num}.csv'),
                    progress_file=progress_file
                )
                
                logger.info(f"Completed batch {batch_num}")
                logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Only wait if we actually processed some BBLs
                if bbls_to_process:
                    logger.info("Waiting 5 minutes before next batch...")
                    time.sleep(300)  # 5 minutes delay
                
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Error processing batch {batch_num} (attempt {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count < max_retries:
                    logger.info("Reinitializing scraper and retrying after 5 minutes...")
                    scraper = BISScraper()  # Create new scraper instance
                    time.sleep(300)  # Wait 5 minutes before retry
                else:
                    logger.error(f"Failed to process batch {batch_num} after {max_retries} attempts")
                    logger.info("Continuing with next batch...")

if __name__ == "__main__":
    try:
        process_batches()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user. Exiting gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise 