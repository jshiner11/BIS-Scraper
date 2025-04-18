import os
from scraper import BISScraper
import time
from datetime import datetime
import pandas as pd
import logging
import sys
from logging.handlers import RotatingFileHandler

# Get the absolute path to the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ensure logs directory exists
log_dir = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(log_dir, exist_ok=True)

# Create logger
logger = logging.getLogger('batch_processor')
logger.setLevel(logging.INFO)

# Remove any existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Configure file handler
log_file = os.path.join(log_dir, 'batch_processing.log')
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    mode='a',  # Append mode
    encoding='utf-8'
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Configure console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Prevent propagation to root logger to avoid duplicate messages
logger.propagate = False

def process_batches(batch_dir: str = None, output_dir: str = None):
    """
    Process BBL batches sequentially.
    
    Args:
        batch_dir (str): Directory containing batch CSV files
        output_dir (str): Directory to save output files
    """
    # Add a clear separator for new runs
    logger.info("="*50)
    logger.info(f"NEW PROCESSING RUN - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*50)
    
    logger.info("Starting batch processing script")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Log directory: {log_dir}")
    
    # Set default directories if not provided
    if batch_dir is None:
        batch_dir = os.path.join(PROJECT_ROOT, 'data', 'input', 'batches')
    if output_dir is None:
        output_dir = os.path.join(PROJECT_ROOT, 'data', 'output')
    
    logger.info(f"Batch directory: {batch_dir}")
    logger.info(f"Output directory: {output_dir}")
    
    # Ensure directories exist
    os.makedirs(batch_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of batch files
    batch_files = [f for f in os.listdir(batch_dir) if f.startswith('batch_') and f.endswith('.csv')]
    batch_files.sort()  # Process in order
    
    if not batch_files:
        logger.warning(f"No batch files found in {batch_dir}")
        return
    
    logger.info(f"Found {len(batch_files)} batch files to process")
    
    # Initialize scraper
    scraper = BISScraper()
    
    # Process each batch
    for batch_file in batch_files:
        try:
            # Extract batch number from filename
            batch_num = int(batch_file.split('_')[1].split('.')[0])
            
            logger.info(f"\nProcessing batch {batch_num} from {batch_file}")
            logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Check if batch is already fully processed
            progress_file = os.path.join(output_dir, f'processed_bbls_batch_{batch_num}.txt')
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    processed_bbls = set(f.read().splitlines())
                
                batch_path = os.path.join(batch_dir, batch_file)
                df = pd.read_csv(batch_path)
                total_bbls = len(df)
                
                if len(processed_bbls) >= total_bbls:
                    logger.info(f"Batch {batch_num} is already fully processed, continuing to next batch...")
                    continue
            
            # Process the batch
            batch_path = os.path.join(batch_dir, batch_file)
            output_csv = os.path.join(output_dir, f'property_data_batch_{batch_num}.csv')
            
            # Initialize retry counter
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    # Reinitialize scraper if we've had too many errors
                    if retry_count > 0:
                        logger.warning("Reinitializing scraper due to high error count")
                        scraper = BISScraper()
                        time.sleep(300)  # Wait 5 minutes before retrying
                    
                    # Process the batch
                    scraper.process_csv(
                        input_csv=batch_path,
                        output_csv=output_csv,
                        progress_file=progress_file
                    )
                    
                    logger.info(f"Completed batch {batch_num}")
                    logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    logger.error(f"Error processing batch {batch_num} (attempt {retry_count}/{max_retries}): {str(e)}")
                    
                    if retry_count < max_retries:
                        logger.info("Reinitializing scraper and retrying after 5 minutes...")
                        time.sleep(300)  # Wait 5 minutes before retrying
                    else:
                        logger.error(f"Failed to process batch {batch_num} after {max_retries} attempts")
                        logger.info("Continuing with next batch...")
            
            # Wait 5 minutes between batches to avoid rate limiting
            logger.info("Waiting 5 minutes before next batch...")
            time.sleep(300)
            
        except KeyboardInterrupt:
            logger.info("\nScript interrupted by user. Exiting gracefully...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            continue

if __name__ == "__main__":
    process_batches() 