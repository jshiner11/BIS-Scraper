import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from typing import List, Dict, Optional
import time
import re
import os
import random
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BISScraper:
    def __init__(self, batch_size: int = 100, save_interval: int = 10):
        """
        Initialize the BIS scraper with specific headers and session configuration.
        
        Args:
            batch_size (int): Number of BBLs to process before saving
            save_interval (int): Number of BBLs to process before showing progress
        """
        self.base_url = "https://a810-bisweb.nyc.gov/bisweb"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://a810-bisweb.nyc.gov/bisweb/bispi00.jsp'
        }
        self.session = self._create_session()
        self.batch_size = batch_size
        self.save_interval = save_interval
        self.start_time = None
        self.processed_count = 0
        self.error_count = 0
        self.last_save_time = None
        self.consecutive_queues = 0
        self.last_request_time = None
        self.min_delay = 1.0  # Minimum delay between requests
        self.max_delay = 3.0  # Maximum delay between requests
        self.session_rotation_interval = 50  # Rotate session every 50 requests

    def _create_session(self) -> requests.Session:
        """Create a new session with randomized headers."""
        session = requests.Session()
        headers = self.headers.copy()
        # Add some variation to the User-Agent
        headers['User-Agent'] = f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_{random.randint(12, 15)}_{random.randint(0, 7)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 120)}.0.{random.randint(1000, 9999)}.{random.randint(100, 999)} Safari/537.36"
        session.headers.update(headers)
        return session

    def _rotate_session(self):
        """Rotate the session to avoid potential session-based rate limiting."""
        self.session = self._create_session()
        logger.info("Rotated session to avoid rate limiting")

    def _calculate_delay(self) -> float:
        """Calculate delay with jitter and progressive backoff."""
        base_delay = self.min_delay
        
        # Add progressive backoff based on consecutive queues
        if self.consecutive_queues > 0:
            base_delay *= (1 + self.consecutive_queues * 0.5)
        
        # Add random jitter
        jitter = random.uniform(0, 0.5)
        delay = min(base_delay + jitter, self.max_delay)
        
        return delay

    def _wait_between_requests(self):
        """Wait between requests with calculated delay."""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay:
                delay = self._calculate_delay()
                logger.debug(f"Waiting {delay:.2f} seconds between requests")
                time.sleep(delay)
        self.last_request_time = time.time()

    def load_progress(self, progress_file: str) -> set:
        """Load already processed BBLs from progress file."""
        processed_bbls = set()
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                for line in f:
                    processed_bbls.add(line.strip())
        return processed_bbls

    def save_progress(self, bbl: str, progress_file: str):
        """Save processed BBL to progress file."""
        with open(progress_file, 'a') as f:
            f.write(f"{bbl}\n")

    def estimate_completion_time(self, total_bbls: int) -> str:
        """Estimate completion time based on current processing rate."""
        if not self.start_time or self.processed_count == 0:
            return "Unknown"
        
        elapsed_time = time.time() - self.start_time
        bbls_per_second = self.processed_count / elapsed_time
        remaining_bbls = total_bbls - self.processed_count
        remaining_seconds = remaining_bbls / bbls_per_second
        
        completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
        return completion_time.strftime("%Y-%m-%d %H:%M:%S")

    def process_bbls_from_csv(self, input_csv: str, output_csv: str = 'property_data.csv', 
                            progress_file: str = 'processed_bbls.txt'):
        """
        Process multiple BBLs from a CSV file and save results to another CSV and text file.
        
        Args:
            input_csv (str): Path to input CSV file containing BBLs
            output_csv (str): Path to output CSV file for results
            progress_file (str): Path to file tracking processed BBLs
        """
        try:
            # Initialize progress tracking
            self.start_time = time.time()
            processed_bbls = self.load_progress(progress_file)
            logger.info(f"Found {len(processed_bbls)} previously processed BBLs")
            
            # Read BBLs from input CSV
            df = pd.read_csv(input_csv, dtype={'BBL': str})
            if 'BBL' not in df.columns:
                raise ValueError("Input CSV must contain a 'BBL' column")
            
            total_bbls = len(df)
            logger.info(f"Found {total_bbls} BBLs to process in {input_csv}")
            
            # Initialize results list
            all_results = []
            current_batch = []
            
            # Open text file in append mode
            with open('property_data.txt', 'a') as txt_file:
                for index, bbl in enumerate(df['BBL'], 1):
                    bbl_str = str(bbl).zfill(10)
                    
                    # Skip if already processed
                    if bbl_str in processed_bbls:
                        logger.info(f"Skipping already processed BBL {index} of {total_bbls}: {bbl_str}")
                        continue
                    
                    try:
                        logger.info(f"Processing BBL {index} of {total_bbls}: {bbl_str}")
                        bbl_components = self.parse_bbl(bbl_str)
                        
                        # Add rate limiting
                        if self.last_save_time:
                            time_since_last = time.time() - self.last_save_time
                            if time_since_last < 1.0:  # Wait at least 1 second between requests
                                time.sleep(1.0 - time_since_last)
                        
                        property_data = self.get_property_profile(
                            borough=bbl_components['borough'],
                            block=bbl_components['block'],
                            lot=bbl_components['lot']
                        )
                        
                        if property_data:
                            current_batch.append(property_data)
                            self.processed_count += 1
                            
                            # Write to text file
                            txt_file.write(f"\nBBL: {bbl_str}\n")
                            txt_file.write("=" * 50 + "\n")
                            txt_file.write(f"Primary Address: {property_data.get('Primary Address', 'N/A')}\n")
                            txt_file.write(f"Secondary Addresses: {property_data.get('Secondary Addresses', 'N/A')}\n")
                            txt_file.write(f"Borough: {property_data.get('Borough', 'N/A')}\n")
                            txt_file.write(f"ZIP Code: {property_data.get('ZIP Code', 'N/A')}\n")
                            txt_file.write("=" * 50 + "\n")
                            
                            # Save progress
                            self.save_progress(bbl_str, progress_file)
                            self.last_save_time = time.time()
                            
                            # Show progress periodically
                            if self.processed_count % self.save_interval == 0:
                                completion_time = self.estimate_completion_time(total_bbls)
                                logger.info(f"Progress: {self.processed_count}/{total_bbls} BBLs processed")
                                logger.info(f"Estimated completion time: {completion_time}")
                                logger.info(f"Success rate: {(self.processed_count/(self.processed_count + self.error_count))*100:.2f}%")
                            
                            # Save batch periodically
                            if len(current_batch) >= self.batch_size:
                                self.save_batch(current_batch, output_csv)
                                current_batch = []
                        else:
                            self.error_count += 1
                            logger.warning(f"No data found for BBL: {bbl_str}")
                            
                    except Exception as e:
                        self.error_count += 1
                        logger.error(f"Error processing BBL {bbl_str}: {str(e)}")
                        continue
            
            # Save any remaining results
            if current_batch:
                self.save_batch(current_batch, output_csv)
            
            # Final statistics
            elapsed_time = time.time() - self.start_time
            logger.info(f"Processing completed in {elapsed_time/3600:.2f} hours")
            logger.info(f"Total BBLs processed: {self.processed_count}")
            logger.info(f"Total errors: {self.error_count}")
            logger.info(f"Success rate: {(self.processed_count/(self.processed_count + self.error_count))*100:.2f}%")
            
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")

    def save_batch(self, batch: List[Dict], output_csv: str):
        """Save a batch of results to the CSV file."""
        try:
            # Convert batch to DataFrame
            batch_df = pd.DataFrame(batch)
            
            # Create BBL column by combining borough, block, and lot
            batch_df['BBL'] = batch_df['Borough Code'].astype(str) + batch_df['Block'].astype(str).str.zfill(5) + batch_df['Lot'].astype(str).str.zfill(4)
            
            # Keep only the columns we want, in the desired order
            columns_to_keep = ['BBL', 'Primary Address', 'Secondary Addresses', 'Borough', 'ZIP Code', 'BIN']
            batch_df = batch_df[columns_to_keep]
            
            # Append to existing CSV or create new one
            if os.path.exists(output_csv):
                batch_df.to_csv(output_csv, mode='a', header=False, index=False)
            else:
                batch_df.to_csv(output_csv, index=False)
            
            logger.info(f"Saved batch of {len(batch)} results to {output_csv}")
        except Exception as e:
            logger.error(f"Error saving batch: {str(e)}")

    def is_queue_page(self, html: str) -> bool:
        """Check if the response is a queue waiting page."""
        return 'Just a moment' in html and 'Your request is being processed' in html

    def fetch_page(self, url: str, max_retries: int = 5, retry_delay: int = 6) -> Optional[str]:
        """
        Fetch a web page and return its content, handling queue system with improved rate limiting.
        
        Args:
            url (str): The URL to fetch
            max_retries (int): Maximum number of retries for queued requests
            retry_delay (int): Base delay in seconds between retries
            
        Returns:
            Optional[str]: The HTML content if successful, None otherwise
        """
        try:
            for attempt in range(max_retries):
                self._wait_between_requests()
                
                # Rotate session periodically
                if self.processed_count % self.session_rotation_interval == 0:
                    self._rotate_session()
                
                logger.info(f"Fetching URL (attempt {attempt + 1}/{max_retries}): {url}")
                response = self.session.get(url)
                response.raise_for_status()
                
                if self.is_queue_page(response.text):
                    self.consecutive_queues += 1
                    logger.info(f"Request is in queue, waiting... (Consecutive queues: {self.consecutive_queues})")
                    
                    # Progressive backoff for queue delays
                    actual_delay = retry_delay * (1 + self.consecutive_queues * 0.5)
                    time.sleep(actual_delay)
                    continue
                
                # Reset consecutive queues counter on success
                self.consecutive_queues = 0
                logger.info(f"Successfully fetched {url}")
                return response.text
                
            logger.error("Max retries reached, still in queue")
            return None
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def clean_text(self, text: str) -> str:
        """Clean and normalize text from HTML."""
        # Remove multiple spaces and newlines
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing spaces
        text = text.strip()
        # Remove special characters
        text = text.replace('\xa0', ' ')
        return text

    def extract_address_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract primary and secondary addresses from the page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Dict containing primary address, secondary addresses, and BIN
        """
        data = {}
        
        # Find primary address, BIN, and borough/ZIP
        maininfo_cells = soup.find_all('td', class_='maininfo')
        for cell in maininfo_cells:
            text = self.clean_text(cell.text)
            if 'BIN#' in text:
                # Extract BIN number
                bin_match = re.search(r'BIN#\s*(\d+)', text)
                if bin_match:
                    data['BIN'] = bin_match.group(1)
            elif any(borough in text for borough in ['MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']):
                # This is the borough and ZIP
                borough_match = re.search(r'(MANHATTAN|BROOKLYN|QUEENS|BRONX|STATEN ISLAND)', text)
                if borough_match:
                    data['Borough'] = borough_match.group(1)
                zip_match = re.search(r'(\d{5})', text)
                if zip_match:
                    data['ZIP Code'] = zip_match.group(1)
            else:
                # This should be the primary address if it's not empty and not the BIN or borough/ZIP
                if text and not any(x in text for x in ['BIN#', 'MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND']):
                    data['Primary Address'] = text
        
        # Find secondary addresses (building number ranges with street names)
        secondary_addresses = []  # Use a list to maintain order
        content_rows = soup.find_all('tr', valign='top')
        for row in content_rows:
            cells = row.find_all('td', class_='content')
            if len(cells) >= 2:  # We need at least the street name and building numbers
                # Skip if this is a cross streets row
                if any(cell.get('colspan') == "4" for cell in cells):
                    continue
                    
                street_name = self.clean_text(cells[0].text)
                building_numbers = self.clean_text(cells[1].text) if len(cells) > 1 else None
                
                # Include only if we have both street name and building numbers
                if (street_name and building_numbers and 
                    not any(x in street_name for x in ['View', 'Browse', 'HPD', 'Number', 'This property', 'OR Enter Action Type', 'OR Select from List']) and
                    not street_name.endswith(':') and
                    not street_name.startswith('Select')):
                    
                    # Clean up the building numbers format
                    building_numbers = re.sub(r'\s*-\s*', '-', building_numbers)
                    # Format as "building_numbers street_name"
                    full_address = f"{building_numbers} {street_name}"
                    if full_address not in secondary_addresses:  # Avoid duplicates while maintaining order
                        secondary_addresses.append(full_address)
        
        if secondary_addresses:
            data['Secondary Addresses'] = ', '.join(secondary_addresses)
        
        return data

    def parse_property_profile(self, html: str) -> Dict:
        """
        Parse the property profile page and extract building information.
        
        Args:
            html (str): The HTML content to parse
            
        Returns:
            Dict: Dictionary containing the extracted property information
        """
        soup = BeautifulSoup(html, 'html.parser')
        data = {}
        
        # Extract address information
        address_info = self.extract_address_info(soup)
        data.update(address_info)
        
        # Find all tables on the page
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables on the page")
        
        # Skip navigation and header tables
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    key = self.clean_text(cols[0].text)
                    value = self.clean_text(cols[1].text)
                    
                    # Skip empty or navigation-related entries
                    if not key or not value or 'BIS Menu' in key or 'Privacy Policy' in key:
                        continue
                    
                    # Clean up the key
                    key = key.replace(':', '')
                    
                    # Skip if this is address-related info we already captured
                    if 'Cross Street' in key or 'View Zoning' in key or 'View Challenge' in key:
                        continue
                    
                    # Store the cleaned data
                    data[key] = value
        
        return data

    def get_property_profile(self, borough: str, block: str, lot: str) -> Optional[Dict]:
        """
        Get the property profile for a specific BBL.
        
        Args:
            borough (str): Borough code (1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island)
            block (str): Block number
            lot (str): Lot number
            
        Returns:
            Optional[Dict]: Property profile information if found, None otherwise
        """
        # Direct URL to property profile
        profile_url = f"{self.base_url}/PropertyProfileOverviewServlet?boro={borough}&block={block}&lot={lot}&go3=+GO+&requestid=0"
        logger.info(f"Accessing property profile for BBL: {borough}-{block}-{lot}")
        
        detail_html = self.fetch_page(profile_url)
        if detail_html:
            # Save the detail page for debugging
            with open('detail_page.html', 'w') as f:
                f.write(detail_html)
            logger.info("Saved detail page to detail_page.html")
            
            data = self.parse_property_profile(detail_html)
            # Add BBL information
            data['Borough Code'] = borough
            data['Block'] = block
            data['Lot'] = lot
            return data
        return None

    def save_data(self, data: Dict, filename: str = 'property_data.csv'):
        """
        Save the property data to a CSV file.
        
        Args:
            data (Dict): The property data to save
            filename (str): The name of the output file
        """
        try:
            # Convert single dictionary to DataFrame
            df = pd.DataFrame([data])
            df.to_csv(filename, index=False)
            logger.info(f"Data saved to {filename}")
            
            # Also save as readable text file
            with open('property_data.txt', 'w') as f:
                # Write BBL information
                f.write("BBL Information:\n")
                for key in ['Borough Code', 'Block', 'Lot']:
                    if key in data:
                        f.write(f"{key}: {data[key]}\n")
                
                # Write BIN
                if 'BIN' in data:
                    f.write(f"BIN: {data['BIN']}\n")
                
                # Write address information
                f.write("\nAddress Information:\n")
                if 'Primary Address' in data:
                    f.write(f"Primary Address: {data['Primary Address']}\n")
                if 'Secondary Addresses' in data:
                    f.write(f"Secondary Addresses: {data['Secondary Addresses']}\n")
                if 'Borough' in data:
                    f.write(f"Borough: {data['Borough']}\n")
                if 'ZIP Code' in data:
                    f.write(f"ZIP Code: {data['ZIP Code']}\n")
                
                f.write("\nBuilding Information:\n")
                # Write the rest of the information
                skip_keys = ['Borough Code', 'Block', 'Lot', 'BIN', 'Primary Address', 
                           'Secondary Addresses', 'Borough', 'ZIP Code']
                for key, value in data.items():
                    if key not in skip_keys:
                        f.write(f"{key}: {value}\n")
            logger.info("Data also saved to property_data.txt")
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")

    def parse_bbl(self, bbl: str) -> Dict[str, str]:
        """
        Parse a 10-digit BBL string into its components.
        
        Args:
            bbl (str): 10-digit BBL string (1 digit borough + 5 digits block + 4 digits lot)
            
        Returns:
            Dict containing borough, block, and lot
        """
        if len(bbl) != 10:
            raise ValueError("BBL must be 10 digits long")
            
        return {
            'borough': bbl[0],
            'block': bbl[1:6],
            'lot': bbl[6:10]
        }

def main():
    scraper = BISScraper()
    
    # Process BBLs from input CSV
    input_csv = 'data/input/input_bbls.csv'  # CSV file containing BBLs
    output_csv = 'data/output/property_data.csv'
    progress_file = 'data/output/processed_bbls.txt'
    
    scraper.process_bbls_from_csv(input_csv, output_csv, progress_file)

if __name__ == "__main__":
    main() 