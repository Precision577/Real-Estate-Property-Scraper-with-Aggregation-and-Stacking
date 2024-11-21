import os
import pandas as pd
import pgeocode
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from functools import partial  # Import partial from functools

def setup_logging():
    """Sets up logging with rotation and console output."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_directory = os.path.join("logs", current_date)
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, "aggeratepropwireandprepareforskiptracing.py")

    # Remove the old log file if it exists
    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    # Setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Log format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # File handler setup
    file_handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=3)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)  # INFO and above goes to the log file
    logger.addHandler(file_handler)

    # Console handler setup
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)  # DEBUG and above to console
    logger.addHandler(console_handler)

    # Specific log levels for verbose libraries
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger


def normalize_address(address):
    """Normalize addresses for better matching accuracy."""
    return ''.join(e for e in address if e.isalnum()).lower()

def get_county_by_zip(zip_code):
    """Determine the county for a given ZIP code using pgeocode."""
    nomi = pgeocode.Nominatim('us')
    county = nomi.query_postal_code(zip_code).county_name
    return county if county else 'Unknown'

def process_chunk(chunk, propwire_dict, propwire_cols):
    """Process a chunk of storage dataframe by appending relevant propwire data."""
    chunk['Address_normalized'] = chunk['address'].apply(normalize_address)
    chunk['County'] = chunk['zip_code'].apply(get_county_by_zip)  # Changed from 'zip' to 'zip_code'

    for index, row in chunk.iterrows():
        county_data = propwire_dict.get(row['County'], {})
        propwire_info = county_data.get(row['Address_normalized'])
        if propwire_info:
            for col in propwire_cols:
                chunk.at[index, col] = propwire_info.get(col, pd.NA)
    return chunk


def merge_data(storage_file, propwire_dict, propwire_cols):
    """Merge data from storage files with propwire dictionary using multiprocessing."""
    logger = logging.getLogger(__name__)
    try:
        chunks = pd.read_csv(storage_file, chunksize=10000, low_memory=False)
        with Pool(processes=4) as pool:
            results = pool.map(partial(process_chunk, propwire_dict=propwire_dict, propwire_cols=propwire_cols), chunks)
        return pd.concat(results, ignore_index=True)
    except Exception as e:
        logger.error(f"Error processing file {storage_file}: {e}")
        return pd.DataFrame()


def load_propwire_data(file_path):
    """Load individual propwire data file with specified data types."""
    logger = logging.getLogger(__name__)
    try:
        df = pd.read_csv(file_path, low_memory=False)
        # Assuming 'County' is a column in your CSV
        county_name = df['County'].iloc[0].strip()
        df['Address_normalized'] = df['Address'].apply(normalize_address)
        df.drop_duplicates(subset='Address_normalized', keep='last', inplace=True)
        return county_name, df.set_index('Address_normalized').to_dict('index')
    except Exception as e:
        logger.error(f"Failed to load or process {file_path}: {e}")
        return None, {}



def main(storage_dir, propwire_dir):
    logger = logging.getLogger(__name__)
    try:
        propwire_files = [os.path.join(propwire_dir, f) for f in os.listdir(propwire_dir) if f.endswith('.csv')]
        if not propwire_files:
            logger.error("No CSV files found in propwire directory.")
            return

        propwire_dict = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(load_propwire_data, f): f for f in propwire_files}
            for future in futures:
                county_name, data = future.result()
                if county_name and data:
                    propwire_dict[county_name] = data
                else:
                    logger.error(f"Failed to load or process {futures[future]}")

        storage_files = [os.path.join(storage_dir, f) for f in os.listdir(storage_dir) if f.endswith('.csv') and f != 'Total_URLs.csv']
        if not storage_files:
            logger.error("No CSV files found in storage directory.")
            return

        propwire_cols = ['Living Square Feet', 'Year Built', 'Lot (Acres)', 'Lot (Square Feet)', 'Bedrooms',
                         'Bathrooms', '# of Stories', 'Owner 1 First Name', 'Owner 1 Last Name', 'Owner Mailing Address',
                         'Owner Mailing City', 'Owner Mailing State', 'Owner Mailing Zip', 'Owner Type', 'Owner Occupied',
                         'Vacant?', 'Last Sale Date', 'Last Sale Amount', 'Estimated Value', 'Estimated Equity',
                         'Equity Percent', 'Open Mortgage Balance', 'Mortgage Interest Rate', 'Mortgage Loan Type',
                         'Lender Name', 'Deed Type', 'Tax Amount', 'Assessment Year', 'Assessed Total Value',
                         'Assessed Land Value', 'Assessed Improvement Value', 'Market Value', 'Market Land Value',
                         'Market Improvement Value', 'Status', 'Default Amount', 'Opening bid', 'Recording Date',
                         'Auction Date']

        with Pool(processes=4) as pool:
            for storage_file in storage_files:
                logger.info(f"Processing file: {storage_file}")
                chunks = pd.read_csv(storage_file, chunksize=10000)
                results = pool.map(partial(process_chunk, propwire_dict=propwire_dict, propwire_cols=propwire_cols), chunks)
                final_df = pd.concat(results, ignore_index=True)
                final_df.to_csv(storage_file, index=False)
                logger.info(f"Data merged and saved successfully for {storage_file}")
    except Exception as e:
        logger.error(f"An error occurred during the merging process: {str(e)}")

if __name__ == "__main__":
    logger = setup_logging()
    cwd = os.getcwd()
    storage_dir = os.path.join(cwd, 'storage')
    propwire_dir = os.path.join(cwd, 'propwire_county_data')
    main(storage_dir, propwire_dir)

