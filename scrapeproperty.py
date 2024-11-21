

import os
import csv
import time
import signal
import re
import sys
import gc
import json
import shutil
import subprocess
import sqlite3
import multiprocessing
from datetime import datetime
from collections import defaultdict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from asyncio.exceptions import TimeoutError
from requests.exceptions import RequestException


import asyncio
import logging
from logging.handlers import RotatingFileHandler
from io import BytesIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

from pythonjsonlogger import jsonlogger
from aiologger.loggers.json import JsonLogger
from aiologger.handlers.files import AsyncFileHandler
from aiologger.formatters.json import JsonFormatter
from aiologger import Logger


from textblob import TextBlob
from PIL import Image
from tqdm import tqdm
from requests.exceptions import ConnectionError
import requests
import urllib3
from urllib3.exceptions import MaxRetryError
from rapidfuzz import fuzz, process

import pytesseract
import backoff


pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

shutdown_requested = False


# Configuration for headless mode
HEADLESS_MODE = True  # Set to False to run with a GUI


# Track occurrences of phone numbers
phone_number_counts = defaultdict(int)
blacklisted_numbers = set()

# Define the load_config function
def load_config():
    config_path = 'config.json'
    default_config = {
        'MAX_TABS': 10,
        'MAX_CONCURRENCY': 10,
        'LOG_DIRECTORY': 'logs',
        'keywords': {},
        'false_positive_phrases': [],
        'negating_phrases': []
    }

    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        
        for key in ['MAX_TABS', 'MAX_CONCURRENCY', 'LOG_DIRECTORY', 'false_positive_phrases', 'negating_phrases', 'keywords']:
            if key not in config or not isinstance(config[key], type(default_config[key])):
                raise ValueError(f"{key} is missing or is not of type {type(default_config[key]).__name__}.")

        return {**default_config, **config}  # Merge defaults with loaded config

    except FileNotFoundError:
        print(f"Configuration file '{config_path}' not found. Using default settings.")
        return default_config
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Configuration validation error: {e}")
        sys.exit(1)






async def setup_async_logger():
    logger = JsonLogger.with_default_handlers(level=logging.DEBUG, formatter=JsonFormatter())
    file_handler = AsyncFileHandler('async_log.json')
    logger.add_handler(file_handler)
    return logger

def find_logs_directory():
    cwd = os.getcwd()
    for root, dirs, files in os.walk(cwd):
        if 'logs' in dirs:
            return os.path.join(root, 'logs')
    return None


class ExcludeUnwantedLogsFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        unwanted_patterns = [
            "GET /session/", "POST /session/", "Remote response",
            "Finished Request", "status=200", "http://localhost"
        ]
        return not any(pattern in message for pattern in unwanted_patterns)


class DuplicateFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.logged_messages = set()

    def filter(self, record):
        log_key = (record.levelno, record.msg)
        if log_key in self.logged_messages:
            return False
        else:
            self.logged_messages.add(log_key)
            return True

class CustomFormatter(logging.Formatter):
    """ Custom formatter to add spacing and structure to log files. """
    def format(self, record):
        if record.levelno == logging.INFO:
            separator = "=" * 70
            return f"\n{separator}\n{super().format(record)}\n{separator}\n"
        elif record.levelno == logging.ERROR:
            return f"!!! ERROR !!!\n{super().format(record)}\n!!! ERROR !!!\n"
        elif record.levelno == logging.DEBUG:
            # Simplify debug logs to show only useful information
            useful_info = re.sub(r"DEBUG:.* (GET|POST|http://localhost).*", "", record.getMessage())
            return f"DEBUG: {useful_info}" if useful_info else ""
        return super().format(record)


def setup_logging(debug_mode=False):
    log_directory = "logs/" + datetime.now().strftime('%Y-%m-%d')
    os.makedirs(log_directory, exist_ok=True)

    log_file_path = os.path.join(log_directory, "scrapeproperty.log")
    file_handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=3)
    console_handler = logging.StreamHandler()

    if debug_mode:
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
    else:
        file_handler.setLevel(logging.INFO)
        console_handler.setLevel(logging.INFO)
    
    formatter = CustomFormatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    if not debug_mode:
        # Add duplicate filter to avoid logging duplicate messages
        duplicate_filter = DuplicateFilter()
        logger.addFilter(duplicate_filter)

        # Add custom filter to exclude unwanted logs
        exclude_unwanted_logs_filter = ExcludeUnwantedLogsFilter()
        logger.addFilter(exclude_unwanted_logs_filter)

    return logger





def log_message(logger, message, level="INFO"):
    if level == "INFO":
        logger.info(message)
    elif level == "DEBUG":
        logger.debug(message)
    elif level == "WARNING":
        logger.warning(message)
    elif level == "ERROR":
        logger.error(message)
    elif level == "CRITICAL":
        logger.critical(message)

def log_critical_operation(logger, operation_func, *args, **kwargs):
    start_time = datetime.now()
    logger.info(f"Starting operation {operation_func.__name__} with args: {args}, kwargs: {kwargs}")
    try:
        result = operation_func(*args, **kwargs)
        logger.info(f"Completed operation {operation_func.__name__} successfully with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error during operation {operation_func.__name__}: {str(e)}", exc_info=True)
    finally:
        elapsed_time = datetime.now() - start_time
        logger.info(f"Operation {operation_func.__name__} took {elapsed_time.total_seconds()} seconds")

class LessThanFilter(logging.Filter):
    def __init__(self, exclusive_maximum, name=""):
        super().__init__(name)
        self.max_level = exclusive_maximum

    def filter(self, record):
        return record.levelno < self.max_level

def log_url_counts(logger, db_connection):
    table_to_be_processed = "to_be_processed"
    table_processed = "processed"
    table_missing = "missing"
    with db_connection as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_to_be_processed}")
        to_be_processed_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_processed}")
        processed_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_missing}")
        missing_count = cursor.fetchone()[0]

        logger.info(f"To be processed URLs: {to_be_processed_count}, Processed URLs: {processed_count}, Missing URLs: {missing_count}")






def periodic_log_counters():
    logger = logging.getLogger('periodic_counters')
    logger.info(f"Total phone numbers extracted: {sum(phone_number_counts.values())}")
    logger.info(f"Total unique phone numbers: {len(phone_number_counts)}")


def safe_webdriver_command(driver, command, *args, retries=3, sleep_interval=2, **kwargs):
    logger = logging.getLogger('webdriver')
    for attempt in range(retries):
        try:
            result = command(*args, **kwargs)
            
            # Add condition to exclude logging screenshot responses
            if command.__name__ == "get_screenshot_as_base64":
                logger.debug(f"WebDriver command executed successfully on attempt {attempt + 1}")
            else:
                logger.debug(f"WebDriver command executed successfully on attempt {attempt + 1} with response: {result}")
                
            return result
        except WebDriverException as e:
            logger.warning(f"WebDriver command failed on attempt {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(sleep_interval)
            else:
                logger.error("Maximum retry attempts reached, operation failed.", exc_info=True)
                raise



def signal_handler(sig, frame):
    global shutdown_requested
    shutdown_requested = True
    print('Shutdown signal received! Preparing to shut down...')


def set_firefox_options():
    options = webdriver.FirefoxOptions()
    if HEADLESS_MODE:
        options.add_argument('--headless')  # Enable headless mode if HEADLESS_MODE is True
    options.set_preference('permissions.default.image', 2)
    options.set_preference('permissions.default.stylesheet', 2)
    options.set_capability('pageLoadStrategy', 'eager')
    return options


# Modify the call in initialize_driver function
def initialize_single_driver(options):
    try:
        driver = webdriver.Firefox(options=options)
        driver.get("https://www.redfin.com")
        load_cookies(driver, "redfin_cookies.txt")
        driver.refresh()
        driver.set_page_load_timeout(2)
        return driver
    except Exception as e:
        print(f"Failed to initialize a driver: {e}")
        return None


async def setup_drivers_async(count, retry_attempts=3):
    options = set_firefox_options()
    loop = asyncio.get_event_loop()
    drivers = []

    while retry_attempts > 0:
        try:
            with ThreadPoolExecutor(max_workers=count) as executor:
                futures = [loop.run_in_executor(executor, initialize_single_driver, options) for _ in range(count)]
                results = await asyncio.gather(*futures, return_exceptions=True)
                drivers = [driver for driver in results if isinstance(driver, webdriver.Firefox)]
            
            if len(drivers) == count:
                print("All drivers initialized successfully.")
                return drivers
            else:
                raise Exception(f"Only {len(drivers)} out of {count} drivers were initialized successfully.")

        except Exception as e:
            print(f"Attempt {4 - retry_attempts} failed: {e}")
            retry_attempts -= 1
            if retry_attempts > 0:
                print("Retrying to initialize drivers...")
                time.sleep(2)  # Sleep before retrying
            else:
                print("Failed to initialize drivers after multiple attempts.")
                return None

    return drivers


def load_cookies(driver, path_to_cookie_file):
    logger = logging.getLogger('webdriver.cookies')
    try:
        driver.get("https://www.redfin.com")  # Ensure driver is on a domain that matches the cookies.
        cookies = []
        with open(path_to_cookie_file, 'r') as cookies_file:
            for line in cookies_file:
                if not line.startswith('#') and line.strip():
                    parts = line.strip().split('\t')
                    if len(parts) == 7:
                        cookie = {
                            'domain': parts[0],
                            'httpOnly': parts[1] == 'TRUE',
                            'path': parts[2],
                            'secure': parts[3] == 'TRUE',
                            'expiry': int(parts[4]) if parts[4] != '0' else None,
                            'name': parts[5],
                            'value': parts[6]
                        }
                        cookies.append(cookie)
        for cookie in cookies:
            if 'expiry' in cookie:
                driver.add_cookie(cookie)
        logger.info("All cookies loaded successfully.")
    except FileNotFoundError:
        logger.error(f"Cookie file not found at {path_to_cookie_file}", exc_info=True)
    except Exception as e:
        logger.error(f"Error loading cookies: {e}", exc_info=True)





def kill_firefox():
    try:
        if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
            subprocess.run(['pkill', 'firefox'], check=True)
        elif sys.platform.startswith('win'):
            subprocess.run(['taskkill', '/IM', 'firefox.exe', '/F'], check=True)
    except subprocess.CalledProcessError as e:
        print("Failed to kill Firefox processes:", e)

def worker_process(url_queue, driver_id):
    driver = setup_driver(driver_id)  # Setup a separate driver for each worker
    try:
        while not url_queue.empty():
            url = url_queue.get()
            process_page(driver, url)
    finally:
        driver.quit()


def distribute_urls_to_drivers(urls):
    url_queue = multiprocessing.Queue()
    for url in urls:
        url_queue.put(url)
    processes = []
    for _ in range(config['MAX_TABS']):  # Using MAX_TABS from config
        p = multiprocessing.Process(target=worker_process, args=(url_queue,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

def close_all_drivers(drivers=None):
    if drivers:
        for driver in drivers:
            try:
                driver.quit()
            except Exception as e:
                print(f"Failed to close driver properly: {e}")
    gc.collect()


def append_to_csv(data, filename, fieldnames):
    """ Append data to a CSV file or create it if it doesn't exist, avoiding duplicates. """
    fieldnames.append('Motivated')  # Add Motivated column to the fieldnames
    csv_path = os.path.join('storage', f"{filename}.csv")
    file_exists = os.path.isfile(csv_path)
    existing_data = set()

    if file_exists:
        with open(csv_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                unique_key = f"{row['address']}_{row['zip_code']}_{row['price']}"
                existing_data.add(unique_key)

    with open(csv_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for item in data:
            unique_key = f"{item['address']}_{item['zip_code']}_{item['price']}"
            if unique_key not in existing_data:
                writer.writerow(item)
                existing_data.add(unique_key)
                print(f"Data for {unique_key} added.")
            else:
                print(f"Duplicate data for {unique_key} not added.")

    print("Data appended to CSV successfully.")


def combine_text_files_to_csv(storage_folder, output_filename):
    fieldnames = [
        'address', 'city', 'state', 'zip_code', 'price', 'rent', 'hoa_fee', 
        'property_taxes', 'insurance', 'agent_name', 'phone_number', 'Motivated', 
        'Trigger Keyword', 'Trigger Sentence', 'Trigger Context', 'Redfin Link'
    ]
    csv_path = os.path.join(storage_folder, f"{output_filename}.csv")
    file_exists = os.path.isfile(csv_path)

    with open(csv_path, 'a', newline='') as csvfile:  # 'a' mode for appending data
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()  # Write header only if file does not exist

        # Collect all text file paths for deletion later
        text_files = []

        for filename in os.listdir(storage_folder):
            if filename.endswith('.txt'):
                path = os.path.join(storage_folder, filename)
                with open(path, 'r') as file:
                    details = {}
                    for line in file:
                        try:
                            key, value = line.strip().split(': ', 1)
                            details[key] = value
                        except ValueError:
                            print(f"Skipping line in {filename} due to format error: {line.strip()}")
                    writer.writerow(details)
                text_files.append(path)
    
    # Delete text files after CSV has been created
    for file_path in text_files:
        os.remove(file_path)
    
    print(f"Combined CSV updated and text files deleted at: {csv_path}")








def read_unique_locations(csv_path):
    locations = set()
    try:
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                locations.add(row['Location'].replace(" ", "_").replace(",", "_"))  # Format as valid table name
    except FileNotFoundError:
        print(f"No file found at {csv_path}. Starting without locations.")
    except Exception as e:
        print(f"An error occurred while reading locations: {e}")
    return locations


def chunk_urls(urls, batch_size=5):
    """Yield successive n-sized chunks from the list of URLs."""
    for i in range(0, len(urls), batch_size):
        yield urls[i:i + batch_size]


@contextmanager
def get_db_connection():
    db_logger = logging.getLogger('db')
    try:
        conn = sqlite3.connect('processed_urls.db')
        db_logger.debug("Database connection opened.")
        yield conn
    except sqlite3.DatabaseError as e:
        db_logger.error("Failed to connect to the database.", exc_info=True)
        raise e
    finally:
        conn.close()
        db_logger.debug("Database connection closed.")

def sanitize_name(name):
    """ Sanitize the location name to be used as a valid SQL table name """
    return re.sub(r'\W+', '_', name)


def initialize_db():
    logger = logging.getLogger('database')
    logger.info("Initializing the database...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            tables_to_create = {
                "processed": "url TEXT PRIMARY KEY, processed_date TEXT",
                "missing": "url TEXT PRIMARY KEY, last_seen_date TEXT",
                "to_be_processed": "url TEXT PRIMARY KEY, status TEXT, attempts INTEGER DEFAULT 0, last_attempt TIMESTAMP"
            }
            for table_name, columns in tables_to_create.items():
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
            conn.commit()
            logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)




def get_processed_urls_count():
    """Retrieve the current count of processed URLs."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed")
        count = cursor.fetchone()[0]
        logging.getLogger('db').debug(f"Processed URLs count: {count}")
        return count




def load_unprocessed_urls():
    logger = logging.getLogger(__name__)
    logger.debug("Loading unprocessed URLs")
    query = """
    SELECT url FROM to_be_processed
    WHERE url NOT IN (SELECT url FROM processed)
    """
    urls = []
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            urls = [row[0] for row in cursor.fetchall()]
            logger.info(f"Loaded {len(urls)} unprocessed URLs.")
    except sqlite3.DatabaseError as e:
        logger.error("Database error during loading unprocessed URLs: ", exc_info=True)
    except Exception as e:
        logger.exception("Unexpected error during loading unprocessed URLs: ", exc_info=True)
    return urls


async def monitor_progress(total_urls, logger):
    """Monitors the progress of URL processing and updates a tqdm progress bar."""
    processed_logger = logger  # Use the passed logger
    pbar = tqdm(total=total_urls, desc="Processing URLs", unit="url")
    processed_count = 0

    while processed_count < total_urls:
        # Refresh the count of processed URLs
        current_processed = get_processed_urls_count()
        pbar.update(current_processed - processed_count)
        processed_count = current_processed
        # Log current progress or other information if needed
        processed_logger.info(f"Processed {processed_count} of {total_urls} URLs")
        # Wait a bit before checking again
        await asyncio.sleep(10)  # Check every 10 seconds

    pbar.close()
    processed_logger.info("URL processing completed.")




def process_url_batch(url_list, storage_folder, output_filename):
    db_logger = logging.getLogger('db')

    # Simple queries to move URLs between tables
    insert_query = "INSERT OR IGNORE INTO processed (url, processed_date) VALUES (?, ?)"
    delete_query = "DELETE FROM to_be_processed WHERE url = ?"

    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            for url in url_list:
                # Log the URL being processed
                db_logger.debug(f"Processing URL: {url}")

                # Insert URL into the processed table
                cursor.execute(insert_query, (url, datetime.now().strftime('%Y-%m-%d')))
                db_logger.debug(f"Inserted URL into processed table: {url}")

                # Delete URL from the to_be_processed table
                cursor.execute(delete_query, (url,))
                db_logger.debug(f"Deleted URL from to_be_processed table: {url}")

            conn.commit()
            db_logger.info(f"Batch processed successfully, URLs processed: {len(url_list)}")

            # Combine text files to CSV after processing each batch
            combine_text_files_to_csv(storage_folder, output_filename)

        except sqlite3.DatabaseError as e:
            conn.rollback()
            db_logger.error("Database error during batch processing: ", exc_info=True)
        except Exception as e:
            conn.rollback()
            db_logger.exception("Unexpected error during batch processing: ", exc_info=True)








def delete_failed_urls(urls):
    db_logger = logging.getLogger('db')
    delete_query = "DELETE FROM to_be_processed WHERE url = ?"

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(delete_query, [(url,) for url in urls])
            conn.commit()
            db_logger.info(f"Failed URLs deleted successfully, URLs deleted: {len(urls)}")
    except sqlite3.DatabaseError as e:
        conn.rollback()
        db_logger.error("Database error during deletion of failed URLs: ", exc_info=True)
        raise
    except Exception as e:
        conn.rollback()
        db_logger.exception("Unexpected error during deletion of failed URLs: ", exc_info=True)
        raise


async def async_scrape_data(driver, url, logger, config):
    try:
        driver.get(url)
        # Wait for page elements to ensure page has loaded correctly
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-rf-test-id='abp-streetLine']"))
        )
        logger.info(f"Successfully navigated to home details page: {url}")

        street_address = extract_address(driver)
        if street_address and street_address.lower() != "undisclosed address":
            details_saved = extract_and_save_home_details(driver, url, config)
            # Check success and presence of a phone number
            if details_saved.get('success') and details_saved.get('phone_number', 'N/A') != 'N/A':
                return (url, True)  # Success and phone number found
            else:
                logger.warning(f"Phone number not found for URL: {url}")
                return (url, True)  # Success but no phone number
        else:
            logger.warning(f"Street address not found or is 'undisclosed address' for URL: {url}")
            return (url, True)  # Not a failure, but data might be incomplete

    except TimeoutException as e:
        logger.error(f"TimeoutException when processing URL: {url}, error: {e}")
        return (url, False)  # True failure, retry might be needed
    except WebDriverException as e:
        logger.error(f"WebDriverException when processing URL: {url}, error: {e}")
        return (url, False)  # True failure, retry might be needed
    except Exception as e:
        logger.exception(f"Unexpected error occurred while processing URL: {url}, error: {e}")
        return (url, False)  # True failure, retry might be needed



async def handle_network_issues():
    if not check_network_connection():
        print("Network appears to be down. Waiting for restoration...")
        await wait_for_network_to_be_restored()
        print("Network restored.")

async def ensure_network_and_recover(drivers, logger):
    while not check_internet_connection():
        logger.warning("Awaiting internet restoration...")
        await asyncio.sleep(5)  # Check every 5 seconds for internet restoration

    logger.info("Internet restored. Restarting drivers...")
    await restart_all_drivers(drivers, logger)


async def monitor_internet_connection():
    while True:
        if not check_internet_connection():
            logger.warning("Internet connection lost. Pausing all operations...")
            await handle_network_issues()
        await asyncio.sleep(10)  # Check connection every 10 seconds





def save_all_processed_urls(url_list, storage_folder, output_filename):
    logger = logging.getLogger('save_all_processed_urls')
    batch_size = 5
    max_retries = 3
    retry_delay = 2
    batches = [url_list[i:i + batch_size] for i in range(0, len(url_list), batch_size)]

    for batch in batches:
        retry_count = 0
        while retry_count < max_retries:
            try:
                process_url_batch(batch, storage_folder, output_filename)  # Pass the necessary arguments
                logger.info(f"Processed a batch of {len(batch)} URLs successfully.")
                break
            except Exception as e:
                retry_count += 1
                logger.warning(f"Retry {retry_count} for batch due to error: {e}")
                if retry_count >= max_retries:
                    logger.error("Max retries reached, moving to next batch or handling failure.")
                time.sleep(retry_delay)


def check_internet_connection():
    try:
        # Ping Google's public DNS server to check for internet connectivity
        subprocess.check_output(["ping", "-c", "1", "8.8.8.8"], stderr=subprocess.STDOUT, universal_newlines=True)
        return True
    except subprocess.CalledProcessError:
        return False


async def wait_for_network_to_be_restored():
    while not check_internet_connection():
        print("Waiting for internet connection to be restored...")
        await asyncio.sleep(5)  # Check every 5 seconds
    print("Internet connection restored.")



async def run_scraping(drivers, urls, options, logger, config, storage_folder, output_filename):
    processed_urls = set()
    failed_urls = set()
    driver_queue = asyncio.Queue()
    consecutive_failures = 0
    max_consecutive_failures = 10  # Consider this for individual failures
    batch_failure_threshold = len(urls)  # Threshold for considering a batch as failed

    # Populate the driver queue with available drivers
    for driver in drivers:
        await driver_queue.put(driver)

    async def scrape(url):
        nonlocal consecutive_failures
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            driver = await driver_queue.get()
            try:
                result, success = await asyncio.wait_for(async_scrape_data(driver, url, logger, config), timeout=4)
                if success:
                    processed_urls.add(url)
                    logger.debug(f"URL processed successfully: {url}")
                    consecutive_failures = 0  # Reset on success
                    await driver_queue.put(driver)
                    break
                else:
                    retry_count += 1
            except TimeoutError as e:
                logger.warning(f"Timeout occurred while processing URL: {url}", exc_info=True)
                retry_count += 1
            except Exception as e:
                logger.error(f"Exception occurred while processing URL: {url}, Error: {e}", exc_info=True)
                retry_count += 1
            finally:
                if driver_queue.qsize() < len(drivers):  # Ensure driver is returned if not crashed
                    await driver_queue.put(driver)

            if retry_count == max_retries:
                failed_urls.add(url)
                consecutive_failures += 1

            # Check batch failure condition
            if len(failed_urls) >= batch_failure_threshold:
                logger.warning("Entire batch failed. Restarting drivers and retrying batch...")
                await ensure_network_and_recover(drivers, logger)
                failed_urls.clear()  # Clear failed URLs to retry batch
                await asyncio.sleep(10)  # Delay before retrying the whole batch

    # Create tasks for all URLs and run them concurrently
    tasks = [scrape(url) for url in urls]
    await asyncio.gather(*tasks)

    # Log results and handle processed URLs
    logger.info(f"Total URLs processed: {len(processed_urls)}, Failed: {len(failed_urls)}")
    save_all_processed_urls(list(processed_urls), storage_folder, output_filename)  # Save successful URLs
    save_all_processed_urls(list(failed_urls), storage_folder, output_filename)  # Optionally save failed URLs for review
    if failed_urls:
        logger.warning(f"Failed URLs need review: {len(failed_urls)}")

async def restart_all_drivers(drivers, logger):
    logger.info("Cleaning up and exiting...")
    for driver in drivers:
        try:
            driver.quit()
        except Exception as e:
            logger.error(f"Failed to quit driver: {e}", exc_info=True)
    
    drivers.clear()  # Clear the list to ensure no dead drivers remain

    # Exiting the script
    sys.exit(0)  # Exit the script with a success status code


async def handle_critical_error(e, logger):
    """
    Handles critical errors by logging, performing cleanup, and then exiting.
    """
    await logger.error("A critical network error occurred:", exc_info=True)
    close_all_drivers(drivers)  # Make sure drivers are closed properly
    sys.exit(1)  # Exit the script with an error code



def process_property_in_new_driver(url, already_processed_urls, main_window_handle, driver):
    try:
        driver.execute_script("window.open('');")
        new_window_handle = driver.window_handles[-1]
        driver.switch_to.window(new_window_handle)
        driver.get(url)

        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-rf-test-id='abp-streetLine']"))
        )
        print(f"Navigating to home details page: {url}")

        street_address = extract_address(driver)
        if street_address:
            print(f"Extracted address: {street_address}")
            success = extract_and_save_home_details(driver, url)
            if success:
                already_processed_urls.add(url)
                print(f"Scraped and saved details for: {street_address}")
            else:
                print("Details not saved due to criteria.")
        else:
            print("Failed to extract address.")
    except Exception as e:
        print(f"Error processing property in new driver: {e}")
    finally:
        driver.close()
        driver.switch_to.window(main_window_handle)


def extract_and_save_home_details(driver, url, config):
    logger = logging.getLogger('extract_and_save_home_details')
    logger.info(f"Accessing URL: {url}")
    result = {'success': False}

    try:
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-rf-test-id='abp-streetLine']"))
        )
        logger.info("Primary address element is visible.")

        address_element = driver.find_element(By.CSS_SELECTOR, "div[data-rf-test-id='abp-streetLine']")
        full_address = address_element.text.strip()
        undisclosed_check = full_address.lower()

        if not re.search(r'\d', full_address):
            logger.warning(f"Skipping address as it appears to be invalid or non-typical: {full_address}")
            return result

        if not full_address or "undisclosed address" in undisclosed_check:
            logger.error("Invalid or undisclosed address, aborting save operation.")
            return result

        city, state, zip_code = extract_city_state_zip(driver)
        price = extract_price(driver)
        phone_number = extract_phone_number(driver)

        # Extract financial details using OCR
        financial_details = extract_financial_details_from_calculator(driver, logger)

        hoa_fee = financial_details.get("HOA dues", "N/A")
        property_taxes = financial_details.get("Property taxes", "N/A")
        insurance = financial_details.get("Homeowners insurance", "N/A")

        agent_name = extract_agent_detail(driver, "Listed by")
        rent_details = extract_rent_detail(driver, logger, debug=True)

        motivated, trigger_keyword, trigger_sentence, trigger_context = extract_motivated_status(driver, config)

        details = {
            'address': full_address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'price': price,
            'rent': rent_details.get("Estimated Rent", "N/A"),
            'hoa_fee': hoa_fee,
            'property_taxes': property_taxes,
            'insurance': insurance,
            'agent_name': agent_name,
            'phone_number': phone_number,
            'Motivated': motivated,
            'Trigger Keyword': trigger_keyword,
            'Trigger Sentence': trigger_sentence,
            'Redfin Link': url
        }

        for key, value in details.items():
            logger.debug(f"{key}: {value}")

        if phone_number not in [None, "N/A"]:
            save_details_to_file(details, full_address)
            log_success_metrics(url, details)
            logger.info("Details saved successfully.")
            result['success'] = True
        else:
            logger.error("Necessary details missing or incomplete, not saving.")
    except TimeoutException as e:
        logger.error("Timeout occurred while trying to extract details from the page.", exc_info=True)
    except Exception as e:
        logger.exception(f"An error occurred while extracting details for {url}: {e}")

    return result


def extract_financial_details_from_calculator(driver, logger):
    SCROLL_OFFSET_Y = 150

    financial_details = {
        "Principal and interest": "N/A",
        "Property taxes": "N/A",
        "HOA dues": "N/A",
        "Homeowners insurance": "N/A"
    }

    try:
        # Using JavaScript to find the payment calculator section by its header text
        js_script = """
        var headings = document.querySelectorAll('h2');
        var targetHeading;
        headings.forEach(function(heading) {
            if (heading.textContent.includes('Payment calculator')) {
                targetHeading = heading;
            }
        });
        return targetHeading;
        """
        payment_calculator_heading = driver.execute_script(js_script)

        if payment_calculator_heading:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", payment_calculator_heading)
            driver.execute_script(f"window.scrollBy(0, {SCROLL_OFFSET_Y});")
            logger.info("Scrolled to Payment calculator section with offset.")

        time.sleep(5)  # Pause to ensure the content is fully loaded
        screenshot_path = "storage/screenshot.png"  # Temporarily save screenshot for OCR processing
        driver.save_screenshot(screenshot_path)

        financial_details = extract_details_via_ocr(screenshot_path, logger)

    except TimeoutException as e:
        logger.warning(f"Timeout waiting for Payment calculator section: {e}")
    except NoSuchElementException as e:
        logger.error("Payment calculator section not found.", exc_info=True)
    except Exception as e:
        logger.error(f"Error while attempting to extract financial details: {e}", exc_info=True)

    return financial_details


def extract_details_via_ocr(image_path, logger):
    financial_details = {
        "Principal and interest": "N/A",
        "Property taxes": "N/A",
        "HOA dues": "N/A",
        "Homeowners insurance": "N/A"
    }

    try:
        image = Image.open(image_path)
        text = pytesseract.image_to_string(image)
        logger.info("OCR extracted text.")  # Log that text was extracted

        patterns = {
            "Principal and interest": r"Principal and interest\s*\$\s*([\d,]+)\.?\d*",
            "Property taxes": r"Property taxes\s*\$\s*([\d,]+)\.?\d*",
            "HOA dues": r"HOA dues\s*\$\s*([\d,]+)\.?\d*",
            "Homeowners insurance": r"Homeowners insurance\s*\$\s*([\d,]+)\.?\d*"
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Convert to integer after removing commas
                financial_details[key] = int(match.group(1).replace(',', ''))
                logger.info(f"Extracted {key}: {financial_details[key]}")
            else:
                logger.warning(f"Pattern not found for {key}: {pattern}")

    except Exception as e:
        logger.error(f"Error extracting details via OCR from {image_path}: {e}")

    return financial_details

def log_success_metrics(url, details):
    logger = logging.getLogger('success_metrics')
    logger.info(f"Successfully processed URL: {url}")
    logger.info(f"Address: {details.get('address', 'N/A')}, Price: {details.get('price', 'N/A')}")
    logger.info(f"Phone Number: {details.get('phone_number', 'N/A')}, Motivated: {details.get('Motivated', 'N/A')}")

# Specific logger for scraping metrics
def log_scraping_metrics(logger, url, missing_details):
    logger.info(f"URL Processed: {url}")
    if missing_details:
        logger.warning(f"Missing information at URL {url}: {missing_details}")

def extract_address(driver):
    try:
        js_script = """
        var address = document.querySelector('div[data-rf-test-id="abp-streetLine"]');
        var cityStateZip = document.querySelector('div.dp-subtext.bp-cityStateZip');
        if (address && cityStateZip) {
            return address.textContent.trim() + ', ' + cityStateZip.textContent.trim();
        }
        return null;  // Return null if elements are not found
        """
        full_address = driver.execute_script(js_script)
        if full_address:
            return full_address
        else:
            print("Address elements not found on the page.")
            return None
    except Exception as e:
        print(f"Failed to extract address using JavaScript: {e}")
        return None



def extract_price(driver):
    try:
        # Adjust the selector as needed to accurately find the price element
        price_element = driver.find_element(By.CSS_SELECTOR, "div.statsValue")
        price_text = price_element.text.strip()
        return price_text
    except Exception as e:
        print(f"Failed to extract price: {e}")
        return "N/A"  # Return a default or error-specific value if extraction fails


def extract_city_state_zip(driver):
    try:
        js_script = """
        var locationElement = document.querySelector('div.dp-subtext.bp-cityStateZip');
        if (locationElement) {
            var text = locationElement.textContent.trim();
            var parts = text.split(',');
            if (parts.length > 1) {
                var city = parts[0].trim();
                var stateZip = parts[1].trim().split(' ');
                var state = stateZip[0];
                var zipCode = stateZip[1];
                return [city, state, zipCode];
            }
        }
        return ['Not found', 'Not found', 'Not found'];
        """
        city_state_zip = driver.execute_script(js_script)
        return tuple(city_state_zip)  # Convert list to tuple to match the original function's return type
    except Exception as e:
        print(f"Failed to extract city, state, and zip using JavaScript: {e}")
        return None, None, None


def extract_financial_detail(driver, label_text):
    strategies = [
        lambda: try_extract_financial_detail(driver, label_text),  # Original strategy
        lambda: try_extract_financial_detail_alternate(driver, label_text)  # Alternate strategy
    ]
    for strategy in strategies:
        try:
            result = strategy()
            if result and result != 'N/A':
                return result
        except TimeoutException:
            print(f"Timeout waiting for {label_text} details.")
        except Exception as e:
            print(f"Failed to extract {label_text} due to an error: {e}")
    return "N/A"

def try_extract_financial_detail(driver, label_text):
    """ Helper function to extract financial detail using JavaScript """
    js_script = f"""
    var headers = document.querySelectorAll('div.Row span.Row--header');
    var content = 'N/A';  // Default to 'N/A' if not found
    headers.forEach(header => {{
        if (header.textContent.includes('{label_text}')) {{
            var contentElement = header.closest('div.Row').querySelector('span.Row--content');
            if (contentElement) {{
                content = contentElement.textContent.trim();
            }}
        }}
    }});
    return content;
    """
    return driver.execute_script(js_script)

def try_extract_financial_detail_alternate(driver, label_text):
    """ Alternate strategy to extract financial detail """
    try:
        element = driver.find_element(By.XPATH, f"//span[contains(text(), '{label_text}')]/../following-sibling::span")
        return element.text.strip()
    except NoSuchElementException:
        return "N/A"


def extract_agent_detail(driver, label_text):
    try:
        # Revised JS script that navigates directly to the nested span containing the agent's name
        js_script = f"""
        var agentLabel = Array.from(document.querySelectorAll('span.agent-basic-details--heading'));
        var agentNameElement = agentLabel.find(span => span.textContent.includes('{label_text}'));
        return agentNameElement ? agentNameElement.querySelector('span').textContent.trim() : 'Not found';
        """
        agent_name = driver.execute_script(js_script)
        if agent_name:
            return agent_name
        else:
            print(f"No agent name found for label '{label_text}' on the page.")
            return "N/A"
    except Exception as e:
        print(f"Failed to extract agent name using JavaScript: {e}")
        return "N/A"


def extract_phone_number(driver):
    try:
        # Using JavaScript to extract the phone number directly
        js_script = "return document.querySelector('div.listingContactSection')?.textContent.trim().split(' ').pop() || 'N/A';"
        phone_number = driver.execute_script(js_script)
        if phone_number == 'N/A':
            logging.error("Phone number element not found.")
            return 'N/A'
        # Format and handle the phone number as needed
        formatted_number = format_phone_number(phone_number)
        return formatted_number
    except Exception as e:
        logging.error(f"Failed to extract phone number: {e}")
        return 'N/A'




def format_phone_number(phone):
    # Normalize and format the phone number
    digits = re.sub(r'[^\d]', '', phone)  # Remove non-digit characters
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) > 10:
        return f"+{digits[:-10]}-{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
    return "N/A"  # Return 'N/A' if the format does not match expected lengths

# Known common numbers that appear on many pages and should be ignored
common_numbers = {
    "813-518-8756"
}


def normalize_phone_number(phone_components):
    # Normalize phone numbers to match the format XXX-XXX-XXXX
    phone = '-'.join(phone_components)
    digits = re.sub(r'\D', '', phone)  # Remove all non-digit characters
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) > 10:
        # Handle international numbers that might include country codes
        return f"+{digits[:-10]}-{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
    return "N/A"  # Return 'N/A' if the format does not match

def extract_rent_detail(driver, logger, debug=False):
    try:
        # Set the scroll offset value
        scroll_offset = 1500

        # Retrieve the current URL from the driver
        current_url = driver.current_url
        # If the URL already has a hash, replace it, otherwise append the new hash
        climate_url = current_url.split('#')[0] + '#climate-section'

        # Navigate to the modified URL
        driver.get(climate_url)
        logger.info(f"Navigated to {climate_url}")

        # Wait for the page to stabilize after navigation
        time.sleep(2)  # Adjust the sleep time based on the actual load time of your page

        # Scroll down by the specified offset
        driver.execute_script(f"window.scrollBy(0, {scroll_offset});")
        logger.info(f"Scrolled down by {scroll_offset} pixels.")
        time.sleep(1)  # Give the page a moment to render any lazy-loaded elements

        # Capture screenshot for rent extraction via OCR
        screenshot_path = "storage/rent_section_screenshot.png"
        driver.save_screenshot(screenshot_path)
        logger.info(f"Screenshot saved for OCR processing: {screenshot_path}")

        # Extract rent details via OCR and output to console for debugging
        rent_details = parse_rent_details_via_ocr(screenshot_path, logger, debug=True)
        return rent_details

    except Exception as e:
        logger.error(f"Error while attempting to scroll to the rent section: {e}")
        return {"Estimated Rent": "N/A"}


def parse_rent_details_via_ocr(image_path, logger, debug=False):
    rent_details = {"Estimated Rent": "N/A"}

    try:
        image = Image.open(image_path)
        text = pytesseract.image_to_string(image)
        logger.info("OCR extracted text for rent.")  # Log that text was extracted

        # Output the raw OCR text to the console for debugging
        if debug:
            print("OCR Raw Rent Text:", text)

        # Improved pattern to capture rent, including more context around it
        rent_pattern = r"Est\.\s*\$?([\d,]+)\s*per month"
        match = re.search(rent_pattern, text, re.IGNORECASE)

        if match:
            # Normalize number format by removing commas and converting to an integer
            rent_details["Estimated Rent"] = int(match.group(1).replace(',', ''))
            logger.info(f"Extracted Estimated Rent: {rent_details['Estimated Rent']}")
        else:
            logger.warning(f"Pattern not found for Estimated Rent in text: {text}")
    except Exception as e:
        logger.error(f"Error extracting rent details via OCR from {image_path}: {e}")

    return rent_details



def validate_context(text, keyword, context_window=30):
    results = []
    # Search for the keyword and extract context
    for match in re.finditer(re.escape(keyword), text, re.IGNORECASE):
        start_idx = max(0, match.start() - context_window)
        end_idx = min(len(text), match.end() + context_window)
        context = text[start_idx:end_idx]
        # Check conditions and add to results
        if 'some condition met':
            results.append((True, 'relevant sentence', context))
        else:
            results.append((False, None, None))
    return results if results else [(False, None, None)]


def extract_motivated_status(driver, config):
    element = driver.find_element(By.CSS_SELECTOR, "div[data-rf-test-id='house-info']")
    text_content = element.text.lower()
    for keyword, synonyms in config['keywords'].items():  # Accessing keywords from config
        for synonym in synonyms:
            if synonym.lower() in text_content:
                results = validate_context(text_content, synonym)
                if results:
                    return True, keyword, results[0][1], results[0][2]
    return False, None, None, None



def save_details_to_file(details, street_address):
    # Define the path to the storage folder
    storage_folder = 'storage'  # Folder name

    # Check if the storage folder exists, create it if it does not
    if not os.path.exists(storage_folder):
        os.makedirs(storage_folder)
    
    # Create a filename that is safe for file systems
    safe_address = street_address.replace(' ', '_').replace(',', '').replace('/', '').replace('|', '')
    filename = f"{safe_address}.txt"
    
    # Construct the full path to save the file
    full_path = os.path.join(storage_folder, filename)
    
    # Write details to the file at the constructed path
    with open(full_path, 'w') as file:
        for key, value in details.items():
            file.write(f"{key}: {value}\n")
    
    print(f"Details saved for {details['address']} in {full_path}")



def read_and_process_csv():
    with open('locations.csv', mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:  # Corrected the syntax here by replacing ':=' with ':'
            if row['Completed'].strip().lower() != 'yes':
                zip_codes = row['ZipCodes'].split(',')
                return row['Location'], zip_codes, reader.line_num - 1  # Return the location, zip codes list, and index
    return None, None, None  # Return None if all are completed or no valid entries

def verify_and_clear_unprocessed(location):
    db_logger = logging.getLogger('db')
    to_be_processed_table = f"{sanitize_name(location)}_to_be_processed"
    processed_table = f"{sanitize_name(location)}_processed"

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {to_be_processed_table}")
            remaining_count = cursor.fetchone()[0]
            if remaining_count > 0:
                cursor.execute(f"SELECT url FROM {to_be_processed_table}")
                unprocessed_urls = cursor.fetchall()
                db_logger.warning(f"Remaining unprocessed URLs: {unprocessed_urls}")
                # Optionally, handle these URLs or log them for manual review.
            else:
                print("All URLs have been processed.")
                
    except sqlite3.DatabaseError as e:
        db_logger.error("Database error during verification of unprocessed URLs.", exc_info=True)
        raise
    except Exception as e:
        db_logger.exception("Unexpected error during verification of unprocessed URLs.", exc_info=True)
        raise

def backup_csv(storage_folder, backup_folder, filename):
    """
    Back up the CSV file, keeping only the three most recent backups.
    Args:
    - storage_folder: The folder where the main CSV is stored.
    - backup_folder: The folder where the backups should be stored.
    - filename: The name of the CSV file to back up.
    """
    try:
        # Format the current time to append to the backup file name
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        source_path = os.path.join(storage_folder, f"{filename}.csv")
        backup_path = os.path.join(backup_folder, f"{filename}_{current_time}.csv")

        # Ensure the backup folder exists; if not, create it
        if not os.path.exists(backup_folder):
            os.makedirs(backup_folder)
            print(f"Backup folder '{backup_folder}' created.")

        # Copy the CSV to the backup folder with the current date and time as the filename
        shutil.copy(source_path, backup_path)
        print(f"Backup of '{filename}.csv' created at '{backup_path}'")

        # Manage the number of backups by retaining only the three most recent ones
        manage_backup_files(backup_folder, filename)
    except Exception as e:
        print(f"An error occurred during the backup process: {e}")

def manage_backup_files(backup_folder, filename_prefix):
    """
    Keep only the three most recent backup files in the specified folder.
    Args:
    - backup_folder: The folder where the backups are stored.
    - filename_prefix: The prefix of the files to filter by.
    """
    try:
        # List all backup files, sorted by creation time (oldest first)
        all_backups = sorted(glob.glob(os.path.join(backup_folder, f"{filename_prefix}_*.csv")), key=os.path.getctime)

        # If more than three backups exist, remove the oldest ones
        while len(all_backups) > 3:
            os.remove(all_backups.pop(0))  # Remove the oldest backup file
            print(f"Deleted oldest backup to maintain limit of three.")
    except Exception as e:
        print(f"Error managing backup files: {e}")



async def main_async(logger: Logger):
    start_time = time.time()
    
    # Load the configuration and log the process
    config = load_config()
    logger.info("Configuration loaded successfully.")

    async_logger = await setup_async_logger()
    await async_logger.info("Async logger set up successfully.")

    drivers = None  # Initialize drivers here to ensure scope covers the finally block

    try:
        initialize_db()
        logger.info("Database initialized successfully.")

        location, zip_codes, _ = read_and_process_csv()
        if location is None:
            await async_logger.info("No locations to process or all locations have been completed.")
            return

        urls_to_process = load_unprocessed_urls()
        total_urls = len(urls_to_process)
        if not urls_to_process:
            await async_logger.info(f"No URLs to process for location {location}. Exiting...")
            return

        drivers = await setup_drivers_async(config['MAX_TABS'])
        if not drivers:
            await async_logger.error("Failed to initialize any drivers. Exiting...")
            return

        monitor_task = asyncio.create_task(monitor_progress(total_urls, async_logger))

        await async_logger.info(f"Starting processing for location {location}")

        options = set_firefox_options()

        storage_folder = 'storage'
        output_filename = location.replace(" ", "_") if location else "default_output"

        for batch in chunk_urls(urls_to_process, batch_size=5):  # Set batch size to 5 here
            await run_scraping(drivers, batch, options, async_logger, config, storage_folder, output_filename)  # Corrected the function call here
            await async_logger.info(f"Processed {len(batch)} URLs for location: {location}")

        await monitor_task

        combine_text_files_to_csv(storage_folder, output_filename)
        backup_csv(storage_folder, 'backup', output_filename)

        verify_and_clear_unprocessed(location)
        await async_logger.info(f"Finished processing. Duplicates removed from {output_filename}.csv")

    except Exception as e:
        await async_logger.exception("An error occurred during the scraping process.", exc_info=True)
        sys.exit(1)  # Exit with an error code to potentially trigger a restart by a supervisor

    finally:
        if drivers:
            close_all_drivers(drivers)
        elapsed_time = time.time() - start_time
        await async_logger.info(f"Total time taken: {elapsed_time:.2f} seconds")




async def main():
    logger = setup_logging()
    logger.info("Starting the application.")

    try:
        initialize_db()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.exception("Failed to initialize the database.")

    try:
        await main_async(logger)
    except Exception as e:
        logger.exception("An error occurred in the main function.")

if __name__ == "__main__":
    # Setup signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())
    except Exception as e:
        print(f"An error occurred in the main function: {e}")


