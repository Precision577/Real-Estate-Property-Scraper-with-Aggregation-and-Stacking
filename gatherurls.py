
import signal
import multiprocessing
import os
import csv
import time
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.common.exceptions import NoSuchElementException
import sqlite3
import asyncio
from contextlib import contextmanager
#from threading import Semaphore
import re
import atexit
import subprocess
import sys
import gc
from datetime import datetime
import aiofiles
import aiosqlite
from functools import wraps


# Constants
MAX_TABS = 10
MAX_CONCURRENCY = 80  # New constant to control concurrency
USE_ASYNC = True
FILTERS = "/filter/property-type=house+multifamily,min-days-on-market=2mo,include=forsale+mlsfsbo,status=active"
drivers = None


async def safe_webdriver_command(driver, command, *args, retries=3, sleep_interval=2, **kwargs):
    loop = asyncio.get_event_loop()
    for attempt in range(retries):
        try:
            # Run the synchronous Selenium command in the default executor
            result = await loop.run_in_executor(None, command, *args, **kwargs)
            return result
        except WebDriverException as e:
            if attempt < retries - 1:
                print(f"WebDriver command failed: {e}. Retrying {attempt + 1}/{retries}")
                await asyncio.sleep(sleep_interval)
            else:
                print("Maximum retry attempts reached, failing.")

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def signal_handler(sig, frame):
    global drivers  # Add this line to specify that we refer to the global variable
    print('You pressed Ctrl+C or termination signal received!')
    if drivers is not None:
        close_all_drivers(drivers)
    sys.exit(0)

def set_firefox_options():
    options = webdriver.FirefoxOptions()
    options.add_argument('--headless')  # Ensure headless mode is activated
    options.set_preference('permissions.default.image', 2)
    options.set_preference('permissions.default.stylesheet', 2)
    options.set_preference("javascript.enabled", False)  # Disable JavaScript
    options.set_capability('pageLoadStrategy', 'eager')
    return options



async def setup_drivers_async(count):
    """ Asynchronously setup drivers to parallelize initialization using concurrent futures. """
    loop = asyncio.get_event_loop()
    
    # Helper function to initialize a single driver
    def init_driver():
        options = set_firefox_options()
        try:
            return webdriver.Firefox(options=options)
        except Exception as e:
            print(f"Failed to initialize a driver: {e}")
            return None

    # Create a list of futures for driver initialization
    futures = [loop.run_in_executor(None, init_driver) for _ in range(count)]
    
    # Wait for all futures to complete
    drivers = await asyncio.gather(*futures)

    # Filter out None values in case some drivers failed to initialize
    drivers = [driver for driver in drivers if driver is not None]
    
    if len(drivers) == count:
        print(f"All {count} drivers have been successfully initialized.")
    else:
        print(f"Only {len(drivers)} out of {count} drivers have been initialized successfully.")

    return drivers




async def async_initialize_driver():
    options = set_firefox_options()
    retries = 3
    for attempt in range(retries):
        driver = None
        try:
            driver = webdriver.Firefox(options=options)
            await asyncio.sleep(0)  # Yield control, simulate asynchronous operation
            driver.get("https://www.redfin.com")
            load_cookies(driver, "cookies.txt")
            driver.set_page_load_timeout(15)
            driver.refresh()
            return driver
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if driver:
                driver.quit()
            await asyncio.sleep(2)  # Delay before retrying
        if attempt == retries - 1:
            return None



def load_cookies(driver, path_to_cookie_file):
    def parse_cookie_line(line):
        parts = line.strip().split('\t')
        if len(parts) == 7:
            return {
                'domain': parts[0],
                'httpOnly': parts[1] == 'TRUE',
                'path': parts[2],
                'secure': parts[3] == 'TRUE',
                'expiry': int(parts[4]) if parts[4] != '0' else None,
                'name': parts[5],
                'value': parts[6]
            }
        return None

    try:
        cookies = []
        with open(path_to_cookie_file, 'r') as cookies_file:
            cookies = [parse_cookie_line(line) for line in cookies_file if line.strip() and not line.startswith('#') and parse_cookie_line(line)]

        js_commands = []
        for cookie in cookies:
            if cookie and 'expiry' in cookie:  # Ensure the cookie has an 'expiry' field
                js_commands.append(
                    f"document.cookie = '{cookie['name']}={cookie['value']}; path={cookie['path']}; domain={cookie['domain']}; expires={(cookie['expiry'] if cookie['expiry'] else '')};{( ' secure;' if cookie['secure'] else '')}'"
                )

        # Execute all JavaScript commands in one go
        if js_commands:
            driver.execute_script("; ".join(js_commands))
            print("All cookies loaded successfully through JS.")
        else:
            print("No valid cookies were parsed to be set.")

    except FileNotFoundError:
        print(f"Cookie file not found at {path_to_cookie_file}")
    except Exception as e:
        print(f"Error loading cookies using JS: {e}")


def kill_firefox():
    try:
        if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
            subprocess.run(['pkill', 'firefox'], check=True)
        elif sys.platform.startswith('win'):
            subprocess.run(['taskkill', '/IM', 'firefox.exe', '/F'], check=True)
    except subprocess.CalledProcessError as e:
        print("Failed to kill Firefox processes:", e)

def worker_process(url_queue, driver_id):
    driver = None
    try:
        driver = setup_driver(driver_id)  # Setup a separate driver for each worker
        while not url_queue.empty():
            url = url_queue.get()
            process_page(driver, url)
    finally:
        if driver:
            driver.quit()



async def distribute_urls_to_drivers_async(drivers, urls):
    tasks = []
    # Round-robin assignment of URLs to drivers
    for i, url in enumerate(urls):
        driver = drivers[i % len(drivers)]
        task = asyncio.create_task(scrape_with_driver(driver, [url]))
        tasks.append(task)
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)


def close_all_drivers(drivers=None):
    if drivers:
        for driver in drivers:
            try:
                driver.quit()
            except Exception as e:
                print(f"Failed to close driver properly: {e}")
    gc.collect()



def read_unique_locations(csv_path):
    locations = set()
    try:
        with open(csv_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(f"Reading location from CSV: {row['Location']}")
                locations.add(row['Location'].replace(" ", "_").replace(",", "_"))
    except FileNotFoundError:
        print(f"No file found at {csv_path}. Starting without locations.")
    except Exception as e:
        print(f"An error occurred while reading locations: {e}")
    return locations




@contextmanager
def get_db_connection(db_path='processed_urls.db'):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        yield conn  # This allows the database connection to be used within a 'with' block
    except sqlite3.Error as e:
        print(f"Database connection failed: {e}")
    finally:
        if conn:
            conn.close()  # Ensure the connection is closed after processing
def sanitize_name(name):
    """ Sanitize the location name to be used as a valid SQL table name """
    return re.sub(r'\W+', '_', name)

def initialize_db():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        create_tables(cursor)
        check_tables_exist(cursor)
        conn.commit()
        print("Database initialization complete.")

def check_tables_exist(cursor):
    tables = ["Default_Location_processed", "processed", "missing", "to_be_processed"]
    for table in tables:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
        result = cursor.fetchone()
        if result:
            print(f"Table '{table}' exists.")
        else:
            print(f"Table '{table}' does not exist, which may cause runtime errors.")



def create_tables(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed (
                url TEXT PRIMARY KEY,
                processed_date TEXT
            )
        """)
        print("Table 'processed' created or already exists.")
    except sqlite3.Error as e:
        print(f"Failed to create 'processed' table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS missing (
                url TEXT PRIMARY KEY,
                last_seen_date TEXT
            )
        """)
        print("Table 'missing' created or already exists.")
    except sqlite3.Error as e:
        print(f"Failed to create 'missing' table: {e}")

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS to_be_processed (
                url TEXT PRIMARY KEY
            )
        """)
        print("Table 'to_be_processed' created or already exists.")
    except sqlite3.Error as e:
        print(f"Failed to create 'to_be_processed' table: {e}")

def update_url_status(url, status, location):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        query = '''
        INSERT INTO location_urls (url, status, location, last_checked)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            status=excluded.status,
            last_checked=excluded.last_checked
        '''
        if safe_db_operation(cursor, query, (url, status, location, today)):
            conn.commit()
        else:
            print("Failed to update URL status after retries.")

def fetch_urls_by_status(location, status):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT url FROM location_urls WHERE location = ? AND status = ?
        ''', (location, status))
        return [row[0] for row in cursor.fetchall()]

def find_and_update_missing_urls(current_urls):
    today = datetime.now().strftime('%Y-%m-%d')
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT url FROM processed')
        processed_urls = {row[0] for row in cursor.fetchall()}
        
        missing_urls = processed_urls - current_urls
        
        # Move missing URLs from 'processed' to 'missing' table
        for url in missing_urls:
            cursor.execute('''
            INSERT INTO missing (url, last_seen_date)
            VALUES (?, ?) ON CONFLICT(url) DO UPDATE SET
            last_seen_date=excluded.last_seen_date
            ''', (url, today))

            cursor.execute('''
            DELETE FROM processed WHERE url=?
            ''', (url,))
        conn.commit()
        print("Updated missing URLs.")
        return len(missing_urls)  # Return the count of missing URLs





def load_processed_urls():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT url FROM processed")
            urls = {row[0] for row in cursor.fetchall()}
            return urls
        except sqlite3.Error as e:
            print(f"Failed to load URLs from 'processed': {e}")
            return set()


async def save_urls_to_be_processed(urls, processed_urls):
    if urls:
        # Filter out URLs that are already processed
        urls_to_save = [url for url in urls if url not in processed_urls]

        if urls_to_save:
            table_name = "to_be_processed"
            async with aiosqlite.connect('processed_urls.db') as conn:
                cursor = await conn.cursor()
                url_tuples = [(url,) for url in urls_to_save]
                await conn.execute("BEGIN")
                await cursor.executemany(f"INSERT OR IGNORE INTO {table_name} (url) VALUES (?)", url_tuples)
                await conn.commit()
                print(f"All {len(urls_to_save)} new URLs saved to '{table_name}'.")
        else:
            print("No new URLs to be saved; all URLs are already processed.")






def collect_urls_for_zip_codes(driver, zip_codes):
    base_url = "https://www.redfin.com/zipcode/"  # Ensure there's no extra slash at the end
    all_collected_urls = {}
    for zip_code in zip_codes:
        zip_code = zip_code.strip()  # Strip spaces to ensure clean zip code
        zip_url = f"{base_url}{zip_code}{FILTERS}"
        print(f"Formatted zip URL: {zip_url}")  # Debug: Show the full URL before navigation
        try:
            urls = collect_urls_from_all_pages(driver, zip_url)
            all_collected_urls[zip_code] = urls
            print(f"Collected {len(urls)} URLs for zip code {zip_code}")
        except Exception as e:
            print(f"Failed to collect URLs for zip code {zip_code}: {e}")
    return all_collected_urls



def collect_urls(driver, location, is_url=True):
    try:
        print(f"Attempting to navigate to: {location}")  # Print before navigation
        driver.get(location)
        print(f"Navigating to URL with filters: {location}")  # Confirm the URL navigation
        print(f"Successfully navigated to: {driver.current_url}")  # Confirm current URL after navigation

        urls = extract_property_urls(driver)
        print(f"Collected {len(urls)} URLs from {driver.current_url}")  # Debugging statement after collection
        return urls
    except TimeoutException as e:
        print(f"Error navigating to search results: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while collecting URLs from {location}: {e}")
        return []

async def collect_urls_from_all_pages(driver, zip_code, base_url):
    """Collect URLs from all pages for a single zip code and report the counts."""
    full_url = f"{base_url}{zip_code.strip()}{FILTERS}"
    print(f"Starting URL collection for zip code: {zip_code.strip()}")
    collected_urls = set()
    current_page = 1
    has_more_pages = True

    while has_more_pages:
        page_url = f"{full_url}/page-{current_page}"
        try:
            await safe_webdriver_command(driver, driver.get, page_url)
            new_urls = set(extract_property_urls(driver))
            if not new_urls.issubset(collected_urls):
                collected_urls.update(new_urls)
                print(f"Page {current_page}: Collected {len(new_urls)} new URLs.")
                current_page += 1
            else:
                has_more_pages = False
        except WebDriverException as e:
            print(f"Failed to navigate or collect URLs on page {current_page}: {e}")
            has_more_pages = False

    print(f"Collected a total of {len(collected_urls)} URLs from {current_page - 1} pages for zip code {zip_code.strip()}.")
    return zip_code.strip(), collected_urls





async def distribute_url_collection(drivers, zip_codes, base_url):
    """Distribute URL collection tasks among drivers."""
    tasks = []
    for i, zip_code in enumerate(zip_codes):
        driver = drivers[i % len(drivers)]
        task = asyncio.create_task(collect_urls_from_all_pages(driver, zip_code, base_url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return {zip_code: urls for zip_code, urls in results}


def extract_property_urls(driver):
    """Use JavaScript to extract all property URLs from the page and filter out blacklisted terms."""
    try:
        js_script = """
        var links = document.querySelectorAll('a.link-and-anchor.visuallyHidden');
        return Array.from(links).map(link => link.href);
        """
        property_urls = driver.execute_script(js_script)
        # Filter out URLs containing the term 'undisclosed' in any case variation
        blacklisted_term = 'undisclosed'
        filtered_urls = [url for url in property_urls if blacklisted_term.lower() not in url.lower()]
        return filtered_urls
    except WebDriverException as e:
        print(f"Failed to extract URLs using JavaScript: {e}")
        return []






def read_and_process_csv():
    with open('locations.csv', mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:  # Corrected the syntax here by replacing ':=' with ':'
            if row['Completed'].strip().lower() != 'yes':
                zip_codes = row['ZipCodes'].split(',')
                return row['Location'], zip_codes, reader.line_num - 1  # Return the location, zip codes list, and index
    return None, None, None  # Return None if all are completed or no valid entries


def save_urls_to_csv(urls, filepath):
    with open(filepath, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['URL'])
        for url in urls:
            writer.writerow([url])

async def read_zip_codes_from_file():
    zip_codes_path = 'zipcodes.txt'
    try:
        with open(zip_codes_path, 'r') as file:
            zip_codes = file.read().strip().split(',')
        print(f"Read {len(zip_codes)} zip codes from {zip_codes_path}.")
        return zip_codes
    except FileNotFoundError:
        print(f"Error: The file {zip_codes_path} was not found.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def reintegrate_missing_urls(missing_urls):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        for url in missing_urls:
            cursor.execute('''
                INSERT INTO processed (url, processed_date)
                VALUES (?, ?) ON CONFLICT(url) DO UPDATE SET
                processed_date=excluded.processed_date
            ''', (url, today))
            cursor.execute('''
                DELETE FROM missing WHERE url=?
            ''', (url,))
        conn.commit()
        print(f"Reintegrated {len(missing_urls)} URLs back into the processed table.")

def find_and_update_missing_urls(current_urls):
    today = datetime.now().strftime('%Y-%m-%d')
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT url FROM processed')
        processed_urls = {row[0] for row in cursor.fetchall()}
        
        missing_urls = processed_urls - current_urls
        
        # Update missing URLs in the 'missing' table
        for url in missing_urls:
            cursor.execute('''
            INSERT INTO missing (url, last_seen_date)
            VALUES (?, ?) ON CONFLICT(url) DO UPDATE SET
            last_seen_date=excluded.last_seen_date
            ''', (url, today))
        conn.commit()
        print("Updated missing URLs.")
        return missing_urls  # Now returns the set of missing URLs



async def main():
    start_time = time.time()
    aggregated_urls = set()  # To hold all URLs from all runs
    drivers = await setup_drivers_async(MAX_TABS)
    if not drivers:
        print("Failed to initialize drivers.")
        return

    try:
        initialize_db()

        for _ in range(3):  # Loop to run the scraping process three times
            zip_codes = await read_zip_codes_from_file()
            if not zip_codes:
                print("No ZIP codes to process.")
                continue  # Skip to the next iteration if no ZIP codes are found

            base_url = "https://www.redfin.com/zipcode/"
            collected_data = await distribute_url_collection(drivers, zip_codes, base_url)
            current_urls = set()
            for zip_code, urls in collected_data.items():
                current_urls.update(urls)
                print(f"Collected {len(urls)} URLs for zip code {zip_code}")

            aggregated_urls.update(current_urls)  # Add current URLs to the aggregated set

        processed_urls = load_processed_urls()  # Load processed URLs from the database
        new_urls = aggregated_urls - processed_urls
        missing_urls = processed_urls - aggregated_urls

        if missing_urls:
            missing_urls_count = find_and_update_missing_urls(aggregated_urls)
            reintegrate_missing_urls(missing_urls)  # Reintegrate missing URLs if found
            print(f"Detected and reintegrated {missing_urls_count} missing URLs.")

        if new_urls:
            print(f"Detected {len(new_urls)} new URLs which will be saved to the database.")
            await save_urls_to_be_processed(new_urls, processed_urls)  # Pass processed_urls for checking
        else:
            print("No new URLs to be processed.")

        save_urls_to_csv(aggregated_urls, 'storage/Total_URLs.csv')

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        close_all_drivers(drivers)
        elapsed_time = time.time() - start_time
        print(f"Total time taken: {elapsed_time:.2f} seconds")
        print(f"Total URLs collected: {len(aggregated_urls)}")
        if elapsed_time > 0:
            print(f"URLs processed per second: {len(aggregated_urls) / elapsed_time:.2f}")

        os._exit(0)

if __name__ == "__main__":
    initialize_db()  # Make sure to initialize the database outside of async context
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    asyncio.run(main())
