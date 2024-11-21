import uuid
import io
import csv
import os
import glob
import logging
import asyncio
import aiofiles
import atexit
import sys
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler



# Setup Advanced Logging
def setup_logging():
    """Sets up logging with rotation and console output."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_directory = os.path.join("logs", current_date)
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, "preparecsv.log")

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

    return logger


def cleanup():
    print("Cleanup tasks are being performed before exit.")
    logging.info("Cleanup completed.")

# Register the cleanup function to be called upon normal script termination
atexit.register(cleanup)

def find_csv_files(directory):
    # Look for CSV files in the specified directory
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    # Filter out the file 'Total_URLs.csv'
    csv_files = [file for file in csv_files if not file.endswith('Total_URLs.csv')]
    if csv_files:
        print("Found CSV files:", csv_files)
    else:
        print("No CSV files found in the directory.")
    return csv_files


def clean_numeric(value):
    if isinstance(value, str):
        # Remove currency symbols, commas, and strip whitespace
        value = value.replace('$', '').replace(',', '').strip()
        # Check if the value is empty or a placeholder for missing data
        if value == '' or value == '—' or value.lower() == 'not found' or value == 'N/A':
            return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def round_numeric(value, decimals=2):
    """
    Rounds a numeric value to a specified number of decimal places.
    Returns None if the value is not a number.
    """
    try:
        return round(float(value), decimals)
    except (ValueError, TypeError):
        return None


def handle_expired_listing(row):
    """Assigns a UUID email to expired listings and modifies the row as needed."""
    row['email'] = f"{uuid.uuid4()}@example.com"
    # Add any other row modifications specific to expired listings here.
    return row


def calculate_term_length(price, monthly_payment):
    present_value = price * 0.9  # 90% of price
    target_value = price * 0.7   # 70% of price
    term_length = 0
    remaining_value = present_value

    while remaining_value > target_value and monthly_payment > 0:
        remaining_value -= monthly_payment
        term_length += 1

    return term_length



def compute_financial_metrics(data):
    price = clean_numeric(data.get('price', '0'))
    rent = clean_numeric(data.get('rent', '0'))
    property_taxes = clean_numeric(data.get('property_taxes', '0'))
    insurance = clean_numeric(data.get('insurance', '0'))
    hoa_fee = clean_numeric(data.get('hoa_fee', '0'))

    management_fee = 0
    capex_fee = 0
    vacancy_fee = 0
    monthly_creative_payments = 0
    downpayment = 0

    cash_offer = round_numeric(price * 0.75)

    if rent > 0:
        management_fee = round_numeric(rent * 0.1)
        capex_fee = round_numeric(rent * 0.05)
        vacancy_fee = round_numeric(rent * 0.05)
        monthly_creative_payments = round_numeric(rent - (350 + insurance + property_taxes + management_fee + capex_fee + vacancy_fee))
        downpayment = round_numeric(price * 0.1)

    term_length_months = calculate_term_length(price, monthly_creative_payments)

    new_fields = {
        'management_fee': management_fee,
        'capex_fee': capex_fee,
        'vacancy_fee': vacancy_fee,
        'monthly_creative_payments': monthly_creative_payments,
        'downpayment': downpayment,
        'cash_offer': cash_offer,
        'hoa_fee': hoa_fee,
        'term_length_months': term_length_months
    }
    data.update(new_fields)

    return data, list(new_fields.keys())  # Return the list of new field names



async def update_csv_file_with_metrics(file_path, data_list):
    if not data_list:
        return

    sample_data, new_fields = compute_financial_metrics(data_list[0])
    fieldnames = list(data_list[0].keys()) + [field for field in new_fields if field not in data_list[0].keys()]

    # Open the file asynchronously with aiofiles
    async with aiofiles.open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        # Create a StringIO object to use as a buffer for the csv.DictWriter
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        # Write the header and data to the buffer
        writer.writeheader()
        for data in data_list:
            updated_data, _ = compute_financial_metrics(data)
            writer.writerow(updated_data)
        
        # Move to the start of the StringIO buffer
        output.seek(0)
        # Asynchronously write the entire buffer to the file
        await csvfile.write(output.getvalue())


async def read_and_filter_csv_data(file_path):
    print(f"Starting to read and filter data from {file_path}")
    logging.info(f"Starting to process file: {file_path}")
    try:
        data_list = []
        original_fieldnames = None
        async with aiofiles.open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            content = await csvfile.read()
        reader = csv.DictReader(io.StringIO(content))
        original_fieldnames = reader.fieldnames
        for row in reader:
            if row.get('phone_number', 'N/A') == 'N/A':
                row = handle_expired_listing(row)
            data_list.append(row)
        return data_list, original_fieldnames
    except Exception as e:
        logging.error(f"Failed to read or process file {file_path}: {e}")
        return [], []

async def process_and_cleanup_files(file_list):
    for csv_file in file_list:
        try:
            data_list, _ = await read_and_filter_csv_data(csv_file)
            await update_csv_file_with_metrics(csv_file, data_list)  # Update in-place
            print(f"Updated CSV file in-place at: {csv_file}")
            logging.info(f"CSV file updated in-place at: {csv_file}")
        except Exception as e:
            logging.error(f"Failed to process file {csv_file}: {e}")
            print(f"Error during processing file {csv_file}: {e}")



async def main():
    directory_path = 'storage'
    csv_files = find_csv_files(directory_path)
    await process_and_cleanup_files(csv_files)
    print("All tasks completed, exiting.")

if __name__ == "__main__":
    asyncio.run(main())

