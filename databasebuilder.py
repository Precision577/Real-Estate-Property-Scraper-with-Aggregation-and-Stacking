from logging.handlers import RotatingFileHandler
import os
import pandas as pd
import json
import glob
import logging
import collections
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def setup_logging():
    """Sets up logging with rotation and console output."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_directory = os.path.join("logs", current_date)
    os.makedirs(log_directory, exist_ok=True)
    log_file_path = os.path.join(log_directory, "databasebuilder.log")

    if os.path.exists(log_file_path):
        os.remove(log_file_path)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, datefmt=date_format)

    file_handler = RotatingFileHandler(log_file_path, maxBytes=10485760, backupCount=3)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    return logger


MAX_ADDRESSES_PER_AGENT = 15

def load_headers(file_path):
    """Load headers from a text file."""
    logger = logging.getLogger()  # Get the logger instance
    try:
        with open(file_path, 'r') as file:
            headers = [line.strip() for line in file if line.strip()]
        headers.append("Email_sent")  # Add the Email_sent header
        logger.debug(f"Headers loaded successfully from '{file_path}'.")
        return headers
    except Exception as e:
        logger.error(f"Failed to load headers from '{file_path}': {e}")
        return []


class OrderedJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder to maintain the order of OrderedDict."""
    def encode(self, o):
        if isinstance(o, collections.OrderedDict):
            return "{" + ", ".join(f'"{k}": {self.encode(v)}' for k, v in o.items()) + "}"
        return super().encode(o)

def find_csv_files(storage_dir, ignore_file="Total_URLs.csv"):
    """Scan for all CSV files in the directory, excluding the specified file."""
    logger = logging.getLogger()  # Get the logger instance
    try:
        all_files = glob.glob(os.path.join(storage_dir, '*.csv'))
        files = [f for f in all_files if os.path.basename(f) != ignore_file]
        logger.debug(f"Found {len(files)} CSV files to process.")
        return files
    except Exception as e:
        logger.error(f"Failed to find CSV files: {e}")
        return []


def load_json_data(file_path):
    """Load data from a JSON file. If the file doesn't exist, return an empty dictionary."""
    logger = logging.getLogger()
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        logger.debug("JSON data loaded successfully.")
        return data
    except FileNotFoundError:
        logger.warning("JSON file not found, creating a new one.")
        return {}
    except json.JSONDecodeError:
        logger.error("Error decoding JSON, starting with an empty dataset.")
        return {}


def save_json_data(data, file_path):
    """Save data to a JSON file using a custom encoder to maintain the order."""
    logger = logging.getLogger()
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4, cls=OrderedJsonEncoder)
        logger.debug("Data saved to JSON file successfully with ordered keys.")
    except Exception as e:
        logger.error(f"Failed to save data to JSON: {e}")


def read_and_process_csv(file_path, headers):
    """Read the CSV file and adjust processing to the columns that are actually present, ensuring ordering."""
    logger = logging.getLogger()
    try:
        df = pd.read_csv(file_path)
        logger.debug(f"All headers found in '{file_path}': {df.columns.tolist()}")

        # Reorder DataFrame columns according to headers, filling missing with NaN
        df = df.reindex(columns=headers)

        # If 'agent_name' is critical for grouping, check if it exists
        if 'agent_name' not in df.columns:
            logger.error(f"'agent_name' column is missing in the file: {file_path}")
            return {}
        
        # Convert DataFrame to dictionary by agent_name
        grouped = df.groupby('agent_name')
        data = {name: group.to_dict('records') for name, group in grouped}
        logger.debug(f"Processed CSV file: {file_path} using columns: {df.columns.tolist()}")
        return data

    except Exception as e:
        logger.error(f"Error processing CSV file {file_path}: {e}")
        return {}

def update_json_database(json_data, new_data, headers):
    """Update JSON database ensuring data integrity and order, filling missing data."""
    logger = logging.getLogger()
    update_count = 0
    new_count = 0
    updated_agents = set()
    new_agents = set()

    try:
        for agent_name, properties in new_data.items():
            if agent_name not in json_data:
                json_data[agent_name] = {}
                new_agents.add(agent_name)

            existing_addresses = {key: json_data[agent_name][key]["address"] for key in json_data[agent_name]}

            for property_dict in properties:
                address = property_dict.get("address", None)
                address_key = next((k for k, v in existing_addresses.items() if v == address), None)

                if address_key:  # Update existing address
                    existing_entry = json_data[agent_name][address_key]
                    is_updated = False
                    for header in headers:
                        if header in existing_entry and pd.isna(existing_entry[header]) and not pd.isna(property_dict.get(header, None)):
                            existing_entry[header] = property_dict[header]
                            is_updated = True
                    if is_updated:
                        update_count += 1
                        updated_agents.add(agent_name)
                else:  # Address is new, create a new entry
                    ordered_property = {header: property_dict.get(header, None) for header in headers}
                    ordered_property["timestamp"] = datetime.now().isoformat()
                    ordered_property["Email_sent"] = False  # Initialize Email_sent as False

                    if len(existing_addresses) >= MAX_ADDRESSES_PER_AGENT:
                        oldest_address_key = min(json_data[agent_name], key=lambda k: json_data[agent_name][k]["timestamp"])
                        del json_data[agent_name][oldest_address_key]
                        existing_addresses.pop(oldest_address_key)

                    address_key = f"Address {len(existing_addresses) + 1}"
                    json_data[agent_name][address_key] = ordered_property
                    existing_addresses[address_key] = address
                    new_count += 1
                    new_agents.add(agent_name)

        logger.info("JSON database updated successfully with ordered data.")
    except Exception as e:
        logger.error(f"Failed to update JSON database: {e}")

    return json_data, new_count, update_count, new_agents, updated_agents


def main():
    # Set up the logger
    setup_logging()
    logger = logging.getLogger()

    cwd = os.getcwd()
    headers_file_path = os.path.join(cwd, 'headers.txt')
    json_file_path = os.path.join(cwd, 'database', 'agents_data.json')
    storage_dir = os.path.join(cwd, 'storage')

    # Load headers from file
    headers = load_headers(headers_file_path)

    # Ensure the database directory exists
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

    # Load existing JSON data
    json_data = load_json_data(json_file_path)

    # Find all relevant CSV files
    csv_files = find_csv_files(storage_dir)

    total_new = 0
    total_updated = 0
    all_new_agents = set()
    all_updated_agents = set()

    # Process CSV files concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(read_and_process_csv, csv_file, headers): csv_file for csv_file in csv_files}
        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                new_data = future.result()
                if new_data:
                    json_data, new_count, update_count, new_agents, updated_agents = update_json_database(json_data, new_data, headers)
                    total_new += new_count
                    total_updated += update_count
                    all_new_agents.update(new_agents)
                    all_updated_agents.update(updated_agents)

                    # Delete the CSV file after processing
                    try:
                        os.remove(csv_file)  # This line deletes the CSV file

                        logger.info(f"Successfully deleted file: {csv_file}")
                    except Exception as e:
                        logger.error(f"Failed to delete file {csv_file}: {e}")
                else:
                    logger.error(f"Failed to process data from {csv_file}, not deleting the file.")
            except Exception as e:
                logger.error(f"Error processing file {csv_file}: {e}")

    # Save updated data back to JSON
    save_json_data(json_data, json_file_path)

    # Log summary of changes
    logger.info(f"Total new contacts added: {total_new}, New agents: {list(all_new_agents)}")
    logger.info(f"Total contacts updated: {total_updated}, Updated agents: {list(all_updated_agents)}")

if __name__ == "__main__":
    main()
