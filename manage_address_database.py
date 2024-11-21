import os
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_directories():
    current_directory = os.getcwd()
    storage_directory = os.path.join(current_directory, 'storage')
    addresses_directory = os.path.join(current_directory, 'Addresses')
    os.makedirs(addresses_directory, exist_ok=True)
    return storage_directory, addresses_directory

def find_csv_filenames(path_to_dir, ignore_file="Total_URLs.csv"):
    try:
        filenames = os.listdir(path_to_dir)
        return [filename for filename in filenames if filename.endswith('.csv') and filename != ignore_file]
    except FileNotFoundError:
        logging.error(f"Directory {path_to_dir} does not exist.")
        return []

def read_csv_files(csv_files, path_to_dir):
    data_frames = []
    for file in csv_files:
        file_path = os.path.join(path_to_dir, file)
        try:
            df = pd.read_csv(file_path)
            df['phone_number'] = df['phone_number'].astype(str).str.strip()  # Clean up phone numbers
            if not df.empty:
                data_frames.append(df)
        except pd.errors.EmptyDataError:
            logging.warning(f"No data in {file_path}")
        except Exception as e:
            logging.error(f"Failed to read {file_path} due to {e}")
    return data_frames

def process_data(data_frames):
    all_data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
    if all_data.empty:
        return all_data

    grouped = all_data.groupby(['phone_number', 'agent_name'])
    output_data = []
    for (phone_number, agent_name), group in grouped:
        unique_addresses = group.apply(
            lambda x: f"{x['address'].strip()}, {x['city'].strip()} {x['state'].strip()} {str(x['zip_code']).strip()}", axis=1)
        agent_data = {'Phone Number': phone_number, 'Agent Name': agent_name, 'Number of Addresses': len(unique_addresses)}
        for idx, address in enumerate(unique_addresses, 1):
            agent_data[f'Address_{idx}'] = address
        output_data.append(agent_data)
    return pd.DataFrame(output_data)

def save_data(data_frame, output_file_path):
    if not data_frame.empty:
        if os.path.exists(output_file_path):
            existing_data = pd.read_csv(output_file_path)
            combined_data = pd.concat([existing_data, data_frame]).drop_duplicates()
            combined_data.to_csv(output_file_path, index=False)
        else:
            data_frame.to_csv(output_file_path, index=False)
        logging.info(f"Data saved to {output_file_path}")
    else:
        logging.info("No data to save.")

def main():
    storage_directory, addresses_directory = setup_directories()
    csv_files = find_csv_filenames(storage_directory)
    if not csv_files:
        logging.info("No CSV files found to process in the 'storage' folder.")
        return
    
    data_frames = read_csv_files(csv_files, storage_directory)
    processed_data = process_data(data_frames)
    output_file_path = os.path.join(addresses_directory, 'Addresses.csv')
    save_data(processed_data, output_file_path)

if __name__ == "__main__":
    main()

