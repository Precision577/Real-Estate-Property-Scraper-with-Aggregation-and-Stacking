import pgeocode

def get_zip_codes_for_county(county_input, state_input):
    # Initialize the ZIP code lookup for the United States
    nomi = pgeocode.Nominatim('us')
    
    # Convert input to proper format
    county_input = county_input.strip().lower()
    state_input = state_input.strip().upper()
    
    # Download the full ZIP code dataset
    all_zipcodes_info = nomi._data
    
    # Filter data for the specified state
    state_zipcodes_info = all_zipcodes_info[all_zipcodes_info['state_code'] == state_input]
    
    # Filter the data to get ZIP codes for the specified county, using contains for partial matches
    county_zipcodes = state_zipcodes_info[state_zipcodes_info['county_name'].str.lower().str.contains(county_input)]
    
    # Extract the ZIP codes
    zip_codes = county_zipcodes['postal_code'].tolist()
    
    return zip_codes

# Main function to ask for user input and output comma-separated ZIP codes
def main():
    county = input("Enter the county name: ").strip()
    state = input("Enter the state abbreviation (e.g., CA, TX): ").strip().upper()
    
    zip_codes = get_zip_codes_for_county(county, state)
    if zip_codes:
        zip_codes_str = ", ".join(zip_codes)
        print(f"ZIP codes in {county.title()}, {state}:")
        print(zip_codes_str)
    else:
        print(f"No ZIP codes found for {county.title()} County in {state}.")

if __name__ == "__main__":
    main()

