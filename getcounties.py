import pgeocode

# Initialize the ZIP code lookup for the United States
nomi = pgeocode.Nominatim('us')

# Read the ZIP codes from a text file, splitting them by commas
with open('zipcodes.txt', 'r') as file:
    zip_codes = file.read().strip().split(', ')

# Dictionary to store the count of ZIP codes per county and state
county_state_zip_count = {}
unidentified_zips = []

# Iterate through each ZIP code and get the corresponding county and state
for zip_code in zip_codes:
    zipcode_info = nomi.query_postal_code(zip_code.strip())
    county = zipcode_info.county_name
    state = zipcode_info.state_name
    if county and state and county != 'nan' and state != 'nan':
        county_state = f"{county}, {state}"
        if county_state in county_state_zip_count:
            county_state_zip_count[county_state] += 1
        else:
            county_state_zip_count[county_state] = 1
    else:
        unidentified_zips.append(zip_code)

# Print the counties, states, and the number of ZIP codes for each county-state combination
for county_state, count in county_state_zip_count.items():
    print(f"{county_state}: {count} ZIP codes")

# Print unidentified ZIP codes
if unidentified_zips:
    print("Unidentified ZIP codes:", unidentified_zips)

