#!/bin/bash

# Configuration for debug mode and script toggles
DEBUG_MODE=False  # Set to True to enable debug mode
RUN_EXPIRED_LISTINGS=True  # Set to False to disable running expired listings script
RUN_EXPIRED_WEBHOOK=True   # Set to False to disable running expired listings webhook

# Define paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CSV_PATH="$SCRIPT_DIR/locations.csv"
CONFIG_PATH="$SCRIPT_DIR/config_toggler.txt"
GATHER_URLS_SCRIPT="$SCRIPT_DIR/gatherurls.py"
SCRAPE_PROPERTY_SCRIPT="$SCRIPT_DIR/scrapeproperty.py"
AGGREGATE_PREPARE_SCRIPT="$SCRIPT_DIR/aggeratepropwireandprepareforskiptracing.py"
PREPARE_CSV_SCRIPT="$SCRIPT_DIR/preparecsv.py" 
DATABASE_BUILDER_SCRIPT="$SCRIPT_DIR/databasebuilder.py"
DATABASE_PATH="$SCRIPT_DIR/processed_urls.db"

# Configurable variables
TIMEOUT_DURATION=3600

# Log function
log() {
    echo "$(date "+%Y-%m-%d %H:%M:%S") - $1"
}

# Check webhook file existence
check_webhook_file() {
    WEBHOOK_PATH="$SCRIPT_DIR/webhook.txt"
    if [ ! -s "$WEBHOOK_PATH" ]; then
        log "Webhook file is empty or does not exist. Exiting script as the county is inactive."
        exit 0
    fi
}

# Read configuration settings
read_config() {
    if [ ! -f "$CONFIG_PATH" ]; then
        log "Configuration file not found."
        exit 1
    fi

    SKIPTRACING=$(awk -F'=' '/skiptracing/ {print $2}' $CONFIG_PATH | tr -d '[:space:]')
    A2P=$(awk -F'=' '/A2p/ {print $2}' $CONFIG_PATH | tr -d '[:space:]')

    log "Configurations: Skiptracing=$SKIPTRACING, A2P=$A2P"
}

# Function to run individual scripts repeatedly until they succeed
run_script() {
    local script_path=$1

    log "Running script: $script_path"
    until timeout $TIMEOUT_DURATION python3 "$script_path"; do
        log "Error: Failed to run script $script_path, retrying..."
        sleep 10  # Optional: wait 10 seconds before retrying
    done
    log "Script $script_path completed successfully."
}

# Check if there are URLs to process
check_urls_left() {
    URL_COUNT=$(sqlite3 "$DATABASE_PATH" "SELECT COUNT(*) FROM to_be_processed;" 2>/dev/null)
    if [[ $? -ne 0 ]]; then
        log "Error: Failed to check URLs left. The table 'to_be_processed' might not exist."
        return 1
    fi

    if [[ "$URL_COUNT" -gt 0 ]]; then
        return 0  # URLs left
    else
        return 1  # No URLs left
    fi
}

# Function to run scrapeproperty.py script until URLs are processed
run_scrape_until_done() {
    while check_urls_left; do
        log "URLs left to process. Running scrapeproperty.py"
        run_script "$SCRAPE_PROPERTY_SCRIPT"
    done
    log "No more URLs to process."
}

# Main execution sequence
while true; do
    check_webhook_file
    read_config

    # Run the scripts in the correct order
    run_script "$GATHER_URLS_SCRIPT"  # First script to create the database and populate it with URLs
    run_scrape_until_done             # Run scrapeproperty.py until all URLs are processed

    log "Scrape complete. Proceeding with other scripts."
    
    # Continue running other scripts in sequence
    run_script "$AGGREGATE_PREPARE_SCRIPT"
    run_script "$PREPARE_CSV_SCRIPT"
    run_script "$DATABASE_BUILDER_SCRIPT"

    log "Processing complete. Looping to start over."
    
    log "Waiting for the next cycle..."
    sleep 300  # Wait for 5 minutes before starting the next cycle
done

