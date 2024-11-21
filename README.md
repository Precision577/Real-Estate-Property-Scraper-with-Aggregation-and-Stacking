
# Redfin Property Scraper and Data Aggregator

This repository provides a complete end-to-end system for scraping real estate property data from Redfin and preparing it for integration into CRMs or skip-tracing workflows. It demonstrates advanced backend automation, data engineering techniques, and robust data aggregation pipelines, showcasing the versatility and skill of a full-stack developer.

## Project Overview

This system is designed to:
- Scrape property data from Redfin by ZIP code.
- Generate CRM-ready JSON files, structured by agents and their associated property listings.
- Enrich property details with external data sources, such as PropWire.
- Prepare clean and consistent CSV files for further analysis or integration.
- Enable flexible, scalable, and error-resilient data processing pipelines.

The project streamlines real estate data handling, providing actionable formats to support lead generation, CRM integration, and targeted skip tracing.

## Features

### 1. Scraping by ZIP Code
- Use the `getzipcodes.py` script to retrieve all ZIP codes for a specific county and save them in `zipcodes.txt`.
- Precise area targeting ensures data relevance for specific markets.

### 2. CRM-Ready JSON Structure
- Aggregates scraped property data into a JSON format grouped by agent for seamless CRM integration.

**Example JSON Structure**:
```json
{
 "Agent Name": {
   "Address 1": {
     "address": "123 Main St",
     "city": "Example City",
     "state": "FL",
     "zip_code": 12345,
     "price": "$250,000",
     ...
   },
   "Address 2": {
     ...
   }
 }
}
```

**Key Benefits**:
- Centralized property management.
- Direct compatibility with popular CRMs.
- Grouped data enables detailed agent-based analytics.

### 3. Advanced Data Aggregation
- Merges and enriches property data from multiple sources, such as PropWire.
- Groups properties by agent, making it easy to identify high-value leads.
- Stacks property listings for enhanced skip-tracing workflows.

### 4. Error-Resilient Scraping
- Implements retry mechanisms for failed scraping tasks.
- Automatically detects and logs bot challenges, such as CAPTCHAs, for immediate debugging.
- Maintains a robust workflow to ensure no data is lost during processing.

### 5. Dynamic Data Preparation
- Prepares CSV files with clean, normalized, and validated data.
- Automatically handles missing values and formatting inconsistencies.
- Ensures all data is ready for analysis, CRM uploads, or targeted outreach.

### 6. Asynchronous and Multithreaded Design
- Fully optimized for multicore systems, leveraging threading and asyncio.
- Supports high-performance execution on a 16-core CPU, minimizing runtime for large datasets.
- Asynchronous processing ensures maximum efficiency during scraping and aggregation.

### 7. Comprehensive Logging
- Tracks the lifecycle of each task, ensuring transparency and accountability.
- Logs critical events, such as:
 - Data extraction progress.
 - Retries for failed tasks.
 - Detection of bot challenges or errors.
- Provides detailed logs for debugging and performance monitoring.

### 8. Database Management
- Uses a SQLite database to track processed URLs and avoid duplicate processing.
- Ensures data lineage and idempotency.
- Supports efficient management of large datasets during scraping and processing.

## Project Structure

The project is organized into distinct modules for modularity and ease of use:
```
Redfin-Property-Scraper/
getzipcodes.py               # Retrieve ZIP codes by county and state
scrapeproperty.py            # Scrape property data from gathered URLs
preparecsv.py                # Normalize and clean data for CRM integration
config_toggler.txt           # Configuration file for enabling/disabling features
logs/                        # Directory for logging output
```

## Usage

### Step 1: Install Dependencies
Ensure all required Python libraries are installed:
```bash
pip install -r requirements.txt
```

### Step 2: Retrieve ZIP Codes
Run `getzipcodes.py` to generate a list of ZIP codes for a specific county:
```bash
python getzipcodes.py
```

**Input Example**:
```
Enter the county name: Orange
Enter the state abbreviation (e.g., CA, FL): FL
```

**Output Example**:
```
ZIP codes in Orange, FL:
32789, 32801, 32803, ...
```

Save these ZIP codes in `zipcodes.txt` for use in the scraping workflow.

### Step 3: Run the Main Workflow
Execute the `master.sh` script to start the entire scraping and processing workflow:
```bash
bash master.sh
```

**Workflow Steps**:
1. **Gather URLs**: `gatherurls.py` collects property URLs for the specified ZIP codes and saves them to the database.
2. **Scrape Properties**: `scrapeproperty.py` scrapes property data and saves it to a CSV file.
3. **Aggregate Data**: `aggregateprep.py` aggregates property data by agent and prepares it for CRM integration.
4. **Prepare CSV**: `preparecsv.py` cleans the data for further processing.
5. **Json Builder**: `Databasebuilder.py` expands apon the existing json file or creates it from the generated csv

The script loops continuously to handle new tasks, ensuring all data is processed.

### Step 4: CRM-Ready Data
- Aggregated JSON files are generated with data grouped by agent.
- Cleaned CSV files are saved for CRM upload or skip tracing.

## Error Handling
- Retries are automatically performed for failed tasks.
- Errors are logged in the `logs/` directory for debugging.
- CAPTCHAs and bot detections are prominently logged for visibility.

## Performance Optimization
- Asynchronous design ensures efficient execution, even with large datasets.
- Fully utilizes modern CPUs with multithreading and multiprocessing.
