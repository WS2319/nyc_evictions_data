import pandas as pd
import requests
from google.cloud import bigquery

def run_eviction_pipeline(event, context):
    """
    Cloud Function to fetch NYC eviction data and load it into BigQuery.
    Triggered by a Cloud Scheduler job.
    """
    
    # --- CONFIGURATION ---
    project_id = 'nyc-eviction-updates'
    table_id = 'nyc-eviction-updates.nyc_evictions_data.eviction_records'
    api_url = "https://data.cityofnewyork.us/resource/6z8x-wfk4.json"
    
    # --- 1. FETCH DATA (PAGINATION) ---
    page_size = 50000
    offset = 0
    all_data_dfs = []
    print("Starting to fetch all eviction data...")

    while True:
        params = {"$limit": page_size, "$offset": offset}
        print(f"Fetching data with offset: {offset}...")
        
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print("No more data to fetch. Loop finished.")
                break
            
            df_page = pd.DataFrame(data)
            all_data_dfs.append(df_page)
            
            if len(df_page) < page_size:
                print("Fetched the last page of data.")
                break
            
            offset += page_size
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred during API call: {e}")
            return  # Exit function on error

    if not all_data_dfs:
        print("No data was retrieved. Exiting.")
        return

    final_df = pd.concat(all_data_dfs, ignore_index=True)
    print(f"Total rows retrieved: {len(final_df)}")

    # --- 2. PREPARE DATAFRAME ---
    bq_columns = [
        'executed_date', 'borough', 'residential_commercial_ind', 'eviction_zip',
        'latitude', 'longitude', 'ejectment', 'council_district', 'court_index_number'
    ]
    df_to_upload = final_df[bq_columns].copy()
    
    # Data Type Correction
    df_to_upload['executed_date'] = pd.to_datetime(df_to_upload['executed_date'])
    df_to_upload['eviction_zip'] = df_to_upload['eviction_zip'].astype(str)
    df_to_upload['latitude'] = pd.to_numeric(df_to_upload['latitude'], errors='coerce')
    df_to_upload['longitude'] = pd.to_numeric(df_to_upload['longitude'], errors='coerce')
    print("DataFrame prepped for upload.")

    # --- 3. LOAD DATA INTO BIGQUERY ---
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    
    print(f"Loading {len(df_to_upload)} rows into {table_id}...")
    load_job = client.load_table_from_dataframe(df_to_upload, table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    
    destination_table = client.get_table(table_id)
    print(f"Success! Loaded {destination_table.num_rows} rows into BigQuery.")
