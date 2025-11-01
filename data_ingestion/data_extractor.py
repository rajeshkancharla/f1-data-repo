"""
F1 Data Extraction Script
Pulls data from OpenF1 API and writes to BigQuery with idempotency writes
Uses custom logger from logger.py
"""
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time
import hashlib
import json

# Import custom logger and config
import config
from utilities.logger import setup_logger

# Initialize logger for this module
logger = setup_logger(__name__)


class F1BigQueryExtractor:
    """Extract F1 data from OpenF1 API and load into BigQuery with idempotent writes"""
    
    BASE_URL = config.BASE_URL
    
    # Define primary keys for each table (for idempotency)
    TABLE_PRIMARY_KEYS = config.TABLE_PRIMARY_KEYS
    
    def __init__(
        self, 
        project_id: str,
        dataset_id: str = "f1_raw_data",
        location: str = "US"
    ):
        """
        Instantiate the extractor with GCP Project ID, Dataset ID, and Location
        Creates the BigQuery Client and ensures dataset exists
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=project_id)
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
        
    def _ensure_dataset_exists(self):
        """
        Create BigQuery dataset if it doesn't exist
        """
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.location
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_ref}")
    
    def _generate_extraction_id(self, endpoint: str, params: Dict) -> str:
        """
        Generate a unique extraction ID based on endpoint and parameters
        """
        param_str = json.dumps(params, sort_keys=True)
        content = f"{endpoint}:{param_str}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _clean_data_for_bigquery(self, data: List[Dict]) -> List[Dict]:
        """
        Clean data to handle NULL values and nested structures properly
        """
        if not data:
            return data
        
        cleaned_data = []
        for row in data:
            cleaned_row = {}
            for key, value in row.items():
                if value is None:
                    continue
                elif isinstance(value, (list, dict)):
                    cleaned_row[key] = json.dumps(value)
                else:
                    cleaned_row[key] = value
            
            cleaned_data.append(cleaned_row)
        
        return cleaned_data
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """Make API request with error handling"""
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            response = requests.get(url, params=params, timeout=config.API_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} records from {endpoint}")
            return data
        except requests.exceptions.HTTPError as e:
            if response.status_code == 422:
                raise ValueError(
                    f"API rejected request - too much data. Try smaller date ranges or add more filters."
                )
            raise
        except requests.exceptions.Timeout:
            raise TimeoutError(f"Request to {endpoint} timed out after 90 seconds")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Request failed: {str(e)}")
    
    def _load_to_bigquery_idempotent(
        self, 
        data: List[Dict], 
        table_name: str,
        temp_table_suffix: Optional[str] = None
    ) -> int:
        """
        Load data to BigQuery with IDEMPOTENT behavior using temp table + MERGE
        
        Process:
        1. Load new data to temp table
        2. MERGE temp into main table (upsert based on primary keys)
        3. Drop temp table
        
        Returns number of rows loaded to main table
        """
        if not data:
            logger.info(f"No data to load for {table_name}")
            return 0
        
        # Clean data
        cleaned_data = self._clean_data_for_bigquery(data)
        
        # Create temp table name
        if temp_table_suffix:
            temp_suffix = temp_table_suffix
        else:
            temp_suffix = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        temp_table_name = f"{table_name}_temp_{temp_suffix}"
        
        # Table references
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        temp_table_ref = f"{dataset_ref}.{temp_table_name}"
        main_table_ref = f"{dataset_ref}.{table_name}"
        
        logger.info(f"Loading {len(cleaned_data)} rows to temp table {temp_table_name}")
        
        # Load to temp table
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        
        load_job = self.client.load_table_from_json(
            cleaned_data, 
            temp_table_ref, 
            job_config=job_config
        )
        load_job.result()
        
        # Check if main table exists
        try:
            self.client.get_table(main_table_ref)
            table_exists = True
        except NotFound:
            table_exists = False
        
        if not table_exists:
            # First load - just rename temp to main
            logger.info(f"Main table {table_name} doesn't exist, will create from temp")
            
            # Copy temp to main
            copy_job = self.client.copy_table(temp_table_ref, main_table_ref)
            copy_job.result()
            
            # Get row count
            table = self.client.get_table(main_table_ref)
            rows_loaded = table.num_rows
            
            logger.info(f"[OK] Created {main_table_ref} with {rows_loaded} rows")
        else:
            # Table exists - MERGE
            rows_loaded = self._merge_tables(temp_table_name, table_name)
            logger.info(f"[OK] Merged {rows_loaded} rows into {table_name}")
        
        # Drop temp table
        self.client.delete_table(temp_table_ref, not_found_ok=True)
        
        return rows_loaded
    
    def _merge_tables(self, source_table_name: str, table_name: str) -> int:
        """Perform MERGE operation to upsert data from source to target"""
        primary_keys = self.TABLE_PRIMARY_KEYS.get(table_name, [])
        
        if not primary_keys:
            raise ValueError(f"No primary keys defined for table {table_name}")
        
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        source_table = f"{dataset_ref}.{source_table_name}"
        target_table = f"{dataset_ref}.{table_name}"
        
        # Build MERGE statement
        on_conditions = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
        
        # Get column list
        source_table_obj = self.client.get_table(source_table)
        columns = [field.name for field in source_table_obj.schema]
        
        # UPDATE and INSERT clauses
        update_columns = [col for col in columns if col not in primary_keys]
        update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
        
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])
        
        merge_query = f"""
        MERGE `{target_table}` AS target
        USING `{source_table}` AS source
        ON {on_conditions}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
        
        logger.info(f"Executing MERGE for {table_name}")
        
        query_job = self.client.query(merge_query)
        result = query_job.result()
        rows_affected = query_job.num_dml_affected_rows
        
        return rows_affected
    
    def extract_and_load_drivers(self, session_key: int) -> List[Dict]:
        """Extract driver information and load to BigQuery (IDEMPOTENT)"""
        logger.info(f"Extracting drivers for session {session_key}")
        
        params = {"session_key": session_key}
        drivers = self._make_request("drivers", params)
        
        extraction_id = self._generate_extraction_id("drivers", params)
        for driver in drivers:
            driver['extracted_at'] = datetime.utcnow().isoformat()
            driver['extraction_id'] = extraction_id
        
        self._load_to_bigquery_idempotent(drivers, "drivers")
        
        return drivers
    
    def extract_and_load_laps(self, session_key: int, driver_number: Optional[int] = None) -> List[Dict]:
        """Extract lap data and load to BigQuery (IDEMPOTENT)"""
        logger.info(f"Extracting laps for session {session_key}" + 
                   (f", driver {driver_number}" if driver_number else ""))
        
        params = {"session_key": session_key}
        if driver_number:
            params["driver_number"] = driver_number
        
        laps = self._make_request("laps", params)
        
        extraction_id = self._generate_extraction_id("laps", params)
        for lap in laps:
            lap['extracted_at'] = datetime.utcnow().isoformat()
            lap['extraction_id'] = extraction_id
        
        self._load_to_bigquery_idempotent(laps, "laps")
        
        return laps
    
    def extract_and_load_pits(self, session_key: int, driver_number: Optional[int] = None) -> List[Dict]:
        """Extract pit stop data and load to BigQuery (IDEMPOTENT)"""
        logger.info(f"Extracting pit stops for session {session_key}" + 
                   (f", driver {driver_number}" if driver_number else ""))
        
        params = {"session_key": session_key}
        if driver_number:
            params["driver_number"] = driver_number
        
        pits = self._make_request("pit", params)
        
        extraction_id = self._generate_extraction_id("pit", params)
        for pit in pits:
            pit['extracted_at'] = datetime.utcnow().isoformat()
            pit['extraction_id'] = extraction_id
        
        self._load_to_bigquery_idempotent(pits, "pit")
        
        return pits
    
    def extract_and_load_locations_paginated(
        self, 
        session_key: int, 
        driver_number: int,
        chunk_size_minutes: int = 5
    ) -> int:
        """
        Extract location data with DATE-BASED PAGINATION for a driver (IDEMPOTENT)
        
        Location endpoint only supports: session_key, driver_number, and date filters.
        We paginate by date ranges to avoid fetching too much data at once.
        
        Args:
            session_key: Required - specific session
            driver_number: Required - specific driver
            chunk_size_minutes: Size of each date chunk (default: 5 minutes)
            
        Returns:
            Total number of location records loaded
        """
        logger.info(f"Extracting locations with date pagination for session {session_key}, driver {driver_number}")
        logger.info(f"  Chunk size: {chunk_size_minutes} minutes")
        
        all_locations = []
        
        # First, get the time range for this driver's session by checking their laps
        try:
            laps_params = {
                "session_key": session_key,
                "driver_number": driver_number
            }
            laps = self._make_request("laps", laps_params)
            
            if not laps:
                logger.warning(f"  No laps found for driver {driver_number}, skipping location extraction")
                return 0
            
            # Get min and max dates from laps
            dates = [lap['date_start'] for lap in laps if lap.get('date_start')]
            if not dates:
                logger.warning(f"  No valid lap dates found for driver {driver_number}")
                return 0
            
            start_date = min(dates)
            end_date = max(dates)
            
            logger.info(f"  Session time range: {start_date} to {end_date}")
            
            # Parse dates
            from dateutil import parser
            start_dt = parser.parse(start_date)
            end_dt = parser.parse(end_date)
            
            # Add buffer before and after
            start_dt = start_dt - timedelta(minutes=2)
            end_dt = end_dt + timedelta(minutes=2)
            
            # Fetch locations in chunks
            current_dt = start_dt
            chunk_number = 0
            
            while current_dt < end_dt:
                chunk_number += 1
                chunk_end = min(current_dt + timedelta(minutes=chunk_size_minutes), end_dt)
                
                try:
                    # Build URL with multiple date parameters
                    # OpenF1 API syntax: date>START&date<END
                    # We need to build the URL manually because requests doesn't support duplicate param names
                    base_url = f"{self.BASE_URL}/location"
                    start_iso = current_dt.isoformat()
                    end_iso = chunk_end.isoformat()
                    
                    # Build URL with proper date filters
                    url = f"{base_url}?session_key={session_key}&driver_number={driver_number}&date>={start_iso}&date<{end_iso}"
                    
                    try:
                        response = requests.get(url, timeout=config.API_TIMEOUT)
                        response.raise_for_status()
                        locations = response.json()
                        logger.info(f"Successfully fetched {len(locations)} records from location")
                    except requests.exceptions.HTTPError as e:
                        if response.status_code == 422:
                            raise ValueError(f"API rejected request - try smaller chunk size")
                        raise
                    except requests.exceptions.Timeout:
                        raise TimeoutError(f"Request timed out after 90 seconds")
                    except requests.exceptions.RequestException as e:
                        raise ConnectionError(f"Request failed: {str(e)}")
                    
                    if locations:
                        all_locations.extend(locations)
                        logger.info(f"    Chunk {chunk_number}: {len(locations)} points ({current_dt.strftime('%H:%M')} - {chunk_end.strftime('%H:%M')})")
                    
                    # Small delay to be nice to API
                    time.sleep(config.RATE_LIMIT_DELAY)
                    
                except Exception as e:
                    logger.warning(f"    Chunk {chunk_number} failed: {str(e)}")
                
                current_dt = chunk_end
            
            logger.info(f"  Total locations fetched: {len(all_locations)}")
            
        except Exception as e:
            logger.error(f"  Error in paginated extraction: {str(e)}")
            return 0
        
        # Load all locations at once with idempotent merge
        if all_locations:
            extraction_id = self._generate_extraction_id("location", {
                "session_key": session_key,
                "driver_number": driver_number
            })
            
            for loc in all_locations:
                loc['extracted_at'] = datetime.utcnow().isoformat()
                loc['extraction_id'] = extraction_id
            
            temp_suffix = f"driver{driver_number}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            loaded = self._load_to_bigquery_idempotent(
                all_locations, 
                "locations",
                temp_table_suffix=temp_suffix
            )
            
            logger.info(f"  [OK] Loaded {loaded} location records for driver {driver_number}")
            return loaded
        else:
            logger.warning(f"  No location data found for driver {driver_number}")
            return 0
    
    def get_latest_session_key(self, year: Optional[int] = None) -> Optional[int]:
        """Get the latest session key for a year"""
        if year is None:
            year = datetime.now().year
        
        params = {"year": year}
        sessions = self._make_request("sessions", params)
        
        if sessions:
            return sessions[-1]['session_key']
        return None
    
    def extract_full_session(
        self, 
        session_key: int,
        driver_numbers: Optional[List[int]] = None
    ) -> Dict[str, int]:
        """
        Extract complete session data (IDEMPOTENT) with DATE-PAGINATED location extraction
        
        Args:
            session_key: Session to extract
            driver_numbers: Optional list of specific drivers
            
        Returns:
            Dictionary with counts of records loaded
        """
        logger.info(f"Starting IDEMPOTENT extraction for session {session_key}")
        
        counts = {}
        
        # 1. Extract drivers
        drivers = self.extract_and_load_drivers(session_key)
        counts['drivers'] = len(drivers)
        
        # 2. Get driver list
        if driver_numbers is None:
            driver_numbers = [d['driver_number'] for d in drivers]
            logger.info(f"Found {len(driver_numbers)} drivers in session")
        
        # 3. Extract laps and locations for each driver
        total_laps = 0
        total_locations = 0
        
        for i, driver_num in enumerate(driver_numbers, 1):
            logger.info(f"\nProcessing driver {driver_num} ({i}/{len(driver_numbers)})")
            
            try:
                # Extract laps first
                laps = self.extract_and_load_laps(session_key, driver_num)
                total_laps += len(laps)
                logger.info(f"  [OK] Loaded {len(laps)} laps for driver {driver_num}")
                
                # Extract locations with date-based pagination
                if laps:
                    locations_count = self.extract_and_load_locations_paginated(
                        session_key, 
                        driver_num,
                        chunk_size_minutes=config.LOCATION_CHUNK_SIZE_MINUTES  # Adjust if needed
                    )
                    total_locations += locations_count
                else:
                    logger.warning(f"  No laps found for driver {driver_num}, skipping locations")
                
                # Small delay between drivers
                time.sleep(config.API_RETRY_DELAY)
                
            except Exception as e:
                logger.error(f"Error processing driver {driver_num}: {str(e)}")
                continue
        
        counts['laps'] = total_laps
        counts['locations'] = total_locations
        
        logger.info(f"\n[OK] IDEMPOTENT extraction complete: {counts}")
        logger.info(f"[OK] Safe to re-run - will update existing records, not create duplicates")
        return counts