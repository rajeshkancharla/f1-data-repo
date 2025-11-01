"""
Main entry point for F1 data extraction to BigQuery
Supports two modes:
1. Session-based: Extract data for a specific session
2. Meeting-based: Extract race data by country and year

Run examples:
  python run_data_extraction.py --session_key 9472
  python run_data_extraction.py --country "Monaco" --year 2024
"""
import sys
import os
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import config and logger
import config
from utilities.logger import setup_logger

# Now we can import from data_ingestion
from data_ingestion.data_extractor import F1BigQueryExtractor

# Initialize logger for this script
logger = setup_logger(__name__, config.get_log_file_path("run_extraction"))


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Extract F1 data to BigQuery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
MODE 1: Session-based extraction
  Extract all drivers, laps, and locations for a specific session
  
  Examples:
    python run_data_extraction.py --session_key 9472
    python run_data_extraction.py --year 2024  # finds latest session

MODE 2: Meeting-based extraction (by country/year)
  Finds the race session for a specific Grand Prix and extracts:
  - Drivers, laps, locations, AND pit stops
  - Only main race (excludes practice, qualifying, sprint)
  
  Examples:
    python run_data_extraction.py --country "Monaco" --year 2024
    python run_data_extraction.py --country "Singapore" --year 2024
    
  Note: Country matching is fuzzy (e.g., "monaco", "Monaco", or "Monte Carlo" all work)
        """
    )
    
    # Mode selection (mutually exclusive groups)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    
    # Session mode
    mode_group.add_argument(
        '--session_key',
        type=int,
        help='[SESSION MODE] Specific session key to extract (e.g., 9472)'
    )
    
    # Meeting mode - requires both country and year
    mode_group.add_argument(
        '--country',
        type=str,
        help='[MEETING MODE] Country/location name (e.g., "Monaco", "Singapore")'
    )
    
    # Year can be used in both modes
    parser.add_argument(
        '--year',
        type=int,
        help='Year - used with --country for meeting mode, or alone to find latest session'
    )
    
    # BigQuery configuration
    parser.add_argument(
        '--project',
        type=str,
        default='plenti-project',
        help='GCP project ID (default: plenti-project)'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default='f1_raw_data',
        help='BigQuery dataset name (default: f1_raw_data)'
    )
    
    # Output options
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Reduce output verbosity'
    )
    
    args = parser.parse_args()
    
    # Validate meeting mode requires year
    if args.country and not args.year:
        parser.error("--country requires --year to be specified")
    
    return args


def find_race_session_for_meeting(extractor, country, year):
    """
    Find the race session for a specific country/year
    
    Returns: (meeting_key, session_key, meeting_info) or (None, None, None)
    """
    import requests
    
    print(f"\n Searching for {country} Grand Prix in {year}...")
    print("-" * 70)
    
    # Get all meetings for the year
    try:
        meetings = requests.get(
            f"https://api.openf1.org/v1/meetings?year={year}",
            timeout=30
        ).json()
        
        if not meetings:
            print(f"[ERROR] No meetings found for year {year}")
            return None, None, None
        
        print(f"  Found {len(meetings)} meetings in {year}")
        
        # Search for matching country/location (case-insensitive, flexible matching)
        country_lower = country.lower()
        matching_meetings = []
        
        for meeting in meetings:
            location = meeting.get('location', '').lower()
            country_name = meeting.get('country_name', '').lower()
            meeting_name = meeting.get('meeting_name', '').lower()
            
            if (country_lower in location or 
                country_lower in country_name or 
                country_lower in meeting_name):
                matching_meetings.append(meeting)
        
        if not matching_meetings:
            print(f"[ERROR] No meetings found matching '{country}'")
            print("\n[INFO] Available locations in {year}:")
            for m in meetings[:10]:  # Show first 10
                print(f"  - {m.get('meeting_name')} ({m.get('location')}, {m.get('country_name')})")
            return None, None, None
        
        if len(matching_meetings) > 1:
            print(f"[WARNING]  Found {len(matching_meetings)} matching meetings:")
            for m in matching_meetings:
                print(f"  - {m.get('meeting_name')}")
            print(f"  Using: {matching_meetings[0].get('meeting_name')}")
        
        meeting = matching_meetings[0]
        meeting_key = meeting['meeting_key']
        
        print(f"\n[OK] Found meeting: {meeting.get('meeting_name')}")
        print(f"  Location: {meeting.get('location')}, {meeting.get('country_name')}")
        print(f"  Meeting Key: {meeting_key}")
        
        # Get all sessions for this meeting
        sessions = requests.get(
            f"https://api.openf1.org/v1/sessions?meeting_key={meeting_key}",
            timeout=30
        ).json()
        
        if not sessions:
            print(f"[ERROR] No sessions found for this meeting")
            return meeting_key, None, meeting
        
        print(f"\n  Sessions found:")
        race_session = None
        
        for session in sessions:
            session_name = session.get('session_name', '')
            session_key = session.get('session_key')
            print(f"    - {session_name} (key: {session_key})")
            
            # Look for the main race (not Sprint, Practice, or Qualifying)
            if session_name == 'Race':
                race_session = session
        
        if not race_session:
            print(f"\n[ERROR] No 'Race' session found in this meeting")
            print(f"  Available sessions: {[s.get('session_name') for s in sessions]}")
            return meeting_key, None, meeting
        
        session_key = race_session['session_key']
        print(f"\n[OK] Found Race session: {session_key}")
        
        return meeting_key, session_key, meeting
        
    except Exception as e:
        print(f"[ERROR] Error searching for meeting: {e}")
        return None, None, None


def main():
    """Main execution function"""
    
    # Parse command-line arguments
    args = parse_arguments()
    
    PROJECT_ID = args.project
    DATASET_ID = args.dataset
    
    # Determine mode
    meeting_mode = args.country is not None
    
    if not args.quiet:
        print("=" * 70)
        print("F1 Data Extraction to BigQuery")
        print("=" * 70)
        print(f"Project: {PROJECT_ID}")
        print(f"Dataset: {DATASET_ID}")
        print(f"Mode: {'MEETING' if meeting_mode else 'SESSION'}")
        print("=" * 70)
        
        if meeting_mode:
            print("\n Meeting Mode - Will extract:")
            print("  [OK] Drivers")
            print("  [OK] Laps")
            print("  [OK] Locations")
            print("  [OK] Pit stops")
            print("  (Race session only - excludes practice/qualifying)")
        else:
            print("\n Session Mode - Will extract:")
            print("  [OK] Drivers")
            print("  [OK] Laps")
            print("  [OK] Locations")
        print("=" * 70)
    
    # Initialize extractor
    try:
        extractor = F1BigQueryExtractor(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID
        )
        if not args.quiet:
            logger.info("BigQuery connection established")
    except Exception as e:
        logger.error(f"Failed to connect to BigQuery: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure GCP authentication is set up")
        print("2. Check that PROJECT_ID is correct")
        print("3. Verify BigQuery API is enabled")
        return 1
    
    # Determine session key based on mode
    meeting_info = None
    
    if meeting_mode:
        # MEETING MODE: Find race session for country/year
        meeting_key, session_key, meeting_info = find_race_session_for_meeting(
            extractor, 
            args.country, 
            args.year
        )
        
        if not session_key:
            print(f"\n[ERROR] Could not find race session for {args.country} {args.year}")
            return 1
            
    else:
        # SESSION MODE: Use provided or find latest
        if args.session_key:
            session_key = args.session_key
            if not args.quiet:
                print(f"\n[OK] Using provided session key: {session_key}")
        else:
            # Find latest session for the year
            year = args.year if args.year else datetime.now().year
            if not args.quiet:
                print(f"\nFinding latest F1 session for {year}...")
            
            try:
                session_key = extractor.get_latest_session_key(year=year)
                
                if not session_key:
                    print(f"[ERROR] No sessions found for {year}")
                    return 1
                
                if not args.quiet:
                    logger.info(f"Found session key: {session_key}")
                
            except Exception as e:
                print(f"[ERROR] Error fetching sessions: {e}")
                return 1
    
    # Start extraction
    if not args.quiet:
        print("\n" + "=" * 70)
        print(f"Starting data extraction for session {session_key}")
        if meeting_mode and meeting_info:
            print(f"Meeting: {meeting_info.get('meeting_name')} ({args.year})")
        print("=" * 70)
    
    try:
        # ==================================================================
        # STEP 1: Extract ALL drivers for the session
        # ==================================================================
        if not args.quiet:
            print("\n[STEP 1] Extracting all drivers for session...")
            print("-" * 70)
        
        drivers = extractor.extract_and_load_drivers(session_key)
        driver_count = len(drivers)
        
        logger.info(f"Loaded {driver_count} drivers to drivers table")
        
        if driver_count == 0:
            print("[ERROR] No drivers found for this session!")
            return 1
        
        driver_numbers = [d['driver_number'] for d in drivers]
        
        if not args.quiet:
            print(f"\nDrivers to process: {driver_numbers}")
        
        # ==================================================================
        # STEP 2: Loop through each driver to extract LAPS
        # ==================================================================
        if not args.quiet:
            print("\n[STEP 2] Extracting laps for each driver...")
            print("-" * 70)
        
        total_laps = 0
        
        for i, driver_num in enumerate(driver_numbers, 1):
            try:
                if args.quiet:
                    print(f"  Driver {driver_num}: ", end="", flush=True)
                else:
                    print(f"\n  Processing driver {driver_num} ({i}/{driver_count})")
                
                laps = extractor.extract_and_load_laps(session_key, driver_num)
                lap_count = len(laps)
                total_laps += lap_count
                
                if args.quiet:
                    print(f"{lap_count} laps")
                else:
                    print(f"    [OK] {lap_count} laps loaded")
                
            except Exception as e:
                print(f"    [ERROR] Error: {e}")
                continue
        
        logger.info(f"Total laps loaded: {total_laps}")
        
        # ==================================================================
        # STEP 3: Loop through each driver to extract LOCATIONS
        # ==================================================================
        if not args.quiet:
            print("\n[STEP 3] Extracting locations for each driver...")
            print("-" * 70)
            print("  Note: Location extraction can take 3-5 minutes per driver")
            print("=" * 70)
        
        total_locations = 0
        
        for i, driver_num in enumerate(driver_numbers, 1):
            try:
                if args.quiet:
                    print(f"  Driver {driver_num}: ", end="", flush=True)
                else:
                    print(f"\n  Processing driver {driver_num} ({i}/{driver_count})")
                
                locations_count = extractor.extract_and_load_locations_paginated(
                    session_key=session_key,
                    driver_number=driver_num,
                    chunk_size_minutes=5
                )
                
                total_locations += locations_count
                
                if args.quiet:
                    print(f"{locations_count:,} locations")
                else:
                    print(f"    [OK] {locations_count:,} locations loaded")
                
            except Exception as e:
                print(f"    [ERROR] Error: {e}")
                continue
        
        logger.info(f"Total locations loaded: {total_locations:,}")
        
        # ==================================================================
        # STEP 4: Extract PIT STOPS (only in meeting mode)
        # ==================================================================
        total_pits = 0
        
        if meeting_mode:
            if not args.quiet:
                print("\n[STEP 4] Extracting pit stops for all drivers...")
                print("-" * 70)
            
            try:
                pits = extractor.extract_and_load_pits(session_key)
                total_pits = len(pits)
                print(f"[OK] Loaded {total_pits} pit stops to pit table")
                
            except Exception as e:
                print(f"[ERROR] Error extracting pit stops: {e}")
        
        # ==================================================================
        # FINAL SUMMARY
        # ==================================================================
        print("\n" + "=" * 70)
        print("[OK] Data extraction complete!")
        print("=" * 70)
        print(f"  Session Key:      {session_key}")
        if meeting_mode and meeting_info:
            print(f"  Meeting:          {meeting_info.get('meeting_name')}")
            print(f"  Location:         {meeting_info.get('location')}, {meeting_info.get('country_name')}")
        print(f"  Drivers loaded:   {driver_count}")
        print(f"  Laps loaded:      {total_laps:,}")
        print(f"  Locations loaded: {total_locations:,}")
        if meeting_mode:
            print(f"  Pit stops loaded: {total_pits:,}")
        print(f"\n  BigQuery dataset: {PROJECT_ID}.{DATASET_ID}")
        tables_updated = "drivers, laps, locations"
        if meeting_mode:
            tables_updated += ", pit"
        print(f"  Tables updated:   {tables_updated}")
        print("=" * 70)
        
        # Show sample query
        if not args.quiet:
            print("\n Sample BigQuery Query:")
            print("-" * 70)
            if meeting_mode:
                print(f"""
SELECT 
    d.driver_number,
    CONCAT(d.first_name, ' ', d.last_name) as driver_name,
    d.team_name,
    COUNT(DISTINCT l.lap_number) as total_laps,
    MIN(l.lap_duration) as fastest_lap,
    COUNT(DISTINCT p.pit_duration) as pit_stops,
    AVG(p.pit_duration) as avg_pit_duration
FROM `{PROJECT_ID}.{DATASET_ID}.drivers` d
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.laps` l 
    ON d.driver_number = l.driver_number 
    AND d.session_key = l.session_key
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.pit` p
    ON d.driver_number = p.driver_number
    AND d.session_key = p.session_key
WHERE d.session_key = {session_key}
  AND l.lap_duration > 0
GROUP BY d.driver_number, driver_name, d.team_name
ORDER BY fastest_lap ASC;
                """)
            else:
                print(f"""
SELECT 
    d.driver_number,
    CONCAT(d.first_name, ' ', d.last_name) as driver_name,
    COUNT(DISTINCT l.lap_number) as total_laps,
    MIN(l.lap_duration) as fastest_lap,
    AVG(loc.speed) as avg_speed
FROM `{PROJECT_ID}.{DATASET_ID}.drivers` d
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.laps` l 
    ON d.driver_number = l.driver_number 
    AND d.session_key = l.session_key
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.locations` loc
    ON l.driver_number = loc.driver_number
    AND l.session_key = loc.session_key
WHERE d.session_key = {session_key}
  AND l.lap_duration > 0
GROUP BY d.driver_number, driver_name
ORDER BY fastest_lap ASC;
                """)
        
        print("\n" + "=" * 70)
        print(" Extraction complete! Check BigQuery for your data.")
        print("=" * 70)
        
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Error during extraction: {e}")
        if not args.quiet:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)