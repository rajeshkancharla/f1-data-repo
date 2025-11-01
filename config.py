"""
Configuration settings for the F1 Analytics Pipeline.
"""
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "plenti-project")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "f1_raw_data")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")

# OpenF1 API Configuration
BASE_URL = os.getenv("BASE_URL", "https://api.openf1.org/v1")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "90"))  # seconds
API_RETRY_DELAY = float(os.getenv("API_RETRY_DELAY", "0.5"))  # seconds

# Location Extraction Configuration
LOCATION_CHUNK_SIZE_MINUTES = int(os.getenv("LOCATION_CHUNK_SIZE_MINUTES", "5"))
LOCATION_DATE_BUFFER_MINUTES = int(os.getenv("LOCATION_DATE_BUFFER_MINUTES", "2"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE_PREFIX = os.getenv("LOG_FILE_PREFIX", "f1_extraction")
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "True").lower() == "true"
LOG_TO_CONSOLE = os.getenv("LOG_TO_CONSOLE", "True").lower() == "true"

# Create logs directory if it doesn't exist
if LOG_TO_FILE:
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# Extraction Configuration
EXTRACTION_MODE = os.getenv("EXTRACTION_MODE", "session")  # 'session' or 'meeting'
DEFAULT_YEAR = int(os.getenv("DEFAULT_YEAR", str(datetime.now().year)))

# Rate Limiting
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "0.2"))  # seconds between requests

# Table Configuration
TABLE_PRIMARY_KEYS = {
    'drivers': ['session_key', 'driver_number'],
    'laps': ['session_key', 'driver_number', 'lap_number'],
    'locations': ['session_key', 'driver_number', 'date'],
    'pit': ['session_key', 'driver_number', 'date']
}


def get_log_file_path(prefix=None):
    """
    Generate log file path with timestamp.
    
    Args:
        prefix: Optional prefix for log file name
        
    Returns:
        str: Full path to log file
    """
    if prefix is None:
        prefix = LOG_FILE_PREFIX
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.log"
    return os.path.join(LOG_DIR, filename)

