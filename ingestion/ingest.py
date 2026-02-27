import os
import hashlib
import logging
import requests
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from ingestion.loader import insert_batch_raw, log_ingestion_status
from database.connection import check_file_hash_exists

# Configure logging to file and console
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ingestion")

def calculate_sha256(file_path: str) -> str:
    """Calculates SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def download_file(url: str, dest_path: Path):
    """Downloads file from URL with timeout."""
    try:
        logger.info(f"Downloading from {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        with open(dest_path, "wb") as f:
            f.write(response.content)
        
        if dest_path.stat().st_size == 0:
            raise ValueError("Downloaded file is empty.")
            
        logger.info(f"Download complete: {dest_path}")
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise

def ingest_dataset(dataset_cfg: dict, storage_cfg: dict):
    """Handles ingestion for a single dataset with idempotency check."""
    source_name = dataset_cfg["name"]
    url = dataset_cfg["url"]
    raw_dir = Path(storage_cfg["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{source_name}_{timestamp}.csv"
    dest_path = raw_dir / file_name

    file_hash = ""
    records_count = 0
    
    try:
        # 1. Download to temp location or direct
        download_file(url, dest_path)
        file_hash = calculate_sha256(str(dest_path))

        # 2. Idempotency Check
        if check_file_hash_exists(file_hash):
            logger.info(f"File with hash {file_hash} already ingested. Skipping.")
            log_ingestion_status(source_name, file_name, file_hash, "SKIPPED", 0, "Duplicate file hash detected.")
            if dest_path.exists():
                dest_path.unlink()
            return

        # 3. Parse Data
        logger.info(f"Parsing CSV data from {dest_path}")
        df = pd.read_csv(dest_path)
        
        # Clean column names (strip whitespace and extra quotes)
        df.columns = [col.strip().replace('"', '') for col in df.columns]
        
        if df.empty:
            raise ValueError("Parsed dataframe is empty.")
        
        records = df.to_dict(orient="records")
        records_count = len(records)

        # 4. Load to DB
        insert_batch_raw(source_name, records)
        
        # 5. Log Success
        log_ingestion_status(source_name, file_name, file_hash, "SUCCESS", records_count, "Ingestion completed successfully.")
        logger.info(f"Ingestion successful for {source_name}. Total records: {records_count}")

    except Exception as e:
        logger.error(f"Ingestion failed for {source_name}: {e}")
        log_ingestion_status(source_name, file_name, file_hash, "FAILED", 0, str(e))
        if dest_path.exists():
            dest_path.unlink()
        raise

def main():
    # Load configuration
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    datasets = config.get("datasets", [])
    storage = config["storage"]
    
    for ds in datasets:
        try:
            ingest_dataset(ds, storage)
        except Exception as e:
            logger.error(f"Failed to ingest dataset {ds['name']}: {e}")
            # Continue with other datasets if one fails
            continue

if __name__ == "__main__":
    main()