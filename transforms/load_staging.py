import logging
import yaml
from pathlib import Path
from database.connection import get_connection

logger = logging.getLogger("transformation")

def validate_data(records: list[dict], expected_columns: list[str]):
    """Basic Data Quality validation: schema, nulls, and types."""
    if not records:
        raise ValueError("No records found for transformation.")
    
    for i, record in enumerate(records):
        # 1. Schema check
        missing_cols = [col for col in expected_columns if col not in record]
        if missing_cols:
            raise ValueError(f"Record {i} is missing columns: {missing_cols}")
        
        # 2. Null check for key columns (Month)
        if record.get("Month") is None:
            raise ValueError(f"Record {i} has null 'Month'.")
            
        # 3. Type check (Year columns should be numeric)
        for year in ["1958", "1959", "1960"]:
            val = record.get(year)
            if val is not None and not str(val).strip().isdigit() and not isinstance(val, (int, float)):
                raise ValueError(f"Record {i} column {year} has non-numeric value: {val}")

def load_dataset_to_staging(dataset_cfg: dict):
    """Loads records from raw_records to staging table with DQ and load mode handling."""
    source_name = dataset_cfg["name"]
    target_table = dataset_cfg["target_stg"]
    load_mode = dataset_cfg.get("load_mode", "FULL")
    
    expected_cols = ["Month", "1958", "1959", "1960"] # Specific to air_travel_stats

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1. Fetch Raw Data
        # For incremental, we could filter by ingested_at > max(loaded_at)
        # For this implementation, we'll fetch all and let the logic handle it or filter if load_mode is incremental
        if load_mode == "INCREMENTAL":
            # Simple logic: only get records ingested in the last batch (not fully idempotent here but follows requirement)
            # A more robust way: use a watermark table or check existing stg data
            cur.execute(f"SELECT MAX(loaded_at) FROM {target_table}")
            last_load = cur.fetchone()[0]
            if last_load:
                cur.execute("SELECT record FROM raw_records WHERE source_name = %s AND ingested_at > %s", (source_name, last_load))
            else:
                cur.execute("SELECT record FROM raw_records WHERE source_name = %s", (source_name,))
        else:
            cur.execute(f"TRUNCATE {target_table};")
            cur.execute("SELECT record FROM raw_records WHERE source_name = %s", (source_name,))
            
        rows = cur.fetchall()
        if not rows:
            logger.info(f"No new records to load for {source_name} (Mode: {load_mode})")
            return

        records = [row[0] for row in rows]

        # 2. Data Quality Validation
        logger.info(f"Validating {len(records)} records for {source_name}")
        validate_data(records, expected_cols)

        # 3. Insert into Staging
        sql = f"""
            INSERT INTO {target_table} (month, year_1958, year_1959, year_1960)
            VALUES (%s, %s, %s, %s)
        """
        for record in records:
            cur.execute(sql, (
                record["Month"],
                int(record["1958"]),
                int(record["1959"]),
                int(record["1960"])
            ))

        conn.commit()
        logger.info(f"Successfully loaded {len(records)} records to {target_table} (Mode: {load_mode})")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Transformation failed for {source_name}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def main():
    config_path = Path("config/config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    for ds in config.get("datasets", []):
        try:
            load_dataset_to_staging(ds)
        except Exception as e:
            logger.error(f"Failed to process staging for {ds['name']}: {e}")
            continue

if __name__ == "__main__":
    main()