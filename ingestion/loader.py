import json
import logging
from database.connection import get_connection

logger = logging.getLogger(__name__)

def insert_batch_raw(source_name: str, rows: list[dict]):
    """Inserts records into raw_records table using JSONB format."""
    if not rows:
        logger.warning("No records to insert.")
        return 0

    conn = get_connection()
    cur = conn.cursor()
    
    try:
        sql = """
            INSERT INTO raw_records (source_name, record)
            VALUES (%s, %s::jsonb)
        """
        batch_data = [(source_name, json.dumps(row)) for row in rows]
        
        cur.executemany(sql, batch_data)
        conn.commit()
        
        logger.info(f"Successfully inserted {len(rows)} records for {source_name}")
        return len(rows)
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert batch: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def log_ingestion_status(source_name: str, file_name: str, file_hash: str, status: str, count: int = 0, notes: str = ""):
    """Logs the result of an ingestion process to the database."""
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        sql = """
            INSERT INTO ingestion_log (source_name, file_name, file_hash, status, records_count, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.execute(sql, (source_name, file_name, file_hash, status, count, notes))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to log ingestion status: {e}")
    finally:
        cur.close()
        conn.close()
