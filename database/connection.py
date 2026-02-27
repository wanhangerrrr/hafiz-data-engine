import os
import logging
import psycopg2
import streamlit as st
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_validated_env():
    """Loads and validates database environment variables. Checks st.secrets first, then .env."""
    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    
    # 1. Check Streamlit Secrets (for Cloud Deployment)
    try:
        if hasattr(st, "secrets") and all(k in st.secrets for k in required_vars):
            logger.info("Using Streamlit Secrets for database connection.")
            return {v: st.secrets[v] for v in required_vars}
    except Exception:
        pass # Not running in streamlit or secrets not set

    # 2. Check local .env (for Local Development)
    root_path = Path(__file__).resolve().parents[1]
    env_path = root_path / ".env"
    
    if not env_path.exists():
        logger.warning(f".env file not found at {env_path}. Fallback to OS environment.")
    else:
        load_dotenv(dotenv_path=env_path)
    
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        msg = f"Missing environment variables: {', '.join(missing)}"
        logger.error(msg)
        raise ValueError(msg)
    
    return {v: os.getenv(v) for v in required_vars}

def get_connection():
    """Returns a production-ready PostgreSQL connection."""
    try:
        creds = load_validated_env()
        # Use DSN string for best compatibility with Neon pooler/SSL
        dsn = f"postgresql://{creds['DB_USER']}:{creds['DB_PASSWORD']}@{creds['DB_HOST']}:{creds['DB_PORT']}/{creds['DB_NAME']}?sslmode=require"
        conn = psycopg2.connect(dsn, connect_timeout=10)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def log_pipeline_start(pipeline_name: str):
    """Logs the start of a pipeline run and returns the run_id."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO pipeline_run_history (pipeline_name, start_time, status) VALUES (%s, %s, %s) RETURNING run_id",
            (pipeline_name, datetime.now(), "RUNNING")
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    except Exception as e:
        logger.error(f"Failed to log pipeline start: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def log_pipeline_end(run_id: int, status: str, error_message: str = None):
    """Logs the end of a pipeline run."""
    if run_id is None:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        end_time = datetime.now()
        cur.execute(
            "SELECT start_time FROM pipeline_run_history WHERE run_id = %s",
            (run_id,)
        )
        start_time = cur.fetchone()[0]
        duration = (end_time - start_time).total_seconds()
        
        cur.execute(
            """
            UPDATE pipeline_run_history 
            SET end_time = %s, duration_seconds = %s, status = %s, error_message = %s 
            WHERE run_id = %s
            """,
            (end_time, duration, status, error_message, run_id)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to log pipeline end: {e}")
    finally:
        cur.close()
        conn.close()

def check_file_hash_exists(file_hash: str) -> bool:
    """Checks if a file hash already exists in successful ingestion logs."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM ingestion_log WHERE file_hash = %s AND status = 'SUCCESS' LIMIT 1",
            (file_hash,)
        )
        return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Failed to check file hash: {e}")
        return False
    finally:
        cur.close()
        conn.close()
