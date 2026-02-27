import logging
from database.connection import get_connection
from pathlib import Path

logger = logging.getLogger("db_init")

def init_db():
    """Initializes the database schema from schema.sql."""
    schema_path = Path("database/schema.sql")
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return

    conn = get_connection()
    cur = conn.cursor()
    
    try:
        logger.info("Cleaning up existing database objects...")
        # Drop tables in reverse order of dependencies
        cur.execute("""
            DROP TABLE IF EXISTS pipeline_run_history;
            DROP TABLE IF EXISTS fct_air_travel;
            DROP TABLE IF EXISTS dim_month;
            DROP TABLE IF EXISTS stg_airtravel;
            DROP TABLE IF EXISTS raw_records;
            DROP TABLE IF EXISTS ingestion_log;
        """)
        
        logger.info("Initializing database schema...")
        with open(schema_path, "r") as f:
            schema_sql = f.read()
        
        cur.execute(schema_sql)
        conn.commit()
        logger.info("Database schema initialized successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to initialize database: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
