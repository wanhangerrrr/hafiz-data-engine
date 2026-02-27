import sys
from pathlib import Path

# Add project root to sys.path
root_path = Path(__file__).resolve().parents[1]
sys.path.append(str(root_path))

try:
    from database.connection import get_connection, logger
    
    logger.info("Attempting to connect to PostgreSQL...")
    conn = get_connection()
    
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    
    logger.info(f"Successfully connected to: {db_version[0]}")
    
    cur.close()
    conn.close()
    logger.info("Connection test PASSED.")
    
except Exception as e:
    print(f"\n❌ Connection test FAILED: {e}")
    sys.exit(1)