import logging
from database.connection import get_connection

logger = logging.getLogger("warehouse")

def load_star_schema():
    """Populates dim_month and fct_air_travel from stg_airtravel."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        logger.info("Starting Warehouse load (Star Schema)")

        # 1. Populate Dimension: dim_month
        # Use ON CONFLICT to skip existing months
        cur.execute("""
            INSERT INTO dim_month (month_name)
            SELECT DISTINCT month FROM stg_airtravel
            ON CONFLICT (month_name) DO NOTHING;
        """)
        
        # 2. Populate Fact: fct_air_travel
        # We'll use a simple "INSERT IF NOT EXISTS" logic based on month and year to avoid duplicates in fact
        # Note: year columns in staging are year_1958, year_1959, year_1960. 
        # We need to unpivot them into the fact table.
        
        years = [1958, 1959, 1960]
        for year in years:
            sql = f"""
                INSERT INTO fct_air_travel (month_id, year_val, passenger_count)
                SELECT d.month_id, {year}, s.year_{year}
                FROM stg_airtravel s
                JOIN dim_month d ON s.month = d.month_name
                WHERE NOT EXISTS (
                    SELECT 1 FROM fct_air_travel f
                    WHERE f.month_id = d.month_id AND f.year_val = {year}
                );
            """
            cur.execute(sql)

        conn.commit()
        logger.info("Warehouse load completed successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Warehouse load failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    load_star_schema()
