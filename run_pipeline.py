import logging
import sys
from database.connection import log_pipeline_start, log_pipeline_end
from ingestion.ingest import main as run_ingestion
from transforms.load_staging import main as run_staging
from warehouse.load_warehouse import load_star_schema as run_warehouse

# Configure root logger for the entire pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("pipeline_orchestrator")

def main():
    pipeline_name = "Public Data Platform Master Pipeline"
    run_id = log_pipeline_start(pipeline_name)
    
    logger.info(f"--- Starting Pipeline Run [ID: {run_id}] ---")
    
    try:
        # Step 1: Ingestion
        logger.info("Step 1/3: Ingestion")
        run_ingestion()
        
        # Step 2: Staging Transformation
        logger.info("Step 2/3: Staging Transformation")
        run_staging()
        
        # Step 3: Warehouse Loading
        logger.info("Step 3/3: Warehouse Loading")
        run_warehouse()
        
        log_pipeline_end(run_id, "SUCCESS")
        logger.info(f"--- Pipeline Run [ID: {run_id}] COMPLETED SUCCESSFULY ---")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Pipeline crashed: {error_msg}")
        log_pipeline_end(run_id, "FAILED", error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()
