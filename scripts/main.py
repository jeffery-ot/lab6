from pyspark.sql import SparkSession
from config import configure_logging, ensure_s3_path_exists, upload_logs_to_s3
from utils import load_and_filter_data
from transformation import process_order_kpis, process_category_kpis
import logging

# Configuration
ORDER_KPI_PATH = "s3a://lab6-presentation/order_kpis_delta/"
CATEGORY_KPI_PATH = "s3a://lab6-presentation/category_kpis_delta/"
ORDER_KPI_DYNAMODB_TABLE = "order_kpis"
CATEGORY_KPI_DYNAMODB_TABLE = "category_kpis"

def main():
    # Configure logging first
    configure_logging()
    logger = logging.getLogger(__name__)
    
    # Initialize Spark with Delta Lake support
    spark = SparkSession.builder \
        .appName("Order Metrics Pipeline with Upsert") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        logger.info("Starting Order Metrics Pipeline...")
        logger.info("Loading staged Delta data...")
        orders, order_items, products = load_and_filter_data(spark)
        
        # Ensure S3 paths exist
        ensure_s3_path_exists(ORDER_KPI_PATH)
        ensure_s3_path_exists(CATEGORY_KPI_PATH)
        
        # Track success/failure
        order_kpis_success = False
        category_kpis_success = False
        
        # Process order-level KPIs
        if orders is not None and order_items is not None:
            logger.info("Setting up temp views for order processing...")
            orders.createOrReplaceTempView("orders")
            order_items.createOrReplaceTempView("order_items")
            
            result = process_order_kpis(spark, ORDER_KPI_PATH, ORDER_KPI_DYNAMODB_TABLE)
            order_kpis_success = result is not None
            
            if order_kpis_success:
                logger.info("Order-level KPIs processed successfully")
            else:
                logger.error("Order-level KPIs processing failed")
        else:
            logger.warning("Skipping order-level KPIs due to missing data.")
        
        # Process category-level KPIs
        if orders is not None and order_items is not None and products is not None:
            logger.info("Setting up temp views for category processing...")
            products.createOrReplaceTempView("products")
            
            result = process_category_kpis(spark, CATEGORY_KPI_PATH, CATEGORY_KPI_DYNAMODB_TABLE)
            category_kpis_success = result is not None
            
            if category_kpis_success:
                logger.info("Category-level KPIs processed successfully")
            else:
                logger.error("Category-level KPIs processing failed")
        else:
            logger.warning("Skipping category-level KPIs due to missing data.")
        
        # Final status
        if order_kpis_success and category_kpis_success:
            logger.info("Pipeline execution completed successfully - All KPIs processed")
        elif order_kpis_success or category_kpis_success:
            logger.warning("Pipeline execution completed with partial success")
        else:
            logger.error("Pipeline execution failed - No KPIs processed successfully")
            raise Exception("Pipeline failed to process any KPIs")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        
        # Upload logs to S3
        upload_logs_to_s3()

if __name__ == "__main__":
    main()