from pyspark.sql import SparkSession
from config import configure_logging, ensure_s3_path_exists
from utils import load_and_filter_data
from transformation import compute_order_level_kpis, compute_category_level_kpis
import logging

ORDER_KPI_PATH = "s3a://lab6-presentation/order_kpis/"
CATEGORY_KPI_PATH = "s3a://lab6-presentation/category_kpis/"

def main():
    spark = SparkSession.builder \
        .appName("Order Metrics Pipeline") \
        .getOrCreate()

    configure_logging()
    logger = logging.getLogger(__name__)

    logger.info("Loading data and filtering to overlapping date range...")
    orders, order_items, products = load_and_filter_data(spark)

    logger.info("Registering temp views for SQL use...")
    orders.createOrReplaceTempView("orders")
    order_items.createOrReplaceTempView("order_items")
    products.createOrReplaceTempView("products")

    logger.info("Computing order-level KPIs...")
    order_kpis = compute_order_level_kpis(spark)
    ensure_s3_path_exists(ORDER_KPI_PATH)
    order_kpis.write.mode("overwrite").parquet(ORDER_KPI_PATH)
    logger.info(f"Order-level KPIs written to {ORDER_KPI_PATH}")

    logger.info("Computing category-level KPIs...")
    category_kpis = compute_category_level_kpis(spark)
    ensure_s3_path_exists(CATEGORY_KPI_PATH)
    category_kpis.write.mode("overwrite").parquet(CATEGORY_KPI_PATH)
    logger.info(f"Category-level KPIs written to {CATEGORY_KPI_PATH}")

    logger.info("Pipeline execution completed.")

if __name__ == "__main__":
    main()
