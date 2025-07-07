from pyspark.sql import SparkSession
from config import configure_logging, upload_log_to_s3
from utils import load_and_filter_data
from transformation import compute_order_level_kpis, compute_category_level_kpis

def main():
    configure_logging()
    import logging
    logger = logging.getLogger(__name__)

    logger.info("Starting KPI pipeline...")

    try:
        spark = SparkSession.builder \
            .appName("KPI Metrics Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

        orders, order_items, products = load_and_filter_data(spark)

        orders.createOrReplaceTempView("orders")
        order_items.createOrReplaceTempView("order_items")
        products.createOrReplaceTempView("products")

        logger.info("Computing order-level KPIs...")
        order_kpis = compute_order_level_kpis(spark)
        order_kpis.show()
        order_kpis.write.mode("overwrite").format("parquet") \
            .save("s3a://lab6-presentation/order_kpis")

        logger.info("Computing category-level KPIs...")
        category_kpis = compute_category_level_kpis(spark)
        category_kpis.show()
        category_kpis.write.mode("overwrite").format("parquet") \
            .save("s3a://lab6-presentation/category_kpis")

        logger.info("KPI pipeline completed successfully.")

    except Exception as e:
        logger.exception("KPI pipeline failed.")
        raise
    finally:
        upload_log_to_s3()

if __name__ == "__main__":
    main()
