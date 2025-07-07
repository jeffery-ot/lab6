from pyspark.sql.functions import to_date, col
import logging

def load_and_filter_data(spark):
    logger = logging.getLogger(__name__)

    orders = spark.read.format("delta").load("s3a://lab6-curated/staged-orders")
    order_items = spark.read.format("delta").load("s3a://lab6-curated/staged-order-items")
    products = spark.read.format("delta").load("s3a://lab6-curated/staged-products")

    orders = orders.withColumn("order_date", to_date(col("created_at")))
    order_items = order_items.withColumn("order_date", to_date(col("created_at")))
    products = products.withColumn("product_date", to_date(col("ingested_at")))

    date_ranges = []
    for df, label in [(orders, "orders"), (order_items, "order_items")]:
        min_date = df.selectExpr("min(order_date)").first()[0]
        max_date = df.selectExpr("max(order_date)").first()[0]
        logger.info(f"{label} date range: {min_date} to {max_date}")
        date_ranges.append((min_date, max_date))

    common_start = max(r[0] for r in date_ranges)
    common_end = min(r[1] for r in date_ranges)
    logger.info(f"Common processing window: {common_start} to {common_end}")

    orders_filtered = orders.filter((col("order_date") >= common_start) & (col("order_date") <= common_end))
    order_items_filtered = order_items.filter((col("order_date") >= common_start) & (col("order_date") <= common_end))

    return orders_filtered, order_items_filtered, products
