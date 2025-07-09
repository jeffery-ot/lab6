import logging

def load_and_filter_data(spark):
    logger = logging.getLogger(__name__)

    def try_load(path):
        try:
            df = spark.read.format("delta").load(path)
            logger.info(f"Loaded data from {path} with {df.count()} rows")
            return df
        except Exception as e:
            logger.warning(f"Failed to load {path}: {e}")
            return None

    orders = try_load("s3a://lab6-curated/staged-orders")
    order_items = try_load("s3a://lab6-curated/staged-order-items")
    products = try_load("s3a://lab6-curated/staged-products")

    # Apply date range filtering only if all required data is present
    if orders is not None and order_items is not None:
        orders.createOrReplaceTempView("orders")
        order_items.createOrReplaceTempView("order_items")

        overlap_sql = """
        SELECT
            MAX(o.created_at_date) AS max_order_date,
            MIN(oi.created_at_date) AS min_order_item_date
        FROM
            orders o
        INNER JOIN order_items oi ON o.order_id = oi.order_id
        """
        date_range = spark.sql(overlap_sql).first()
        if date_range and date_range["min_order_item_date"] and date_range["max_order_date"]:
            min_date = date_range["min_order_item_date"]
            max_date = date_range["max_order_date"]
            orders = orders.filter(f"created_at_date BETWEEN DATE('{min_date}') AND DATE('{max_date}')")
            order_items = order_items.filter(f"created_at_date BETWEEN DATE('{min_date}') AND DATE('{max_date}')")
            logger.info(f"Filtered to overlapping date range: {min_date} to {max_date}")
        else:
            logger.warning("Date overlap query returned no results. Skipping filtering.")

    return orders, order_items, products
