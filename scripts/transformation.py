def compute_order_level_kpis(spark):
    return spark.sql("""
        SELECT 
            o.order_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(oi.sale_price) AS total_revenue,
            SUM(o.num_of_item) AS total_items_sold,
            COUNT(DISTINCT o.user_id) AS unique_customers,
            SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returned_orders,
            ROUND(SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) / COUNT(DISTINCT o.order_id) * 100, 2) AS return_rate
        FROM orders o
        INNER JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.created_at_date
    """)

def compute_category_level_kpis(spark):
    return spark.sql("""
        SELECT
            p.category,
            oi.order_date,
            ROUND(SUM(oi.sale_price), 2) AS daily_revenue,
            ROUND(SUM(oi.sale_price) / COUNT(DISTINCT oi.order_id), 2) AS avg_order_value,
            ROUND(SUM(CASE WHEN oi.status = 'returned' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS avg_return_rate
        FROM order_items oi
        INNER JOIN orders o ON oi.order_id = o.order_id
        INNER JOIN products p ON oi.product_id = p.id
        GROUP BY p.category, oi.created_at_date
    """)
