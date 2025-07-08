from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
import logging
import boto3
from decimal import Decimal

def compute_order_level_kpis(spark):
    """Compute order-level KPIs from the staged data"""
    return spark.sql("""
        SELECT
            o.created_at_date AS order_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(SUM(CAST(oi.sale_price AS DOUBLE)), 2) AS total_revenue,
            SUM(CAST(o.num_of_item AS INT)) AS total_items_sold,
            COUNT(DISTINCT o.user_id) AS unique_customers,
            SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returned_orders,
            ROUND(
                (CAST(SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS DOUBLE) /
                 CAST(COUNT(DISTINCT o.order_id) AS DOUBLE)) * 100, 2
            ) AS return_rate
        FROM
            orders o
        JOIN
            order_items oi ON o.order_id = oi.order_id
        GROUP BY
            o.created_at_date
    """)

def compute_category_level_kpis(spark):
    """Compute category-level KPIs from the staged data"""
    return spark.sql("""
        SELECT
            p.category,
            o.created_at_date AS order_date,
            ROUND(SUM(CAST(oi.sale_price AS DOUBLE)), 2) AS daily_revenue,
            ROUND(SUM(CAST(oi.sale_price AS DOUBLE)) / COUNT(DISTINCT oi.order_id), 2) AS avg_order_value,
            ROUND(
                SUM(CASE WHEN oi.status = 'returned' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2
            ) AS avg_return_rate
        FROM order_items oi
        INNER JOIN orders o ON oi.order_id = o.order_id
        INNER JOIN products p ON oi.product_id = p.id
        GROUP BY p.category, o.created_at_date
    """)

def create_delta_table_if_not_exists(spark, path, df, table_name):
    """Create Delta table if it doesn't exist"""
    logger = logging.getLogger(__name__)
    try:
        # Try to load existing table
        DeltaTable.forPath(spark, path)
        logger.info(f"Delta table {table_name} already exists at {path}")
        return True
    except Exception as e:
        # Table doesn't exist, create it
        logger.info(f"Creating new Delta table {table_name} at {path}")
        try:
            df.write.format("delta").mode("overwrite").save(path)
            logger.info(f"Delta table {table_name} created successfully")
            return True
        except Exception as create_error:
            logger.error(f"Failed to create Delta table {table_name}: {create_error}")
            return False

def upsert_to_delta(spark, df, path, merge_keys, table_name):
    """Perform upsert operation to Delta table"""
    logger = logging.getLogger(__name__)
    try:
        # Load existing Delta table
        delta_table = DeltaTable.forPath(spark, path)
        
        # Build merge condition
        merge_condition = " AND ".join([f"existing.{key} = updates.{key}" for key in merge_keys])
        
        # Add timestamp for tracking
        df_with_timestamp = df.withColumn("last_updated", current_timestamp())
        
        # Perform merge (upsert)
        delta_table.alias("existing").merge(
            df_with_timestamp.alias("updates"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        logger.info(f"Upsert completed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error during upsert for {table_name}: {str(e)}")
        return False

def convert_to_dynamodb_format(row):
    """Convert PySpark Row to DynamoDB format"""
    item = {}
    for key, value in row.asDict().items():
        if value is not None:
            if isinstance(value, (int, float)):
                item[key] = Decimal(str(value))
            elif isinstance(value, str):
                item[key] = value
            else:
                item[key] = str(value)
    return item

def create_dynamodb_table_if_not_exists(dynamodb_client, table_name, key_schema, attribute_definitions):
    """Create DynamoDB table if it doesn't exist"""
    logger = logging.getLogger(__name__)
    try:
        # Check if table exists
        dynamodb_client.describe_table(TableName=table_name)
        logger.info(f"DynamoDB table {table_name} already exists")
        return True
    except dynamodb_client.exceptions.ResourceNotFoundException:
        # Table doesn't exist, create it
        logger.info(f"Creating DynamoDB table {table_name}")
        try:
            dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode='PAY_PER_REQUEST'
            )
            # Wait for table to be created
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            logger.info(f"DynamoDB table {table_name} created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create DynamoDB table {table_name}: {e}")
            return False

def write_to_dynamodb(df, table_name, batch_size=25):
    """Write DataFrame to DynamoDB"""
    logger = logging.getLogger(__name__)
    try:
        # Initialize DynamoDB clients
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_resource = boto3.resource('dynamodb')
        
        # Define table schemas based on table name
        if "order_kpis" in table_name:
            key_schema = [
                {'AttributeName': 'order_date', 'KeyType': 'HASH'}
            ]
            attribute_definitions = [
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ]
        else:  # category_kpis
            key_schema = [
                {'AttributeName': 'category', 'KeyType': 'HASH'},
                {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
            ]
            attribute_definitions = [
                {'AttributeName': 'category', 'AttributeType': 'S'},
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ]
        
        # Create table if it doesn't exist
        if not create_dynamodb_table_if_not_exists(dynamodb_client, table_name, key_schema, attribute_definitions):
            logger.error(f"Failed to ensure DynamoDB table {table_name} exists")
            return False
        
        # Get table reference
        table = dynamodb_resource.Table(table_name)
        
        # Convert DataFrame to list of dictionaries
        records = df.collect()
        logger.info(f"Writing {len(records)} records to DynamoDB table {table_name}")
        
        # Write in batches
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            try:
                with table.batch_writer() as batch_writer:
                    for record in batch:
                        item = convert_to_dynamodb_format(record)
                        batch_writer.put_item(Item=item)
                
                logger.info(f"Wrote batch {i//batch_size + 1} to DynamoDB")
            except Exception as batch_error:
                logger.error(f"Error writing batch {i//batch_size + 1} to DynamoDB: {batch_error}")
                return False
        
        logger.info(f"Successfully wrote all records to DynamoDB table {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing to DynamoDB table {table_name}: {str(e)}")
        return False

def process_order_kpis(spark, s3_path, dynamodb_table_name):
    """Complete processing pipeline for order KPIs"""
    logger = logging.getLogger(__name__)
    logger.info("Computing order-level KPIs...")
    
    try:
        # Compute KPIs
        order_kpis = compute_order_level_kpis(spark)
        
        # Create Delta table if needed
        if not create_delta_table_if_not_exists(spark, s3_path, order_kpis, "order_kpis"):
            logger.error("Failed to create Delta table for order KPIs")
            return None
        
        # Upsert to Delta table
        if not upsert_to_delta(spark, order_kpis, s3_path, ["order_date"], "order_kpis"):
            logger.error("Failed to upsert order KPIs to Delta table")
            return None
        
        logger.info(f"Order-level KPIs upserted to {s3_path}")
        
        # Write to DynamoDB
        if not write_to_dynamodb(order_kpis, dynamodb_table_name):
            logger.error("Failed to write order KPIs to DynamoDB")
            return None
        
        logger.info("Order-level KPIs processing completed successfully")
        return order_kpis
        
    except Exception as e:
        logger.error(f"Error processing order KPIs: {str(e)}")
        return None

def process_category_kpis(spark, s3_path, dynamodb_table_name):
    """Complete processing pipeline for category KPIs"""
    logger = logging.getLogger(__name__)
    logger.info("Computing category-level KPIs...")
    
    try:
        # Compute KPIs
        category_kpis = compute_category_level_kpis(spark)
        
        # Create Delta table if needed
        if not create_delta_table_if_not_exists(spark, s3_path, category_kpis, "category_kpis"):
            logger.error("Failed to create Delta table for category KPIs")
            return None
        
        # Upsert to Delta table
        if not upsert_to_delta(spark, category_kpis, s3_path, ["category", "order_date"], "category_kpis"):
            logger.error("Failed to upsert category KPIs to Delta table")
            return None
        
        logger.info(f"Category-level KPIs upserted to {s3_path}")
        
        # Write to DynamoDB
        if not write_to_dynamodb(category_kpis, dynamodb_table_name):
            logger.error("Failed to write category KPIs to DynamoDB")
            return None
        
        logger.info("Category-level KPIs processing completed successfully")
        return category_kpis
        
    except Exception as e:
        logger.error(f"Error processing category KPIs: {str(e)}")
        return None