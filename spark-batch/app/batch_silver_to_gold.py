"""
Batch ETL: Silver -> Gold
Thực hiện transformations và aggregations để tạo Gold layer (analytical data)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType
from datetime import datetime


def transform_customers(spark, silver_path, gold_path):
    """
    Transform customers data
    """
    print("\n--- Processing Customers ---")
    df = spark.read.format("delta").load(f"{silver_path}/northwind.public.customers")
    
    # Clean and transform
    transformed = df.select(
        F.col("customer_id").alias("customer_id"),
        F.col("company_name").alias("company_name"),
        F.col("contact_name").alias("contact_name"),
        F.col("contact_title").alias("contact_title"),
        F.col("address").alias("address"),
        F.col("city").alias("city"),
        F.col("region").alias("region"),
        F.col("postal_code").alias("postal_code"),
        F.col("country").alias("country"),
        F.col("phone").alias("phone"),
        F.col("fax").alias("fax")
    )
    
    transformed.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")
    print(f"✓ Customers: {transformed.count()} records")
    return transformed


def transform_products(spark, silver_path, gold_path):
    """
    Transform products with categories
    """
    print("\n--- Processing Products ---")
    products = spark.read.format("delta").load(f"{silver_path}/northwind.public.products")
    categories = spark.read.format("delta").load(f"{silver_path}/northwind.public.categories")
    suppliers = spark.read.format("delta").load(f"{silver_path}/northwind.public.suppliers")
    
    # Join products with categories and suppliers
    transformed = products.alias("p") \
        .join(categories.alias("c"), F.col("p.category_id") == F.col("c.category_id"), "left") \
        .join(suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.supplier_id"), "left") \
        .select(
            F.col("p.product_id").cast(IntegerType()).alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("c.category_id").cast(IntegerType()).alias("category_id"),
            F.col("c.category_name").alias("category_name"),
            F.col("c.description").alias("category_description"),
            F.col("s.supplier_id").cast(IntegerType()).alias("supplier_id"),
            F.col("s.company_name").alias("supplier_name"),
            F.col("s.country").alias("supplier_country"),
            F.col("p.quantity_per_unit").alias("quantity_per_unit"),
            F.col("p.unit_price").cast(DoubleType()).alias("unit_price"),
            F.col("p.units_in_stock").cast(IntegerType()).alias("units_in_stock"),
            F.col("p.units_on_order").cast(IntegerType()).alias("units_on_order"),
            F.col("p.reorder_level").cast(IntegerType()).alias("reorder_level"),
            F.col("p.discontinued").cast(IntegerType()).alias("discontinued")
        )
    
    transformed.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_products")
    print(f"✓ Products: {transformed.count()} records")
    return transformed


def transform_employees(spark, silver_path, gold_path):
    """
    Transform employees data
    """
    print("\n--- Processing Employees ---")
    
    # Check if table exists
    try:
        df = spark.read.format("delta").load(f"{silver_path}/northwind.public.employees")
    except Exception as e:
        print(f"⚠ Skipping employees: {str(e)}")
        return None
    
    transformed = df.select(
        F.col("employee_id").cast(IntegerType()).alias("employee_id"),
        F.col("last_name").alias("last_name"),
        F.col("first_name").alias("first_name"),
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
        F.col("title").alias("title"),
        F.col("title_of_courtesy").alias("title_of_courtesy"),
        # birth_date và hire_date là số ngày từ epoch, convert sang date
        F.when(F.col("birth_date").cast(IntegerType()).isNotNull(), 
               F.expr("date_add('1970-01-01', CAST(birth_date AS INT))")).alias("birth_date"),
        F.when(F.col("hire_date").cast(IntegerType()).isNotNull(), 
               F.expr("date_add('1970-01-01', CAST(hire_date AS INT))")).alias("hire_date"),
        F.col("address").alias("address"),
        F.col("city").alias("city"),
        F.col("region").alias("region"),
        F.col("postal_code").alias("postal_code"),
        F.col("country").alias("country"),
        F.col("home_phone").alias("home_phone"),
        F.col("reports_to").cast(IntegerType()).alias("reports_to")
    )
    
    transformed.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_employees")
    print(f"✓ Employees: {transformed.count()} records")
    return transformed


def create_fact_orders(spark, silver_path, gold_path):
    """
    Create fact table for orders with order details
    """
    print("\n--- Creating Fact Orders ---")
    orders = spark.read.format("delta").load(f"{silver_path}/northwind.public.orders")
    order_details = spark.read.format("delta").load(f"{silver_path}/northwind.public.order_details")
    
    # Join orders with order details
    fact = orders.alias("o") \
        .join(order_details.alias("od"), F.col("o.order_id") == F.col("od.order_id"), "inner") \
        .select(
            F.col("o.order_id").cast(IntegerType()).alias("order_id"),
            F.col("o.customer_id").alias("customer_id"),
            F.col("o.employee_id").cast(IntegerType()).alias("employee_id"),
            F.col("o.order_date").cast(DateType()).alias("order_date"),
            F.col("o.required_date").cast(DateType()).alias("required_date"),
            F.col("o.shipped_date").cast(DateType()).alias("shipped_date"),
            F.col("o.ship_via").cast(IntegerType()).alias("shipper_id"),
            F.col("o.freight").cast(DoubleType()).alias("freight"),
            F.col("o.ship_name").alias("ship_name"),
            F.col("o.ship_address").alias("ship_address"),
            F.col("o.ship_city").alias("ship_city"),
            F.col("o.ship_region").alias("ship_region"),
            F.col("o.ship_postal_code").alias("ship_postal_code"),
            F.col("o.ship_country").alias("ship_country"),
            F.col("od.product_id").cast(IntegerType()).alias("product_id"),
            F.col("od.unit_price").cast(DoubleType()).alias("unit_price"),
            F.col("od.quantity").cast(IntegerType()).alias("quantity"),
            F.col("od.discount").cast(DoubleType()).alias("discount")
        )
    
    # Calculate line total
    fact = fact.withColumn(
        "line_total",
        (F.col("unit_price") * F.col("quantity") * (1 - F.col("discount")))
    )
    
    # Add time dimensions
    fact = fact.withColumn("year", F.year(F.col("order_date"))) \
               .withColumn("month", F.month(F.col("order_date"))) \
               .withColumn("quarter", F.quarter(F.col("order_date"))) \
               .withColumn("day_of_week", F.dayofweek(F.col("order_date")))
    
    fact.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_orders")
    print(f"✓ Fact Orders: {fact.count()} records")
    return fact


def create_aggregated_sales(spark, gold_path):
    """
    Create aggregated sales metrics
    """
    print("\n--- Creating Aggregated Sales ---")
    fact_orders = spark.read.format("delta").load(f"{gold_path}/fact_orders")
    
    # Sales by product
    sales_by_product = fact_orders.groupBy("product_id") \
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("line_total").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price")
        )
    sales_by_product.write.format("delta").mode("overwrite").save(f"{gold_path}/agg_sales_by_product")
    print(f"✓ Sales by Product: {sales_by_product.count()} records")
    
    # Sales by customer
    sales_by_customer = fact_orders.groupBy("customer_id") \
        .agg(
            F.sum("line_total").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("line_total").alias("avg_order_value")
        )
    sales_by_customer.write.format("delta").mode("overwrite").save(f"{gold_path}/agg_sales_by_customer")
    print(f"✓ Sales by Customer: {sales_by_customer.count()} records")
    
    # Sales by month
    sales_by_month = fact_orders.groupBy("year", "month") \
        .agg(
            F.sum("line_total").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("line_total").alias("avg_order_value")
        ) \
        .orderBy("year", "month")
    sales_by_month.write.format("delta").mode("overwrite").save(f"{gold_path}/agg_sales_by_month")
    print(f"✓ Sales by Month: {sales_by_month.count()} records")


def main():
    """
    Main function để chạy batch ETL từ Silver sang Gold
    """
    print("\n" + "="*80)
    print("BATCH ETL: SILVER -> GOLD")
    print("="*80 + "\n")
    
    # Tạo Spark session
    spark = SparkSession.builder \
        .appName("Batch-Silver-to-Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Cấu hình MinIO
    print("Configuring MinIO access...")
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", "http://minio:9000")
    hconf.set("fs.s3a.access.key", "minioadmin")
    hconf.set("fs.s3a.secret.key", "minioadmin")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("com.amazonaws.sdk.disableEc2Metadata", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Định nghĩa paths
    silver_base = "s3a://silver/topics"
    gold_base = "s3a://gold"
    
    try:
        # Transform dimensions
        transform_customers(spark, silver_base, gold_base)
        transform_products(spark, silver_base, gold_base)
        emp_df = transform_employees(spark, silver_base, gold_base)
        
        # Create fact table - only if employees data exists
        if emp_df is not None:
            create_fact_orders(spark, silver_base, gold_base)
            create_aggregated_sales(spark, gold_base)
        else:
            print("\n⚠ Skipping fact tables due to missing employees data")
        
        print("\n" + "="*80)
        print("BATCH ETL COMPLETED SUCCESSFULLY")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error during ETL process: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
