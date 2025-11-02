"""
Batch ETL: Bronze -> Silver
Đọc dữ liệu từ Bronze layer (MinIO), parse Debezium format và ghi vào Silver layer (Delta format)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
import json


def parse_debezium_json(df):
    """
    Parse Debezium JSON format từ Bronze layer
    Trích xuất: op (operation), ts_ms (timestamp), after (data)
    """
    # Schema cho Debezium message (không có wrapper payload)
    debezium_schema = StructType([
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("before", MapType(StringType(), StringType()), True),
        StructField("after", MapType(StringType(), StringType()), True),
        StructField("source", MapType(StringType(), StringType()), True)
    ])
    
    # Parse JSON value
    parsed_df = df.select(
        F.from_json(F.col("value"), debezium_schema).alias("data")
    )
    
    # Extract fields
    result_df = parsed_df.select(
        F.col("data.op").alias("operation"),
        F.col("data.ts_ms").alias("timestamp_ms"),
        F.col("data.after").alias("after"),
        F.col("data.before").alias("before"),
        F.col("data.source").alias("source")
    )
    
    return result_df


def expand_map_to_columns(df, map_col="after"):
    """
    Chuyển Map column thành các cột riêng lẻ
    Sử dụng schema inference từ first non-null row
    """
    # Lấy sample row có after không null
    sample = df.filter(F.col(map_col).isNotNull()).limit(1).collect()
    
    if not sample or sample[0][map_col] is None:
        # Nếu không có data, return df as-is
        print(f"Warning: No data found in '{map_col}' column")
        return df
    
    # Lấy keys từ sample row
    key_list = list(sample[0][map_col].keys())
    
    # Tạo column cho mỗi key bằng cách sử dụng getItem()
    for key in key_list:
        df = df.withColumn(key, F.col(map_col).getItem(key))
    
    return df


def batch_bronze_to_silver(spark, topic_name, bronze_base_path, silver_base_path):
    """
    Batch processing từ Bronze sang Silver cho một topic
    
    Args:
        spark: SparkSession
        topic_name: Tên topic (vd: "northwind.public.customers")
        bronze_base_path: Đường dẫn base của Bronze layer
        silver_base_path: Đường dẫn base của Silver layer
    """
    print(f"\n{'='*80}")
    print(f"Processing topic: {topic_name}")
    print(f"{'='*80}\n")
    
    # Đường dẫn Bronze cho topic này (sử dụng S3A protocol, không có partition=0)
    bronze_path = f"{bronze_base_path}/{topic_name}/partition=0/*.json"
    silver_path = f"{silver_base_path}/{topic_name}"
    
    print(f"Reading from Bronze: {bronze_path}")
    
    # Đọc dữ liệu từ Bronze (text format)
    try:
        raw_df = spark.read.text(bronze_path)
        
        # Đếm số records
        total_records = raw_df.count()
        print(f"Total records in Bronze: {total_records}")
        
        if total_records == 0:
            print(f"No data found in {bronze_path}")
            return
        
        # Rename column to 'value' for consistency
        raw_df = raw_df.withColumnRenamed("value", "value")
        
        # Parse Debezium format
        print("Parsing Debezium format...")
        parsed_df = parse_debezium_json(raw_df)
        
        # Filter out null 'after' (delete operations)
        cleaned_df = parsed_df.filter(F.col("after").isNotNull())
        
        valid_records = cleaned_df.count()
        print(f"Valid records (after filtering deletes): {valid_records}")
        
        # Expand map to columns
        print("Expanding map columns...")
        expanded_df = expand_map_to_columns(cleaned_df, "after")
        
        # Drop the map columns and keep only expanded columns
        expanded_df = expanded_df.drop("after", "before")
        
        # Add processing metadata
        final_df = expanded_df.withColumn("processed_at", F.current_timestamp())
        
        # Show sample
        print("\nSample data:")
        final_df.show(5, truncate=False)
        
        # Write to Silver layer as Delta format
        print(f"\nWriting to Silver: {silver_path}")
        final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)
        
        print(f"✓ Successfully processed {valid_records} records to Silver layer")
        
    except Exception as e:
        print(f"✗ Error processing topic {topic_name}: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """
    Main function để chạy batch ETL
    """
    print("\n" + "="*80)
    print("BATCH ETL: BRONZE -> SILVER")
    print("="*80 + "\n")
    
    # Tạo Spark session
    spark = SparkSession.builder \
        .appName("Batch-Bronze-to-Silver") \
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
    bronze_base = "s3a://bronze/topics"
    silver_base = "s3a://silver/topics"
    
    # Danh sách các topics cần xử lý
    topics = [
        "northwind.public.customers",
        "northwind.public.orders",
        "northwind.public.order_details",
        "northwind.public.products",
        "northwind.public.categories",
        "northwind.public.employees",
        "northwind.public.shippers",
        "northwind.public.suppliers"
    ]
    
    # Xử lý từng topic
    for topic in topics:
        batch_bronze_to_silver(spark, topic, bronze_base, silver_base)
    
    print("\n" + "="*80)
    print("BATCH ETL COMPLETED")
    print("="*80 + "\n")
    
    spark.stop()


if __name__ == "__main__":
    main()
