import json
import time
import threading
from pyspark.sql import functions as F
from pyspark.sql.types import (
    MapType, StringType, StructType, StructField, LongType
)

# Note: chúng ta không dùng parse_debezium_value từ utils ở đây nữa,
# vì ta tự implement parser tổng quát ngay trong file này để dễ điều chỉnh
# và đảm bảo tương thích với dữ liệu Debezium "after" trực tiếp.

def configure_s3a(spark, minio_conf):
    """Cấu hình Hadoop S3A để kết nối tới MinIO bằng thông tin trong silver_conf"""
    if not minio_conf:
        return
    endpoint = minio_conf.get("endpoint")
    access = minio_conf.get("access_key")
    secret = minio_conf.get("secret_key")
    hconf = spark._jsc.hadoopConfiguration()
    if endpoint:
        ep = endpoint.replace("http://", "").replace("https://", "")
        hconf.set("fs.s3a.endpoint", ep)
    if access:
        hconf.set("fs.s3a.access.key", access)
    if secret:
        hconf.set("fs.s3a.secret.key", secret)
    hconf.set("fs.s3a.path.style.access", "true")
    if endpoint and endpoint.startswith("http://"):
        hconf.set("fs.s3a.connection.ssl.enabled", "false")


def monitor_stream(query, interval=10):
    """
    Luồng phụ để in log trạng thái của stream định kỳ.
    Nếu query.active = True nhưng không có batch mới => coi như đang chờ dữ liệu.
    """
    last_progress = None
    while query.isActive:
        progress = query.lastProgress
        if progress is None or progress == last_progress:
            print("[INFO] Silver stream is waiting for new files in Bronze layer (no new data yet).")
        else:
            num_input = progress.get("numInputRows", 0)
            num_output = progress.get("numOutputRows", 0)
            print(f"[INFO] New micro-batch processed, rows_in={num_input}, rows_out={num_output}")
            last_progress = progress
        time.sleep(interval)


def get_generic_debezium_schema():
    """
    Schema tổng quát cho Debezium JSON: before/after là MapType để chấp nhận bất kỳ cặp key-value nào.
    """
    return StructType([
        StructField("before", MapType(StringType(), StringType()), True),
        StructField("after", MapType(StringType(), StringType()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ])


def parse_debezium_df(df_with_value):
    """
    Parse chung Debezium envelope từ cột 'value' (string) thành các cột:
      - before (map)
      - after (map)
      - op
      - ts_ms
    Trả về DataFrame đã tách các cột trên (không flatten `after`).
    """
    schema = get_generic_debezium_schema()
    parsed = df_with_value.withColumn("json_data", F.from_json(F.col("value"), schema))
    # select expand at top level so columns are 'before','after','op','ts_ms'
    parsed = parsed.select("json_data.before", "json_data.after", "json_data.op", "json_data.ts_ms")
    return parsed


def coalesce_after_map(col, candidates, alias):
    """
    Trả về expression coalesce cho các key có thể xuất hiện trong map 'after'.
    col: tên cột map (chúng ta dùng 'after').
    candidates: danh sách tên khóa có thể.
    alias: tên alias cho cột kết quả.
    """
    # F.coalesce expects column expressions, so create them from map access
    exprs = [F.col(f"after['{k}']") for k in candidates]
    return F.coalesce(*exprs).alias(alias)


def flatten_after_map(df, prefix=None):
    """
    Tự động tạo các cột từ map 'after'. Trả về df với các cột mới.
    Lưu ý: collect keys từng batch (đắt nếu dữ liệu lớn). Sử dụng khi dataset nhỏ hoặc test.
    """
    # Lấy tất cả key hiện có (cẩn trọng, bằng cách này ta thu về driver)
    keys = df.select(F.explode(F.map_keys("after")).alias("k")).distinct().rdd.map(lambda r: r.k).collect()
    for k in keys:
        col_name = k if not prefix else f"{prefix}_{k}"
        df = df.withColumn(col_name, F.col(f"after['{k}']"))
    return df


def run_silver(spark, kafka_conf, silver_conf):
    """
    Đọc dữ liệu thô từ Bronze layer (MinIO JSON),
    phân tích (parse) cấu trúc Debezium envelope và ghi dữ liệu đã làm sạch ra Silver layer (Delta).
    """

    # cấu hình kết nối s3a -> MinIO
    configure_s3a(spark, silver_conf.get("minio", {}))

    # lấy cấu hình path từ silver_conf
    bronze_base = silver_conf.get("bronze_base", "s3a://bronze/topics")
    topics = silver_conf.get("topics", {})
    bronze_delta_base = silver_conf.get("bronze_delta_base", "s3a://data/bronze_delta")
    path_base = silver_conf.get("path_base", "s3a://silver/topics")
    checkpoint_base = silver_conf.get("checkpoint_base", "s3a://silver/topics/checkpoints")

    queries = {}

    # Hàm phụ: lấy giá trị của các cột có thể khác nhau tên (do schema thay đổi) trong map “after”
    def coalesce_after(col_map, candidates, alias):
        exprs = [F.col("after").getItem(k) for k in candidates]
        return F.coalesce(*exprs).alias(alias)

    # Duyệt qua từng topic tạo một stream riêng
    for logical_name, topic in topics.items():
        bronze_path = f"{bronze_base}/{topic}/partition=*/**"
        print(f"[INFO] Will read bronze path: {bronze_path}")

        # đọc raw file line-by-line (connector writes JSON per record)
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 2)
            .load(bronze_path)
        )

        # Ép kiểu và tạo cột key/value giống Kafka record
        df_with_value = raw.select(F.col("value").cast("string").alias("value")) \
                           .withColumn("key", F.lit(None).cast("string")) \
                           .select("key", "value")

        # Parse Debezium generic (before/after as MapType)
        parsed = parse_debezium_df(df_with_value)

        # Filter bỏ event không có after (ví dụ delete-only hoặc tombstone)
        parsed_nonnull = parsed.filter(F.col("after").isNotNull())

        # Tại thời điểm này, 'parsed_nonnull' có cột:
        #   after (map<string,string>), op (string), ts_ms (long)
        # Bây giờ làm phẳng theo từng topic

        if logical_name == "orders":
            # Lấy trực tiếp các trường phổ biến từ map 'after' (dùng cast nếu cần)
            flattened = parsed_nonnull.select(
                F.col("after").getItem("order_id").alias("order_id").cast("int"),
                F.col("after").getItem("customer_id").alias("customer_id"),
                F.col("after").getItem("employee_id").alias("employee_id").cast("int"),
                F.date_add(F.lit("1970-01-01"), F.col("after").getItem("order_date").cast("int")).alias("order_date"),
                F.date_add(F.lit("1970-01-01"), F.col("after").getItem("required_date").cast("int")).alias("required_date"),
                F.date_add(F.lit("1970-01-01"), F.col("after").getItem("shipped_date").cast("int")).alias("shipped_date"),
                F.col("after").getItem("ship_via").alias("ship_via").cast("int"),
                F.col("after").getItem("freight").alias("freight").cast("double"),
                F.col("after").getItem("ship_name").alias("ship_name"),
                F.col("after").getItem("ship_address").alias("ship_address"),
                F.col("after").getItem("ship_city").alias("ship_city"),
                F.col("after").getItem("ship_region").alias("ship_region"),
                F.col("after").getItem("ship_postal_code").alias("ship_postal_code"),
                F.col("after").getItem("ship_country").alias("ship_country"),
                F.col("op"),
                F.col("ts_ms").alias("ingest_ts").cast("bigint")
            ).withColumn("processed_at", F.current_timestamp())

            orders_target = f"{path_base}/{topic}/"
            orders_ckpt = f"{checkpoint_base}/{topic}"
            q = (
                flattened.writeStream.format("delta")
                .outputMode("append")
                .option("checkpointLocation", orders_ckpt)
                .option("mergeSchema", "true")
                .start(orders_target)
            )
            queries[logical_name] = q

        elif logical_name == "order_details":
            # order_details có các trường: order_id, product_id, unit_price, quantity, discount
            flattened = parsed_nonnull.select(
                F.coalesce(
                    F.col("after").getItem("OrderID"),
                    F.col("after").getItem("order_id"),
                    F.col("after").getItem("orderid")
                ).alias("order_id").cast("int"),
                F.coalesce(
                    F.col("after").getItem("ProductID"),
                    F.col("after").getItem("product_id"),
                    F.col("after").getItem("productid")
                ).alias("product_id").cast("int"),
                F.coalesce(
                    F.col("after").getItem("UnitPrice"),
                    F.col("after").getItem("unit_price"),
                    F.col("after").getItem("unitprice")
                ).alias("unit_price").cast("double"),
                F.coalesce(
                    F.col("after").getItem("Quantity"),
                    F.col("after").getItem("quantity")
                ).alias("quantity").cast("double"),
                F.coalesce(
                    F.col("after").getItem("Discount"),
                    F.col("after").getItem("discount")
                ).alias("discount").cast("double"),
                F.col("op"),
                F.col("ts_ms").alias("ingest_ts").cast("bigint")
            )

            details_target = f"{path_base}/{topic}/"
            details_ckpt = f"{checkpoint_base}/{topic}"
            q = (
                flattened.writeStream.format("delta")
                .outputMode("append")
                .option("checkpointLocation", details_ckpt)
                .option("mergeSchema", "true")
                .start(details_target)
            )
            queries[logical_name] = q

        else:
            # generic: ghi toàn bộ map 'after' (dùng để debug hoặc xử lý bảng khác)
            generic = parsed_nonnull.select(
                F.col("after"),
                F.col("op"),
                F.col("ts_ms")
            )
            generic_target = f"{path_base}/{topic}/"
            generic_ckpt = f"{checkpoint_base}/{topic}"
            q = (
                generic.writeStream.format("delta")
                .outputMode("append")
                .option("checkpointLocation", generic_ckpt)
                .option("mergeSchema", "true")
                .start(generic_target)
            )
            queries[logical_name] = q

        print(f"[START] Silver stream for topic {topic} -> {path_base}/{topic}/")

    # start monitor thread for each query (optional)
    for name, q in queries.items():
        t = threading.Thread(target=monitor_stream, args=(q,), daemon=True)
        t.start()

    return queries


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import json

    spark = (
        SparkSession.builder
        .appName("streaming-silver")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # Đọc config
    with open("/opt/config/kafka.conf", "r", encoding="utf-8") as f:
        kafka_conf = json.load(f)
    with open("/opt/config/silver.conf", "r", encoding="utf-8") as f:
        silver_conf = json.load(f)

    queries = run_silver(spark, kafka_conf, silver_conf)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping silver streams...")
        for q in spark.streams.active:
            q.stop()
