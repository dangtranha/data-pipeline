import json
import time
import threading
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from utils.schema_parser import parse_debezium_value


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
            print(f"[INFO] New micro-batch processed, rows={num_input}")
            last_progress = progress
        time.sleep(interval)


def run_silver(spark, kafka_conf, silver_conf):
    """
    Đọc dữ liệu thô từ Bronze layer (được lưu trong MinIO ở định dạng JSON),
    phân tích (parse) cấu trúc Debezium envelope và ghi dữ liệu đã làm sạch ra Silver layer (định dạng Delta).
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
        # Mẫu đường dẫn để đọc dữ liệu JSON theo partition (do Debezium ghi)
        bronze_path = f"{bronze_base}/{topic}/partition=*/**"

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

        # Ép kiểu và tạo cột key/value giống Kafka record
        parsed = parse_debezium_value(spark, df_with_value)
        cleaned = parsed.filter(F.col("after").isNotNull())

        # Làm phẳng dữ liệu “after” thành các cột cụ thể tuỳ theo từng bảng
        if logical_name == "orders":
            # cố gắng lấy các field phổ biến của orders
            flattened = cleaned.select(
                coalesce_after(cleaned, ["OrderID", "order_id", "orderid"], "order_id"),
                coalesce_after(cleaned, ["CustomerID", "customer_id"], "customer_id"),
                coalesce_after(cleaned, ["EmployeeID", "employee_id"], "employee_id"),
                coalesce_after(cleaned, ["OrderDate", "order_date"], "order_date"),
                coalesce_after(cleaned, ["RequiredDate", "required_date"], "required_date"),
                coalesce_after(cleaned, ["ShippedDate", "shipped_date"], "shipped_date"),
                coalesce_after(cleaned, ["ShipVia", "ship_via"], "ship_via"),
                coalesce_after(cleaned, ["Freight", "freight"], "freight"),
                F.col("op"),
                F.col("ts_ms")
            )

            # chuyển kiểu dữ liệu nếu cần
            orders_target = f"{path_base}/{topic}/"
            orders_ckpt = f"{checkpoint_base}/{topic}"
            q = (flattened.writeStream.format("delta").outputMode("append").option("checkpointLocation", orders_ckpt).start(orders_target))
            queries[logical_name] = q

        elif logical_name == "order_details":
            flattened = cleaned.select(
                coalesce_after(cleaned, ["OrderID", "order_id", "orderid"], "order_id"),
                coalesce_after(cleaned, ["ProductID", "product_id", "productid"], "product_id"),
                coalesce_after(cleaned, ["UnitPrice", "unit_price"], "unit_price"),
                coalesce_after(cleaned, ["Quantity", "quantity"], "quantity"),
                coalesce_after(cleaned, ["Discount", "discount"], "discount"),
                F.col("op"),
                F.col("ts_ms")
            )
            details_target = f"{path_base}/{topic}/"
            details_ckpt = f"{checkpoint_base}/{topic}"
            q = (flattened.writeStream.format("delta").outputMode("append").option("checkpointLocation", details_ckpt).start(details_target))
            queries[logical_name] = q

        # NOTE: `customers` topic is intentionally not handled here — it will be processed
        # by a separate batch ETL job. If an unexpected topic appears, it will fall through
        # to the generic writer below.

        else:
            # generic: write parsed after map as-is to a delta path
            generic_target = f"{path_base}/{topic}/"
            generic_ckpt = f"{checkpoint_base}/{topic}"
            q = (
                cleaned.writeStream.format("delta").outputMode("append").option("checkpointLocation", generic_ckpt).start(generic_target)
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

    # Tạo SparkSession
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

    # Đọc config từ file (như main.py trước đây)
    import json
    with open("/opt/config/kafka.conf", "r", encoding="utf-8") as f:
        kafka_conf = json.load(f)
    with open("/opt/config/silver.conf", "r", encoding="utf-8") as f:
        silver_conf = json.load(f)

    # Gọi hàm chính
    queries = run_silver(spark, kafka_conf, silver_conf)

    # Chờ stream kết thúc
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping silver streams...")
        for q in spark.streams.active:
            q.stop()

