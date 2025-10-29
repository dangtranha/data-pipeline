import json
import time
import threading
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from utils.schema_parser import parse_debezium_value


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
    Read Bronze files from MinIO (s3a:// ...) as a streaming file source,
    parse Debezium envelope JSON and write cleaned records to Silver Delta path.
    """

    # Đường dẫn đến Bronze Layer trong MinIO (nơi Debezium ghi dữ liệu thô)
    bronze_path = silver_conf.get("bronze_path", "s3a://bronze/topics/northwind.public.customers/partition=0")

    # Read files as text stream (one line per record)
    raw = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", 1)
        .load(bronze_path)
    )

    # Tạo các cột 'key' và 'value' cần thiết cho quá trình parse
    df_with_value = raw.select(F.col("value").cast("string").alias("value")) \
                       .withColumn("key", F.lit(None).cast("string")) \
                       .select("key", "value")

    # Phân tích cấu trúc Debezium (trích xuất key, op, ts_ms, after)
    parsed = parse_debezium_value(spark, df_with_value)

    # Loại bỏ các bản ghi không có dữ liệu payload
    cleaned = parsed.filter(F.col("after").isNotNull())

    silver_path = "s3a://silver/topics/northwind.public.customers/"
    checkpoint = "s3a://silver/topics/northwind.public.customers/checkpoints/"

    query = (
        cleaned.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .start(silver_path)
    )

    print(f"[START] Silver stream started.")
    print(f"        Bronze source: {bronze_path}")
    print(f"        Silver target: {silver_path}")
    print(f"        Checkpoint:    {checkpoint}")

    # Tạo thread riêng theo dõi trạng thái query
    monitor_thread = threading.Thread(target=monitor_stream, args=(query,), daemon=True)
    monitor_thread.start()

    return query
