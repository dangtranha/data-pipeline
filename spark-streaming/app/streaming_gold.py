from pyspark.sql import functions as F
import time
import os
from delta import DeltaTable


def configure_s3a(spark, minio_conf):
    """Cấu hình Hadoop S3A để Spark kết nối MinIO"""
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


def wait_for_silver_ready(spark, silver_path, max_wait=300):
    """Chờ Silver layer có sẵn Delta log trước khi đọc."""
    start = time.time()
    print(f"[INFO] Waiting for Silver table to be ready at {silver_path} ...")
    while True:
        try:
            if DeltaTable.isDeltaTable(spark, silver_path):
                dt = DeltaTable.forPath(spark, silver_path)
                version = dt.history(1).collect()
                print(f"[INFO] Silver Delta table ready (latest version: {version[0]['version'] if version else 0}).")
                return True
        except Exception:
            pass
        if time.time() - start > max_wait:
            print("[WARN] Timeout waiting for Silver table — no data yet.")
            return False
        time.sleep(10)


def run_gold(spark, silver_conf, gold_conf):
    """Đọc dữ liệu Silver, tổng hợp và ghi ra Gold layer."""
    configure_s3a(spark, gold_conf.get("minio") or silver_conf.get("minio"))

    silver_base = silver_conf.get("path_base", "s3a://silver/topics")
    topics = silver_conf.get("topics", {})
    orders_topic = topics.get("orders")
    details_topic = topics.get("order_details")

    silver_orders = f"{silver_base}/{orders_topic}/"
    silver_details = f"{silver_base}/{details_topic}/"

    gold_base = gold_conf.get("path", "s3a://gold/aggregations")
    gold_orders = os.path.join(gold_base, "orders_fact")
    gold_revenue = os.path.join(gold_base, "revenue_by_day")
    checkpoint = gold_conf.get("checkpoint", "/tmp/checkpoints/gold")

    wait_for_silver_ready(spark, silver_orders)
    wait_for_silver_ready(spark, silver_details)

    orders_stream = spark.readStream.format("delta").load(silver_orders)

    def _foreach_batch(micro_batch_df, batch_id):
        try:
            print(f"[INFO] Processing batch {batch_id} ...")
            # đọc snapshot order_details
            try:
                details_df = spark.read.format("delta").load(silver_details)
            except Exception as e:
                print(f"[WARN] Cannot read order_details: {e}")
                details_df = None

            # dữ liệu orders đã chuẩn hóa cột chữ thường ở Silver
            orders_batch = (
                micro_batch_df
                .select("order_id", "customer_id", "order_date", "ts_ms")
                .withColumnRenamed("ts_ms", "ingest_ts")
                .dropDuplicates(["order_id"])
            )

            # chuẩn hóa order_details
            if details_df is not None:
                details_df = (
                    details_df
                    .select(
                        "order_id",
                        "product_id",
                        F.col("unit_price").cast("double"),
                        F.col("quantity").cast("double"),
                        F.col("discount").cast("double")
                    )
                )

            # ghi vào orders_fact (UPSERT)
            try:
                if DeltaTable.isDeltaTable(spark, gold_orders):
                    dt = DeltaTable.forPath(spark, gold_orders)
                    (dt.alias("t")
                     .merge(orders_batch.alias("s"), "t.order_id = s.order_id")
                     .whenMatchedUpdateAll()
                     .whenNotMatchedInsertAll()
                     .execute())
                else:
                    orders_batch.write.format("delta").mode("overwrite").save(gold_orders)
                print(f"[INFO] orders_fact updated for batch {batch_id}")
            except Exception as e:
                print(f"[ERROR] write/merge orders_fact failed: {e}")

            # tính doanh thu theo ngày
            try:
                if details_df is not None and not orders_batch.rdd.isEmpty():
                    joined = details_df.join(orders_batch, "order_id", "inner")
                    rev = joined.withColumn(
                        "revenue",
                        F.col("unit_price") * F.col("quantity") * (1 - F.col("discount"))
                    )
                    rev_by_day = (
                        rev.withColumn("order_date", F.to_date("order_date"))
                        .groupBy("order_date")
                        .agg(F.sum("revenue").alias("total_revenue"))
                    )
                    rev_by_day.write.format("delta").mode("append").save(gold_revenue)
                    print(f"[INFO] revenue_by_day updated for batch {batch_id}")
            except Exception as e:
                print(f"[WARN] revenue aggregation failed for batch {batch_id}: {e}")

        except Exception as e:
            print(f"[ERROR] foreach_batch failed for batch {batch_id}: {e}")

    query = (
        orders_stream.writeStream
        .foreachBatch(_foreach_batch)
        .option("checkpointLocation", os.path.join(checkpoint, "orders"))
        .start()
    )

    print(f"[START] Gold stream started.")
    print(f"        Silver source (orders): {silver_orders}")
    print(f"        Gold outputs: {gold_orders}, {gold_revenue}")

    return query


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import json

    spark = (
        SparkSession.builder
        .appName("streaming-gold")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    with open("/opt/config/silver.conf", "r", encoding="utf-8") as f:
        silver_conf = json.load(f)
    with open("/opt/config/gold.conf", "r", encoding="utf-8") as f:
        gold_conf = json.load(f)

    query = run_gold(spark, silver_conf, gold_conf)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping gold stream...")
        for q in spark.streams.active:
            q.stop()
