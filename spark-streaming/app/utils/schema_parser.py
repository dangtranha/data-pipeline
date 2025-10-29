from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


def parse_debezium_value(spark, df):
    """Parse Debezium envelope from Kafka 'value' (binary) column.

    Returns DataFrame with columns: key (string), op (string), after (map<string,string>), ts_ms
    """
    # Ensure value is string
    val = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

    # extract common fields from Debezium envelope
    op_col = F.get_json_object(F.col("value"), "$.payload.op").alias("op")
    ts_col = F.get_json_object(F.col("value"), "$.payload.ts_ms").alias("ts_ms")
    after_json = F.get_json_object(F.col("value"), "$.payload.after")

    # parse after into map<string,string> to keep schema-agnostic
    after_map = F.from_json(after_json, MapType(StringType(), StringType())).alias("after")

    parsed = val.select(
        F.col("key"),
        op_col,
        ts_col,
        after_map
    )

    return parsed
