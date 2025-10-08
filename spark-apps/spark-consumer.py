from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f

KAFKA_TOPIC_NAME_CONS = "rating"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka:9092'

input_schema = StructType() \
    .add("userId", StringType()) \
    .add("movieId", StringType()) \
    .add("rating", StringType()) \
    .add("timestamp", TimestampType())

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Rating Processing") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
        .option("subscribe", KAFKA_TOPIC_NAME_CONS)
        .load())

    parsed = (df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), input_schema).alias("data"))
        .select("data.*"))

    output_df = (parsed
        .select(
            col("userId").alias("user"),
            col("movieId").alias("movie"),
            col("rating"),
            col("timestamp")
        )
    )

    query = (output_df.writeStream
        .outputMode("append")
        .format("es")
        .option("es.nodes", "elasticsearch")
        .option("es.port", "9200")
        .option("es.resource", "logs")
        .option("es.mapping.exclude._type", "true")
        .option("es.nodes.wan.only", "true")
        .option("checkpointLocation", "/tmp/spark-checkpoints/movies-to-es")
        .start())

    query.awaitTermination()
    spark.stop()