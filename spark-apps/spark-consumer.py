from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "movielence"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka:9092'

# TODO: think about this
schema = StructType() \
    .add("movie_id", IntegerType()) \
    .add("title", StringType()) \
    .add("genres", ArrayType(StringType()))

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")
    spark = SparkSession \
             .builder \
             .appName("PySpark Structured Streaming with Kafka Demo") \
            .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(" kafka Started ...")
    # Construct a streaming DataFrame that reads from testtopic
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",                  KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .load()    
    print("Printing Schema of transaction_detail_df: ")
    df.printSchema()
    # Write final result into console for debugging purpose
    # trans_detail_write_stream = transaction_detail_df \
    #    .writeStream \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()
    # trans_detail_write_stream.awaitTermination()

    parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    print(parsed)

    query = parsed.writeStream \
        .outputMode("append") \
        .format("es") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "logs") \
        .option("es.mapping.exclude._type", "true") \
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/movie-stream") \
        .start()

    query.awaitTermination()
    spark.stop()