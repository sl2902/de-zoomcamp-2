import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

schema = StructType(
            [
                StructField("lpep_pickup_datetime", TimestampType()),
                StructField("lpep_dropoff_datetime", TimestampType()),
                StructField("PULocationID", IntegerType()),
                StructField("DOLocationID", IntegerType()),
                StructField("passenger_count", FloatType()),
                StructField("trip_distance", FloatType()),
                StructField("tip_amount", FloatType()),
            ]
)

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

# query = green_stream.writeStream.foreachBatch(peek).start()
green_stream = (
    green_stream
    .select(from_json(col("value").cast('string'), schema=schema).alias('data'))
    .select('data.*')
)

agg_df = (
    green_stream
        .withColumn("timestamp", current_timestamp())
        .groupBy(window(col("timestamp"), "5 minutes"), col("DOLocationID"))
        .agg(count("*").alias("count"))
        .orderBy(desc(col("count")))
)
query = agg_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
query.awaitTermination(60)
query.stop()