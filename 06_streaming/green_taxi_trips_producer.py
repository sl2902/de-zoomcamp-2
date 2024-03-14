from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkFiles
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from producer import *
from tqdm import tqdm
from time import time
from datetime import datetime

file_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
chunk = 100_000

conf = SparkConf()
conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

schema = StructType(
            [
                StructField("lpep_pickup_datetime", TimestampType()),
                StructField("lpep_dropoff_datetime", TimestampType()),
                StructField("PULocationID", IntegerType()),
                StructField("DOLocationID", IntegerType()),
                StructField("passenger_count", IntegerType()),
                StructField("trip_distance", FloatType()),
                StructField("tip_amount", FloatType()),
            ]
)

def create_spark_session():
    spark = (
        SparkSession.builder
                .config(conf=conf)
                .master("local[*]")
                .appName("Pyspark streaming")
                .getOrCreate()
    )
    return spark



def read_file(spark, file_url):
    spark.sparkContext.addFile(file_url)
    df = (
    spark
        .read
        # .schema(schema)
        .option("header", "true")
        .csv(SparkFiles.get(file_url.split("/")[-1]))
        .select(
            col('lpep_pickup_datetime'), 
            col('lpep_dropoff_datetime'), 
            col('PULocationID'), 
            col('DOLocationID'),
            col('passenger_count'),
            col('trip_distance'),
            col('tip_amount')
        )
)
    df = (
        df
        .withColumn('PULocationID', col('PULocationID').cast(IntegerType()))
        .withColumn('DOLocationID', col('DOLocationID'). cast(IntegerType()))
        .withColumn('passenger_count', col('passenger_count').cast(IntegerType()))
        .withColumn('trip_distance', col('trip_distance').cast(FloatType()))
        .withColumn('tip_amount', col('tip_amount').cast(FloatType()))
    )
    return df

def publish_topic(df, topic_name="green-trips"):
    pdf = df.toPandas()
    # pdf["lpep_pickup_datetime"] = pdf["lpep_pickup_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
    # pdf["lpep_dropoff_datetime"] = pdf["lpep_dropoff_datetime"].dt.strftime('%Y-%m-%d %H:%M:%S')
    batch = 0
    for row in tqdm(pdf.itertuples(index=False), total=pdf.shape[0]):
        row_dict = {col: getattr(row, col) for col in row._fields}
        producer.send(topic_name, value=row_dict)
        batch += 1
        if batch % chunk == 0:
            producer.flush()
            batch = 0
    if batch > 0:
        producer.flush()


def main():
    start = time()
    spark = create_spark_session()
    df = read_file(spark, file_url)
    publish_topic(df)
    end = time()
    print(f'Time to publish data to the topic {round(lit(end-start))} seconds')

if __name__ == "__main__":
    main()