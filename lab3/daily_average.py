import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, split, desc, rank, avg, round
from pyspark.sql.window import Window

scala_version = '2.12'
spark_version = '3.4.0'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

findspark.add_packages(','.join(packages))
findspark.init()

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SparkVjezba")
    .getOrCreate()
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "BTC")
    .option("startingOffsets", "earliest")
    .load()
)

kafka_df = kafka_df.withColumn("value", kafka_df["value"].cast("string"))

split_columns = split(kafka_df['value'], ',')
kafka_df = kafka_df.withColumn('date', to_date(split_columns.getItem(0)))
kafka_df = kafka_df.withColumn('timestamp', split_columns.getItem(1).cast('integer'))
kafka_df = kafka_df.withColumn('price', split_columns.getItem(2).cast('decimal(30, 15)'))
kafka_df = kafka_df.select(['date', 'price'])

kafka_df = (
    kafka_df.groupBy('date')
    .agg(round(avg('price'), 1).alias('avg_price'))
    .orderBy('avg_price', ascending=False)
)

# kafka_df = kafka_df.withColumn("rank", rank().over(
#     Window.partitionBy('timestamp')
#     .orderBy(desc('avg_price'))
# ))Y

kafka_query = (
    kafka_df.writeStream
    .format("console")
    # .option("path", "output/data")
    # .option("checkpointLocation", "output/checkpoint")
    .outputMode("complete")
    .start()
)

kafka_query.processAllAvailable()

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "BTC")
    .option("startingOffsets", "earliest")
    .load()
)

kafka_df = kafka_df.withColumn("value", kafka_df["value"].cast("string"))

split_columns = split(kafka_df['value'], ',')
kafka_df = kafka_df.withColumn('date', to_date(split_columns.getItem(0)))
kafka_df = kafka_df.withColumn('timestamp', split_columns.getItem(1).cast('integer'))
kafka_df = kafka_df.withColumn('price', split_columns.getItem(2).cast('decimal(30, 15)'))
kafka_df = kafka_df.select(['date', 'price'])

kafka_query = (
    kafka_df.writeStream
    .format("csv")
    .option("path", "output/data")
    .option("checkpointLocation", "output/checkpoint")
    .outputMode("append")
    .start()
)

kafka_query.processAllAvailable()
