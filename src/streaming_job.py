from pyspark.sql import SparkSession,functions as F,types as T
schema=T.StructType([T.StructField("event_id",T.StringType()),T.StructField("event_ts",T.TimestampType()),T.StructField("product_id",T.StringType()),T.StructField("quantity",T.IntegerType()),T.StructField("unit_price",T.DoubleType())])
def build(spark:SparkSession,servers:str):
 raw=(spark.readStream.format("kafka").option("kafka.bootstrap.servers",servers).option("subscribe","sales-events").option("startingOffsets","earliest").load())
 events=raw.select(F.from_json(F.col("value").cast("string"),schema).alias("e")).select("e.*")
 return (events.withWatermark("event_ts","10 minutes").dropDuplicates(["event_id"])
  .withColumn("revenue",F.col("quantity")*F.col("unit_price"))
  .groupBy(F.window("event_ts","5 minutes"),"product_id").agg(F.sum("revenue").alias("revenue")))
if __name__=="__main__":
 spark=SparkSession.builder.appName("sales-stream").getOrCreate();build(spark,"localhost:9092").writeStream.format("console").outputMode("update").option("checkpointLocation",".checkpoints/sales").start().awaitTermination()