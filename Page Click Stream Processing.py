# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as sTy
from uuid import uuid4

# COMMAND ----------

kafka_bootstrap_servers_tls = dbutils.secrets.get("kafka-cluster", "kafka-bootstrap-servers-tls"      )
read_topic = 'page_click'
write_topic = 'page_click_aggregate'
checkpoint_location = '/FileStore/vh_tmp/'+write_topic+'/checkpoints/'

# COMMAND ----------

#Resetting the testing environment by removing the checkpoint
dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------


#More efficient state provider for larger state storage
spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)

spark.conf.set(
  "spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled",
  "false"
)

#New Settings
#Improves reliability and durability of stream
spark.conf.set(
  "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
  "true"
)


#Allows rebalancing in the state partitioning
spark.conf.set(
  "spark.sql.streaming.statefulOperator.stateRebalancing.enabled",
  "true",
)

#Number of partitions should be multiple of number of cores
spark.conf.set("spark.sql.shuffle.partitions", "32")

# COMMAND ----------

uuidUdf = udf(lambda: str(uuid4()), sTy.StringType())

# COMMAND ----------

raw_page_clicks = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL")
  .option("subscribe", read_topic)
  .option("startingOffsets", "latest")
  .option("minPartitions", "64")
  .load()
)

# COMMAND ----------

page_clicks = raw_page_clicks.select(
    F.from_json(
        F.col("value").cast("string"),
        sTy.StructType(
            [
                sTy.StructField("pageId", sTy.StringType()),
                sTy.StructField("eventTime", sTy.TimestampType()),
            ]
        ),
    ).alias("payload")
).select("payload.pageId", "payload.eventTime")

# COMMAND ----------

page_clicks = (
    page_clicks
    # Time watermark active for each event
    .withWatermark("eventTime", "5 seconds")
    # We want to send the updated results
    .groupBy("pageId", F.window("eventTime", "5 seconds"))
    .agg(
        F.count("*").alias("click_count"), 
        F.max(F.col("eventTime")).alias("eventTime")
    )
    .filter(F.col("click_count") > 5000)
)

# COMMAND ----------

output = (
    page_clicks.withColumn("key", uuidUdf())
    .withColumn(
        "value",
        F.to_json(
          F.struct(
            F.col("pageId"), 
            F.col("click_count"), 
            F.col("eventTime")
            )
          ),
    )
    .select("key", "value")
)

# COMMAND ----------

result = (
    output.writeStream.outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
    .option("kafka.security.protocol", "SSL")
    .option("checkpointLocation", checkpoint_location)
    .option("topic", write_topic)
    .trigger(processingTime="5 seconds")
    .start()
)

# COMMAND ----------

#Sink output sample
result = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL")
  .option("subscribe", write_topic)
  .option("startingOffsets", "latest")
  .load()
  
  .select(F.from_json(
    F.col("value").cast("string"),
    sTy.StructType([
      sTy.StructField('pageId', sTy.StringType()),
      sTy.StructField('click_count', sTy.LongType()),
      sTy.StructField('eventTime', sTy.TimestampType())       
    ])
  ).alias('payload')
  )
  .select('payload.pageId','payload.click_count', 'payload.eventTime')
)
