# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from faker import Faker
from uuid import uuid4

# COMMAND ----------

#Number of unique pages and rows we want to generate.
#Increasing the number of unique pages greatly increases the time to initiate the generator
rows_second = "50000"
unique_pages = 200000

# COMMAND ----------

pages = []
for i in range(unique_pages):
  pages.append(str(uuid4()).split('-')[-1])
pages

# COMMAND ----------

uuidUdf = udf(lambda: str(uuid4()), StringType())

# COMMAND ----------

#Change with your config
kafka_bootstrap_servers_tls = dbutils.secrets.get("kafka-cluster", "kafka-bootstrap-servers-tls"      )
topic = 'page_click'
checkpoint_location = '/FileStore/vh_tmp/'+topic+'/checkpoints/'

# COMMAND ----------

#Resetting the testing environment by removing the checkpoint
dbutils.fs.rm(checkpoint_location, True)

# COMMAND ----------

import numpy as np

# Generating a random sample from a normal distribution truncated between 0 and 1 to simulate a real world scenario where some pages get more clicks than others
sample_size = unique_pages
mean = 0.5
std_dev = 0.2
normal_distribution_sample = np.clip(np.random.normal(mean, std_dev, sample_size), 0, 1)


# COMMAND ----------

import matplotlib.pyplot as plt
dist = (normal_distribution_sample*unique_pages).astype(int).tolist()
plt.hist(dist, bins = 'auto')
plt.xlabel('Weighted Pages')
plt.ylabel('Frequency')
plt.title('Weighted Pages Histogram')
plt.show()

# COMMAND ----------


dummy_stream = (
  spark
 .readStream
 .format('rate')
 .option("rowsPerSecond", rows_second)
 .load()
 .withColumn('pageId', F.lit(pages)[ F.lit(dist)[(F.rand()*unique_pages).cast('int')]])
 .withColumn('eventTime', (F.current_timestamp()))
 .withColumn('key', uuidUdf())
 .drop('value')
 .withColumn('value', F.to_json(F.struct(F.col('pageId'), F.col('eventTime'))))
 .select('key', 'value')
 )


# COMMAND ----------

(
  dummy_stream
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
  .option("kafka.security.protocol", "SSL")
  .option("checkpointLocation", checkpoint_location )
  .option("topic", topic)
  .start()
)
