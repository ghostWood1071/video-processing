
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils

# create session and context
session = SparkSession.builder\
          .appName("video-processing.com")\
          .getOrCreate()
context = session.sparkContext
context.setLogLevel("WARN")
# define config info
host = "192.168.248.6:9092, 192.168.248.7:9093"
stream_format = "kafka"
topic = "stream"



# start streaming from kafka source
streaming_df = session.\
               readStream.\
               format(stream_format).\
               option("kafka.bootstrap.servers", host).\
               option("subscribe", topic).\
               load()
# query data
cols = 'id string, frame string'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .select('value.*')
# start streaming
query = data_streaming_df.writeStream.foreachBatch(utils.process_batch).start()
query.awaitTermination()






