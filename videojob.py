from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import *
from datetime import datetime
from uuid import uuid4
import torch
from happybase import *
from typing import *
import detect



# create session and context
session = SparkSession.builder\
          .appName("video-processing.com")\
          .getOrCreate()

context = session.sparkContext 
context.setLogLevel("WARN")
context.addPyFile("yolo.zip")

# define config info
host = "192.168.56.7:9092,192.168.56.8:9093"
stream_format = "kafka"
topic = "test1"

model_weights = torch.load('yolov5s.pt', map_location='cpu')
dist_weight = context.broadcast(model_weights)
global start_time
start_time = datetime.now()
global segment_id
segment_id = uuid4()


# start streaming from kafka source
streaming_df = session.\
               readStream.\
               format(stream_format).\
               option("kafka.bootstrap.servers", host).\
               option("subscribe", topic).\
               load()

schema = StructType([
  StructField('key', StringType()),
  StructField('video_id', StringType()),
  StructField('segment_id', StringType()),
  StructField('frame_id', StringType()),
  StructField('name', StringType()),
  StructField('frame', StringType())
])

def process_batch_udf(data):
  global start_time
  global segment_id
  this_time = datetime.now()
  if (this_time - start_time).total_seconds()/60 > 10:
     start_time = this_time
     segment_id = uuid4()
  results = detect.run(dist_weight.value, data, segment_id)
  return results
    

def process(row):
    conn = Connection(host='10.0.2.195', port=9090, autoconnect=False)
    conn.open()
    table = conn.table('video-processing')
    data = {
        'video:video_id': row['video_id'],
        'video:segment_id': row['segment_id'],
        'video:frame_id': row['frame_id'],
        'object:name': row['name'],
        'video:frame': row['frame']
    }
    table.put(f'{row["key"]}', data)
    conn.close()

video_df = context.parallelize([
  {
    'video_id': 'c370a4d1-f4b9-4906-a66d-a7292b86ee3a',
    'num_obj': 0
  },
  {
    'video_id': 'c370a4d1-f4b9-4906-a66d-a7292b86ee3b',
    'num_obj': 0
  }]).toDF("video_id", "num_obj")

# query data
cols = 'video_id string, frame string'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .mapInPandas(process_batch_udf, schema)\
                                .select(col('key'), 
                                        col('video_id'), 
                                        col('segment_id'), 
                                        col('frame_id'), 
                                        col('name'), 
                                        col('frame'))
query = data_streaming_df.writeStream\
.foreach(process)\
.start()

query.awaitTermination()
