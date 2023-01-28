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
topic = "thu"

model_weights = torch.load('yolov5s.pt', map_location='cpu')
dist_weight = context.broadcast(model_weights)
start_time = context.broadcast(datetime.now())
segment_id = context.broadcast(str(uuid4()))


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

def gen_segment_id(send_time):
  global start_time_in_excutor
  if start_time_in_excutor is None:
    start_time_in_excutor = start_time.value
  elif start_time_in_excutor != start_time.value:
    #if (send_time - start_time_in_excutor).total_seconds()/60 > 10:
    if (send_time - start_time_in_excutor).total_seconds() > 5:
      start_time_in_excutor = datetime.now()
      segment_id.unpersist(True)
      segment_id = context.broadcast(str(uuid4()))
  return segment_id.value

def process_batch_udf(data):
  results = detect.run(dist_weight.value, data, gen_segment_id)
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

# query data
cols = 'video_id string, frame string, timestamp float'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'), col('timestamp'))\
                                .select(from_json(col('value'), cols).name('value'), col('timestamp'))\
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
