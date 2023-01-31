from pyspark.sql import SparkSession, DataFrame
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
          .appName('video-processing.com')\
          .getOrCreate()

context = session.sparkContext 
context.setLogLevel('WARN')
context.addPyFile('yolo.zip')

# define config info
host = '192.168.100.124:9092,192.168.100.125:9093'
stream_format = 'kafka'
topic = 'thu'

model_weights = torch.load('yolov5s.pt', map_location='cpu')
dist_weight = context.broadcast(model_weights)
start_time = context.broadcast(datetime.now())


# start streaming from kafka source
streaming_df = session.\
               readStream.\
               format(stream_format).\
               option('kafka.bootstrap.servers', host).\
               option('subscribe', topic).\
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
  results = detect.run(dist_weight.value, data)
  return results
    

def process(row):
    conn = Connection(host='10.0.2.195', port=9090, autoconnect=False)
    conn.open()
    table = conn.table('video-processing')
    data = {
        'video:video_id': row['video_id'],
        'video:segment_id': row['segment_id'],
        'video:send_time': row['timestamp'],
        'video:frame_id': row['frame_id'],
        'object:name': row['name'],
        'video:frame': row['frame'],
    }
    table.put(f'{row["key"]}', data)
    conn.close()

# query data
cols = 'video_id string, segment_id string, frame string, timestamp float'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .mapInPandas(process_batch_udf, schema)\
                                .select(col('key'), 
                                        col('video_id'), 
                                        col('segment_id'), 
                                        col('frame_id'), 
                                        col('name'), 
                                        col('frame'),
                                        col('timestamp'))
query = data_streaming_df.writeStream\
.foreach(process)\
.start()

query.awaitTermination()


