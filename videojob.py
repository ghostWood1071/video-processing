from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import *
import json
import time
from datetime import datetime
from uuid import uuid4
import torch
from happybase import *
from typing import *
from detect import detect 



# create session and context
session = SparkSession.builder\
          .appName("video-processing.com")\
          .getOrCreate()

context = session.sparkContext 
context.setLogLevel("WARN")
context.addPyFile("models.zip")
context.addPyFile("utils.zip")
context.addPyFile("detect.zip") 

# define config info
host = "192.168.56.7:9092,192.168.56.8:9093"
stream_format = "kafka"
topic = "777"

model_weights = torch.load('yolov5s.pt', map_location='cpu')
dist_weight = context.broadcast(model_weights)
print(dist_weight.value)
global time 
time = datetime.now()
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


catalog = json.dumps({
  "table":{"namespace":"default","name":"video-processing"},
  "rowkey":"key",
  "columns":{
  "key":{"cf":"rowkey","col":"key","type":"string"},
  "video_id":{"cf":"video","col":"video_id","type":"string"},
  "segment_id":{"cf":"video","col":"segment_id","type":"string"},
  "frame_id":{"cf":"video","col":"frame_id","type":"string"},
  "name":{"cf":"object","col":"name","type":"string"},
  "frame":{"cf":"object","col":"frame","type":"string"},
  }
})

def process_batch_udf(data):
  global time
  global segment_id
  this_time = datetime.now()
  if (this_time - time).total_seconds()/60 > 10:
     time = this_time
     segment_id = uuid4()
  results = detect.run(dist_weight.value, data, segment_id)
  return results


# query data
cols = 'id string, frame string'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .mapInPandas(process_batch_udf, schema)\
                                .select(col('key'), 
                                        col('video_id'), 
                                        col('segment_id'), 
                                        col('frame_id'), 
                                        col('name'), 
                                        col('frame'))

class WriteHbaseRow:
    def __init__(self) -> None:
        self.conn:Connection = Connection(host='10.0.2.195', port=9090, autoconnect=False)
        self.partition_id = None
        self.epoch_id = None

    def open(self, partition_id, epoch_id):
        self.partition_id = partition_id
        self.epoch_id = epoch_id
        self.conn.open()

    def process(self, row):
        table:Table = self.conn.table('video-processing')
        data:Dict[bytes, bytes] = {
            b'video:video_id': row['video_id'],
            b'video:segment_id': row['segment_id'],
            b'video:frame_id': row['frame_id'],
            b'object:name': row['name']
        }
        table.put(f'{self.epoch_id}-{self.partition_id}-{row["key"]}', data)
        self.conn.close()

    def close(self, err):
        self.conn.close()
        print(err)


query = data_streaming_df.writeStream\
.foreach(WriteHbaseRow)\
.start()
# .format("org.apache.hadoop.hbase.spark")\
# .options(catalogs=catalog)\
# .option('hbase.use.hbase.context', False)\
# .start()
query.awaitTermination()
