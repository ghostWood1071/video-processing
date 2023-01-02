from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, ArrayType, Row, StructField
#from pyspark.sql import Row
from pyspark.sql.functions import *
import base64
import json
import time
from datetime import datetime
from uuid import uuid4
import cv2
import numpy as np
import torch
#from yolov5.detect import run
import pandas as pd
import io


# create session and context
session = SparkSession.builder\
          .appName("video-processing.com")\
          .getOrCreate()

context = session.sparkContext
context.setLogLevel("WARN")

context.addPyFile("models.zip")
context.addPyFile("utils.zip")
context.addFile("yolov5s.pt")
# define config info
host = "192.168.56.7:9092,192.168.56.8:9093"
stream_format = "kafka"
topic = "video10"
segment_id = uuid4()

model_weights = torch.load('yolov5s.pt', map_location='cpu')
dist_weight = context.broadcast(model_weights)
time = context.broadcast(datetime.now())

from detect import run

# start streaming from kafka source
streaming_df = session.\
               readStream.\
               format(stream_format).\
               option("kafka.bootstrap.servers", host).\
               option("subscribe", topic).\
               load()

return_type = ArrayType(StructType([
  StructField('video_id', StringType()),
  StructField('segment_id', StringType()),
  StructField('frame_id', StringType()),
  StructField('name', StringType())
]), containsNull=True)

@pandas_udf(returnType=StringType())
def process_batch_udf(data):
  print('asddddddddddddddddddddddddddddddddddddd: ', type(data))
  # main_time = time.value
  # this_time = datetime.now()
  # if (this_time - main_time).total_seconds()/60 > 10:
  #   time = this_time
  segment_id = uuid4()
  results = run(dist_weight.value, data.values.tolist(), segment_id)
  # return pd.DataFrame(results)

  return pd.Series(results)


# query data
cols = 'id string, frame string'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .select(process_batch_udf(col('value')))


query = data_streaming_df.writeStream.foreach(lambda row: print(row)).start()
#query = data_streaming_df.writeStream.start()
query.awaitTermination()