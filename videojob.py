from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
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
topic = "videos"
segment_id = uuid4()

with open('yolov5s.pt', mode='rb') as f:
    buff = io.BytesIO(f.read())
    #weights =   #torch.load('yolov5s.pt', map_location='cpu' )
dist_weight = context.broadcast(buff)
time = datetime.now()

from detect import run

# start streaming from kafka source
streaming_df = session.\
               readStream.\
               format(stream_format).\
               option("kafka.bootstrap.servers", host).\
               option("subscribe", topic).\
               load()


@pandas_udf(returnType=StructType())
def process_batch_udf(data):
  bff = dist_weight.value
  weights = torch.load(bff)
  this_time = datetime.now()
  if (this_time - time).total_seconds()/60 > 10:
    time = this_time
    segment_id = uuid4()
  results = run(weights, data.values.tolist(), segment_id)
  return pd.DataFrame(results)


# query data
cols = 'id string, frame string'
data_streaming_df = streaming_df.select(col('value').cast('string').name('value'))\
                                .select(from_json(col('value'), cols).name('value'))\
                                .select(process_batch_udf(col('value')))


query = data_streaming_df.writeStream.foreach(lambda row: print(row)).start()
#query = data_streaming_df.writeStream.start()
query.awaitTermination()