
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import base64
import json
import time
from datetime import datetime
import datetime as dt
from uuid import uuid4
import cv2
import numpy as np
import requests
from ObjectDetection.Detection import ObjectDetectModel
from BodyPortionDetect.BodyDetector import BodyPortionDetector

# create session and context
session = SparkSession.builder\
          .appName("video-processing.com")\
          .getOrCreate()
context = session.sparkContext
# context.setLogLevel("WARN")
# define config info
host = "192.168.248.6:9092, 192.168.248.7:9093"
stream_format = "kafka"
topic = "stream"

object_detect_model = ObjectDetectModel()
body_portion_model = BodyPortionDetector()

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


def process_row(row):
    cam_id = row[0]
    string = row[1]
    jpg_origin = base64.b64decode(string)
    buff = np.frombuffer(jpg_origin, dtype=np.uint8)
    frame = cv2.imdecode(buff, flags=1)
    result = object_detect_model.detect(frame)
    print(result['labels'])


def process_batch(df, epoch_id):
    # segments = reset_frame_segments(start_time)
    # seg_json = json.dumps(segments).encode('utf-8')
    # print(seg_json)
    # requests.post(url=url+'/add-segments', data=seg_json, headers= headers)
    rows = df.collect()
    for row in rows:
        process_row(row)


# start streaming
query = data_streaming_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()






