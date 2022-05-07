
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
from utils import decode_frame, encode_frame, DatabaseBusiness

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

object_detect_model = ObjectDetectModel()
body_portion_model = BodyPortionDetector()
db_buss = DatabaseBusiness()

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
    frame = decode_frame(string)
    result = object_detect_model.detect(frame)
    result_encode = encode_frame(result['frame'])

    # update frame if exists extra object
    db_buss.update_frame_seqs(result['num_obj'], cam_id, result['labels'])

    # create frame according frame_sequences
    frames = db_buss.create_frame(db_buss.segments[cam_id]['frame_seqs'], result_encode, result['frame'])
    things = db_buss.create_things(cam_id, frames)
    people = db_buss.create_people(cam_id, frames, body_portion_model)


def process_batch(df, epoch_id):
    db_buss.reset_frame_segments()
    rows = df.collect()
    for row in rows:
        process_row(row)


# start streaming
query = data_streaming_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()






