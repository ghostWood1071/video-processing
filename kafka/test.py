import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer
from datetime import datetime
from uuid import uuid4

global camera_id
global segment_id
global start_time
camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"
segment_id = str(uuid4())
start_time = datetime.now()

def gen_segment_id(send_time):
    global start_time
    global segment_id
    #if (send_time - start_time_in_excutor).total_seconds()/60 > 10:
    if (send_time - start_time).total_seconds() > 5:
        start_time = datetime.now()
        segment_id = str(uuid4())
    return segment_id

def encode(frame):
    _, buff = cv2.imencode('.jpg', frame)
    print("size of jpg buffer: ", sys.getsizeof(buff))
    b64 = base64.b64encode(buff).decode()
    print("size of bas64: ", sys.getsizeof(b64))
    global camera_id
    # a = datetime.now().timestamp()
    send_time = datetime.now()
    data = {
        'video_id': camera_id,
        'segment_id': gen_segment_id(send_time),
        'frame': b64,
        'send_time': send_time.timestamp()
    }
    json_data = json.dumps(data)
    print(json_data)
    # print(datetime.fromtimestamp(a))
    return json_data.encode('utf-8')

camera = cv2.VideoCapture(0)
try:
    while True:
        success, frame = camera.read()
        print('size of frame: ', sys.getsizeof(frame))
        encoded = encode(frame)
        print('size of send data: ', sys.getsizeof(encoded))
        print("sending...")
        time.sleep(3)
except Exception as e:
    print(e)
    sys.exit(1)