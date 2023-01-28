import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer
from datetime import datetime

global camera_id
camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"

def encode(frame):
    _, buff = cv2.imencode('.jpg', frame)
    print("size of jpg buffer: ", sys.getsizeof(buff))
    b64 = base64.b64encode(buff).decode()
    print("size of bas64: ", sys.getsizeof(b64))
    global camera_id
    a = datetime.now().timestamp()
    data = {
        'video_id': camera_id,
        'frame': b64,
        'time': a
    }
    print(datetime.fromtimestamp(a))
    return json.dumps(data).encode('utf-8')

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