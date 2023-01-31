import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer
from datetime import datetime
from uuid import uuid4

hosts = ['192.168.56.7:9092', '192.168.56.8:9093']
global camera_id
global segment_id
global start_time
camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"
segment_id = str(uuid4())
start_time = datetime.now()

def gen_segment_id(send_time):
    global start_time
    #if (send_time - start_time_in_excutor).total_seconds()/60 > 10:
    if (send_time - start_time).total_seconds() > 5:
        start_time = datetime.now()
        segment_id = str(uuid4())
    return segment_id

def encode(frame):
    _, buff = cv2.imencode('.jpg', frame)
    b64 = base64.b64encode(buff).decode()
    global camera_id
    send_time = datetime.now()
    data = {
        'video_id': camera_id,
        'segment_id': gen_segment_id(send_time),
        'frame': b64,
        'timestamp': send_time.timestamp()
    }
    return json.dumps(data).encode('utf-8')


def publish_camera(topic, video):
    producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=lambda x: encode(x))
   
    camera = cv2.VideoCapture(0)
    try:
        while True:
            success, frame = camera.read()
            producer.send(topic, frame)
            print("sending...")
            time.sleep(3)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        topic_name = "thu"  # sys.argv[1]
        video_path = "c"  # sys.argv[2]
        publish_camera(topic_name, video_path)
    else:
        print("dont have any topic or video")
