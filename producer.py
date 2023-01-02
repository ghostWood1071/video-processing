import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer

hosts = ['192.168.56.7:9092', '192.168.56.8:9093']
camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"


def encode(frame):
    _, buff = cv2.imencode('.jpg', frame)
    b64 = base64.b64encode(buff).decode()
    data = {
        'id': camera_id,
        'frame': b64
    }
    return json.dumps(data).encode('utf-8')


def publish_camera(topic, video):
    producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=lambda x: encode(x))
   
    camera = cv2.VideoCapture(0)
    try:
        while True:
            success, frame = camera.read()
            print(encode(frame))
            producer.send(topic, frame)
            print("sending...")
            time.sleep(3)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        topic_name = "videos"  # sys.argv[1]
        video_path = "c"  # sys.argv[2]
        publish_camera(topic_name, video_path)
    else:
        print("dont have any topic or video")