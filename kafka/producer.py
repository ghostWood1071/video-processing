import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer
from datetime import datetime
from uuid import uuid4
import requests

global camera_id
global segment_id
global start_time
global checking_change 
hosts = ['192.168.100.124:9092', '192.168.100.125:9093']
camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"
segment_id = str(uuid4())
start_time = datetime.now()
checking_change = False


def gen_segment_id(send_time):
    global start_time
    global segment_id
    global checking_change
    if (send_time - start_time).total_seconds()/60 > 1:
    #if (send_time - start_time).total_seconds() > 5:
        start_time = datetime.now()
        segment_id = str(uuid4())
        checking_change = True
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
        'send_time': str(send_time.timestamp())
    }
    return json.dumps(data).encode('utf-8')

def create_video_writer(video_name, f_w, f_h):
    return {
        'writer':  cv2.VideoWriter(video_name+".avi",cv2.VideoWriter_fourcc('M','J','P','G'), 10, (f_w,f_h)),
        'video_name': video_name+".avi"
    }

def upload_video(file_name):
    url = f'http://master:9870/webhdfs/v1/video_cam/{camera_id}/{file_name}?op=CREATE'
    file_path = ""+file_name
    file = open(file_path, mode='rb')
    res = requests.put(url, files={'form_field_name': file})
    return res.ok

def write_video(out ,video_name, frame, f_w, f_h):
   global checking_change
   if checking_change: 
    out.get('writer').release()
    upload_video(out.get('video_name'))
    out = create_video_writer(video_name, f_w, f_h)
    checking_change = False
   out.get('writer').write(frame)

def publish_camera(cam):
    while True:
        sucess, frame = cam.read()
        if not sucess:
            break;
        time.sleep(3)
        yield frame

def run(topic, video_path):
    global segment_id
    producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=lambda x: encode(x))
    camera = cv2.VideoCapture("1.mp4")
    out = create_video_writer(segment_id, int(camera.get(3)), int(camera.get(4)))
    try:
        for frame in publish_camera(camera):
            send_frame = cv2.resize(frame, (640,640), interpolation=cv2.INTER_AREA)
            producer.send(topic, send_frame)
            write_video(out, segment_id, frame, int(camera.get(3)), int(camera.get(4)))
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        topic_name = "video"  # sys.argv[1]
        video_path = "c"  # sys.argv[2]
        run(topic_name, video_path)
    else:
        print("dont have any topic or video")
