import base64
import json
import sys
import time
import cv2
from kafka import KafkaProducer
from datetime import datetime
from uuid import uuid4
import requests
import threading
from typing import *
import os
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;udp"



class  IntervalTask(threading.Thread):
    def __init__(self, event: threading.Event, iter_time: int, call_back):
        threading.Thread.__init__(self)
        self.event = event
        self.iter_time = iter_time*60
        self.callback = call_back

    def run(self):
        # global segment_id
        while True:
            # segment_id = str(uuid4())
            self.callback()
            self.event.set()
            time.sleep(self.iter_time)

class UploadVideo(threading.Thread):
    def __init__(self, cam_id, file_name):
        threading.Thread.__init__(self)
        self.cam_id = cam_id
        self.file_name = file_name

    def run(self):
        try:
            url = f'http://master:9870/webhdfs/v1/video_cam/{self.camera_id}/{self.file_name}?op=CREATE'
            file_path = ""+ self.file_name
            file = open(file_path, mode='rb')
            res = requests.put(url, files={'form_field_name': file})
            return res.ok
        except Exception as e:
            print(str(e))

class WriteVideo(threading.Thread):
    def __init__(self, event: threading.Event, video_source, f_w: int, f_h:int):
        threading.Thread.__init__(self)
        self.event = event
        self.f_w = f_w
        self.f_h = f_h
        self.video_source = video_source
    
    def create_video_writer(self, video_name, f_w, f_h):
        return cv2.VideoWriter(video_name+".avi",cv2.VideoWriter_fourcc('M','J','P','G'), 10, (f_w,f_h))

    def run(self):
        global segment_id
        global cam_id
        global access_frame
        segment_id_backup = segment_id
        video_writer = self.create_video_writer(segment_id, self.f_w, self.f_h)
        for frame in self.video_source:
            if self.event.is_set():
                video_writer.release()
                upload_task = UploadVideo(camera_id, segment_id_backup)
                upload_task.start()
                upload_task.join()
                segment_id_backup = segment_id
                video_writer = self.create_video_writer(segment_id, self.f_w, self.f_h)
                print(segment_id)
            access_frame = frame
            video_writer.write(frame)

class Producer(IntervalTask):
    def __init__(self, hosts:List[str], topic, iter_time, callback):
        IntervalTask.__init__(self, None, iter_time, callback)
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=hosts, value_serializer=lambda x: self.encode(x))
        
    def encode(self, frame):
        global segment_id
        _, buff = cv2.imencode('.jpg', frame)
        b64 = base64.b64encode(buff).decode()
        global camera_id
        send_time = datetime.now()
        data = {
            'video_id': camera_id,
            'segment_id': segment_id,
            'frame': b64,
            'send_time': str(send_time.timestamp())
        }
        return json.dumps(data).encode('utf-8')

    def run(self):
        global segment_id
        self.iter_time = int(self.iter_time/60)
        while True:
            self.callback(self.producer, self.topic)
            time.sleep(self.iter_time)

def set_segment_id():
    global segment_id 
    segment_id = str(uuid4())

def send_to_kafka(producer, topic):
    global access_frame
    if access_frame is None:
        return
    frame = access_frame
    send_frame = cv2.resize(frame, (640,640), interpolation=cv2.INTER_AREA)
    producer.send(topic, send_frame)
    
def get_frames(): 
    ip = "rtsp://admin:thinh111@192.168.100.110:554/onvif1"
    cap = cv2.VideoCapture(ip, cv2.CAP_FFMPEG)
    print(cap.isOpened())
    if not cap.isOpened():
        print('Cannot open RTSP stream')
        exit(-1)
    while True:
        success, frame = cap.read()
        if not success:
            break
        yield frame

def run(topic):
    global camera_id
    global segment_id
    global start_time
    global checking_change 
    global access_frame
    access_frame = None
    camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"
    segment_id = str(uuid4())
    start_time = datetime.now()

    hosts = ['192.168.100.124:9092', '192.168.100.125:9093']
    camsource = get_frames()
    new_segment_event = threading.Event()
    gen_segment_task = IntervalTask(new_segment_event, 1, set_segment_id)
    write_video_task = WriteVideo(new_segment_event, camsource, 1280, 720)
    producer_task = Producer(hosts, topic, 3, send_to_kafka)

    gen_segment_task.start()
    write_video_task.start()
    producer_task.start()

    gen_segment_task.join()
    write_video_task.join()
    producer_task.join()
    
    
if __name__ == '__main__':
    if len(sys.argv) == 1:
        topic_name = "video"  # sys.argv[1]
        # video_path = "c"  # sys.argv[2]
        run(topic_name)
    else:
        print("dont have any topic or video")
