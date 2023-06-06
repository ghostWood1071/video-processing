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
from happybase import Connection



class  IntervalTask(threading.Thread):
    def __init__(self, event: threading.Event, iter_time: int, call_back, args):
        threading.Thread.__init__(self, args=args)
        self.event = event
        self.iter_time = iter_time*60
        self.callback = call_back

    def run(self):
        global exit_flag
        while True:
            if exit_flag:
                print("exit interval task")
                break
            self.callback()
            self.event.set()
            time.sleep(self.iter_time)

# class UploadVideo(threading.Thread):
#     def __init__(self, cam_id, file_name):
#         threading.Thread.__init__(self)
#         self.cam_id = cam_id
#         self.file_name = file_name+".avi"

#     def run(self):
#         try:
#             url = f'http://master:9870/webhdfs/v1/video_cam/{self.cam_id}/{self.file_name}?op=CREATE'
#             file_path = ""+ self.file_name
#             file = open(file_path, mode='rb')
#             res = requests.put(url, files={'form_field_name': file})
#             return res.ok
#         except Exception as e:
#             print(str(e))

class WriteVideo(threading.Thread):
    def __init__(self, event: threading.Event, video_source, f_w: int, f_h:int, args):
        threading.Thread.__init__(self, args=args)
        self.event = event
        self.f_w = f_w
        self.f_h = f_h
        self.video_source = video_source
        self.hbase_table = 'segments'
    
    def create_video_writer(self, video_name, f_w, f_h):
        return cv2.VideoWriter('./video/'+video_name+".webm",cv2.VideoWriter_fourcc(*'vp90'), 10, (f_w,f_h))
 
    def up_segement_to_Hbase(self, camera_id, segment_id):
        global hbase_host
        global hbase_port
        global start_time
        conn = Connection(hbase_host, port=hbase_port, autoconnect=False)
        conn.open()
        table = conn.table(self.hbase_table)
        table.put(str(segment_id), {
            'video:video_id': camera_id,
            'video:segment_id': segment_id,
            'video:url': f'http://master:9870/webhdfs/v1/video_cam/{camera_id}/{segment_id}.webm?op=OPEN',
            'time:time_start': str(start_time), 
            'time:time_end': str(datetime.now().timestamp())
        })
        conn.close()
        

    def run(self):
        global segment_id
        global camera_id
        global access_frame
        global exit_flag
        segment_id_backup = segment_id
        video_writer = self.create_video_writer(segment_id, self.f_w, self.f_h)
        for frame in self.video_source:
            if exit_flag:
                print("exit video writer")
                break
            if self.event.is_set():
                video_writer.release()
                self.up_segement_to_Hbase(camera_id, segment_id_backup)
                segment_id_backup = segment_id
                video_writer = self.create_video_writer(segment_id, self.f_w, self.f_h)
                print(segment_id)
                self.event.clear()
            access_frame = frame
            video_writer.write(frame)

class Producer(IntervalTask):
    def __init__(self, hosts:List[str], topic, iter_time, callback, args):
        IntervalTask.__init__(self, None, iter_time, callback, args=args)
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
            'send_time': send_time.timestamp()
        }
        return json.dumps(data).encode('utf-8')

    def run(self):
        global segment_id
        global exit_flag
        self.iter_time = int(self.iter_time/60)
        while True:
            if exit_flag:
                print("exit producer.....................")
                break
            self.callback(self.producer, self.topic)
            time.sleep(self.iter_time)

def set_segment_id():
    global segment_id 
    global start_time
    segment_id = str(uuid4())
    start_time = datetime.now().timestamp()

def send_to_kafka(producer, topic):
    global access_frame
    if access_frame is None:
        return
    frame = access_frame
    send_frame = cv2.resize(frame, (640,640), interpolation=cv2.INTER_AREA)
    producer.send(topic, send_frame)
    print("sent...")
    
def get_frames(): 
    ip = "rtsp://admin:thinh111@192.168.100.105:554/onvif1"
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

def get_frames_test():
    print("test ..............................")
    cap = cv2.VideoCapture('2.mp4')
    count = 1
    try:
        while True:
            success, frame = cap.read()
            cv2.imshow('lol', frame) 
            if not success:
                break
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
            # print(frame.shape)
            if count%10==0:
                count = 1
                yield frame
            count+=1
            time.sleep(0.05)
    except Exception as e:
        global exit_flag
        exit_flag = True
        

def run(topic):
    global camera_id
    global segment_id
    global start_time
    global access_frame
    global hbase_host
    global hbase_port
    global exit_flag
    exit_flag = False
    access_frame = None
    camera_id = "c370a4d1-f4b9-4906-a66d-a7292b86ee3a"
    segment_id = str(uuid4())
    start_time = datetime.now().timestamp()
    hbase_host = '192.168.100.124'
    hbase_port = 9090
    hosts = ['192.168.100.124:9092', '192.168.100.125:9093']
    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;udp"

    camsource = get_frames_test()
    new_segment_event = threading.Event()
    gen_segment_task = IntervalTask(new_segment_event, 1, set_segment_id, args=(lambda: exit_flag))
    # write_video_task = WriteVideo(new_segment_event, camsource, 1280, 720, args=(lambda: exit_flag))
    write_video_task = WriteVideo(new_segment_event, camsource, 1910, 1080, args=(lambda: exit_flag))
    producer_task = Producer(hosts, topic, 3, send_to_kafka, args=(lambda: exit_flag))

    gen_segment_task.start()
    write_video_task.start()
    producer_task.start()

    gen_segment_task.join()
    write_video_task.join()
    producer_task.join()
    
    
if __name__ == '__main__':
    if len(sys.argv) == 1:
        topic_name = "video"
        run(topic_name)
    else:
        print("dont have any topic or video")
