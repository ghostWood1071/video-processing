import base64
import json
import time
from datetime import datetime
import datetime as dt
from uuid import uuid4
from BodyPortionDetect.BodyDetector import  BodyPortionDetector
import cv2
import numpy as np
import requests
import copy


def encode_frame(frame):
    _, buff = cv2.imencode('.jpg', frame)
    ba64 = base64.b64encode(buff)
    b64json = ba64.decode()
    return b64json


def decode_frame(string: str):
    jpg_origin = base64.b64decode(string)
    buff = np.frombuffer(jpg_origin, dtype=np.uint8)
    frame = cv2.imdecode(buff, flags=1)
    return frame


def get_cams() -> dict:
    f = open('camera.config.json', mode='r')
    cameras = json.load(f)
    print("this is camera: ", cameras)
    f.close()
    return cameras


def round_seconds(date_time):
    if date_time.microsecond >= 500_000:
        date_time += dt.timedelta(seconds=1)
    return date_time.replace(microsecond=0)


def get_color(portion: dict) -> str:
    if not portion:
        return ''
    return portion.get('color')


def post_segments(segments):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = json.dumps(segments)
    result = requests.post('http://192.168.248.1:1071/send-segments', data=data, headers=headers)
    return result


def post_sequences(sequences):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = json.dumps(sequences)
    result = requests.post('http://192.168.248.1:1071/send-sequences', data=data, headers=headers)
    return result


def post_frames(frames):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    frame_post = copy.deepcopy(frames)
    for frame in frame_post:
        frame['frame_matrix'] = None
    data = json.dumps(frame_post)
    result = requests.post('http://192.168.248.1:1071/send-frames', data=data, headers=headers)
    return result


def post_people(people):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = json.dumps(people)
    result = requests.post('http://192.168.248.1:1071/send-people', data=data, headers=headers)
    return result


def post_things(things):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = json.dumps(things)
    result = requests.post('http://192.168.248.1:1071/send-things', data=data, headers=headers)
    return result


class DatabaseBusiness:
    def __init__(self):
        self.start_time = time.time()
        self.cams = get_cams()
        self.segments = self.init_frame_seg()
        print("_________ init db business __________")
        print("start time: ", self.start_time)
        print("cameras: ", self.cams)
        print("segments: ", self.segments)
        print("_________initial finished ___________")

    def init_frame_seg(self):
        frame_segments = dict()
        start_time_detail = round_seconds(datetime.now()).__str__()
        for cam_ids in self.cams:
            print(cam_ids)
            frame_segments[cam_ids] = {
                'video_seg_id': uuid4().__str__(),
                'video_id': cam_ids,
                'frame_seqs': [],
                'video_seg_name': round_seconds(datetime.now()).__str__(),
                'start_time': start_time_detail,
                'finish_time': round_seconds(datetime.now()).__str__()
            }
        post_segments(frame_segments)
        return frame_segments

    def reset_frame_segments(self):
        current = time.time()
        print(current-self.start_time)
        start_time_detail = round_seconds(datetime.now()).__str__()
        if current-self.start_time >= 150:
            self.segments.clear()
            for cam_id in self.cams:
                new_segment = {
                    'video_seg_id': uuid4().__str__(),
                    'video_id': cam_id,
                    'frame_seqs': [],
                    'video_seg_name': round_seconds(datetime.now()).__str__(),
                    'start_time': start_time_detail,
                    'finish_time': round_seconds(datetime.now()).__str__()
                }
                self.segments[cam_id] = new_segment
                self.start_time = time.time()
            post_segments(self.segments)

    def update_frame_seqs(self, num_obj, cam_id, obj_list):
        frame_seqs = self.segments[cam_id]['frame_seqs']
        current_len = len(frame_seqs)
        ex = current_len - num_obj
        exts = []
        if ex > 0:
            for i in range(ex):
                frame_seqs.pop()
        else:
            for i in range(num_obj-current_len):
                frame_seq = {
                    'frame_seq_id': uuid4().__str__(),
                    'video_seg_id': self.segments[cam_id]['video_seg_id'],
                    'start_time': round_seconds(datetime.now()).__str__(),
                    'finish_time': '',
                    'description': obj_list[i+current_len]
                }
                # create_frame_seq
                exts.append(frame_seq)
                frame_seqs.append(frame_seq)
            post_sequences(exts)
            return exts

    def create_frame(self, frame_seq, b64_frame, frame):
        frames = list()
        for i in frame_seq:
            frames.append({
                'frame_id': uuid4().__str__(),
                'frame': b64_frame,
                'frame_matrix': frame,
                'frame_seq_id': i['frame_seq_id']
            })
        post_frames(frames)
        return frames

    def create_things(self, cam_id, frames):
        frame_seqs = self.segments[cam_id]['frame_seqs']
        things = list()
        for i, frame in enumerate(frames):
            if frame_seqs[i]['description'] != 'person':
                things.append({
                    'thing_id': uuid4().__str__(),
                    'frame_id': frame['frame_id'],
                    'name': frame_seqs[i]['description'],
                    'description': ''
                })
        post_things(things)
        return things

    def create_people(self, cam_id, frames, body_portion_model: BodyPortionDetector):
        frame_seqs = self.segments[cam_id]['frame_seqs']
        people = list()
        for i, frame in enumerate(frames):
            if frame_seqs[i]['description'] == 'person':
                result = body_portion_model.detect(frame['frame_matrix'])
                people.append({
                    'person_id': uuid4().__str__(),
                    'frame_id': frame['frame_id'],
                    'upper': get_color(result.get('upper')),
                    'lower': get_color(result.get('lower'))
                })
        post_people(people)
        return people




