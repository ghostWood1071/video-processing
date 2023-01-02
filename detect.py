import torch
# from models.common import DetectMultiBackend
from  utils.general import (LOGGER, Profile, check_img_size, check_requirements, cv2,non_max_suppression, scale_boxes)
from  utils.plots import colors
from  utils.torch_utils import select_device, smart_inference_mode
import numpy as np
from  utils.augmentations import letterbox
from pathlib import Path
import cv2
import torch.nn as nn
import base64
from uuid import uuid4
from pyspark.sql.types import Row
from  utils.dataloaders import letterbox
from  utils.general import (LOGGER, ROOT, Profile, check_requirements, non_max_suppression, scale_boxes)
from  utils.torch_utils import  smart_inference_mode
    # Ultralytics color palette https://ultralytics.com/




    # YOLOv5 Profile class. Usage: @Profile() decorator or 'with Profile():' context manager
   

class Ensemble(nn.ModuleList):
    # Ensemble of models
    def __init__(self):
        super().__init__()

    def forward(self, x, augment=False, profile=False, visualize=False):
        y = [module(x, augment, profile, visualize)[0] for module in self]
        y = torch.cat(y, 1)  # nms ensemble
        return y, None  # inference, train output

class DetectMultiBackend(nn.Module):
    # YOLOv5 MultiBackend class for python inference on various backends
    def __init__(self, weights, device=torch.device('cpu'), fp16=False, fuse=True, dnn=False, data=None):# scoped to avoid circular import
        
        super().__init__()
        # w = str(weights[0] if isinstance(weights, list) else weights)
        stride = 32  # default stride
        cuda = torch.cuda.is_available() and device.type != 'cpu'  # use CUDA
        model = self.load_weights(weights, device=device, inplace=True, fuse=fuse)
        names = model.module.names if hasattr(model, 'module') else model.names  # get class names
        model.half() if fp16 else model.float()
        self.model = model  # explicitly assign for to(), cpu(), cuda(), half()
        self.__dict__.update(locals())  # assign all variables to self
    

    def load_weights(self, ckpt, device=None, inplace=True, fuse=True):
    # Loads an ensemble of models weights=[a,b,c] or a single model weights=[a] or weights=a
        from  models.yolo import Detect, Model
        model = Ensemble()
        # ckpt = torch.load(weights, map_location='cpu')  # load
        ckpt = (ckpt.get('ema') or ckpt['model']).to(device).float()  # FP32 model

        # Model compatibility updates
        if not hasattr(ckpt, 'stride'):
            ckpt.stride = torch.tensor([32.])
        if hasattr(ckpt, 'names') and isinstance(ckpt.names, (list, tuple)):
            ckpt.names = dict(enumerate(ckpt.names))  # convert to dict

        model.append(ckpt.fuse().eval() if fuse and hasattr(ckpt, 'fuse') else ckpt.eval())  # model in eval mode

        # Module compatibility updates
        for m in model.modules():
            t = type(m)
            if t in (nn.Hardswish, nn.LeakyReLU, nn.ReLU, nn.ReLU6, nn.SiLU, Detect, Model):
                m.inplace = inplace  # torch 1.7.0 compatibility
                if t is Detect and not isinstance(m.anchor_grid, list):
                    delattr(m, 'anchor_grid')
                    setattr(m, 'anchor_grid', [torch.zeros(1)] * m.nl)
            elif t is nn.Upsample and not hasattr(m, 'recompute_scale_factor'):
                m.recompute_scale_factor = None  # torch 1.11.0 compatibility

        # Return model
        if len(model) == 1:
            return model[-1]

        # Return detection ensemble
        # print(f'Ensemble created with {weights}\n')
        for k in 'names', 'nc', 'yaml':
            setattr(model, k, getattr(model[0], k))
        model.stride = model[torch.argmax(torch.tensor([m.stride.max() for m in model])).int()].stride  # max stride
        assert all(model[0].nc == m.nc for m in model), f'Models have different class counts: {[m.nc for m in model]}'
        return model
        
    def forward(self, im, augment=False, visualize=False):

        y = self.model(im, augment=augment, visualize=visualize) if augment or visualize else self.model(im)

        if isinstance(y, (list, tuple)):
            return self.from_numpy(y[0]) if len(y) == 1 else [self.from_numpy(x) for x in y]
        else:
            return self.from_numpy(y)

    def from_numpy(self, x):
        return torch.from_numpy(x).to(self.device) if isinstance(x, np.ndarray) else x

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

def loadData(row): 
    torch.backends.cudnn.benchmark = True  
    img_size=np.array([640,640])
    stride=32 
    auto=True
    frame= decode_frame(row[1])
    im0 = frame.copy()
    im = np.stack([letterbox(x, img_size, stride=stride, auto=auto)[0] for x in [im0]])  # resize
    im = im[..., ::-1].transpose((0, 3, 1, 2))  # BGR to RGB, BHWC to BCHW
    im = np.ascontiguousarray(im) 
    # print(f'type video_id: {type(row[0])}, im: {type(im)}, im0: {type(im0)}')
    return row[0], im, im0

def draw_box(img, box, label, color=(128, 128, 128),txt_color=(255, 255, 255), line_width=10):
    p1, p2 = (int(box[0]), int(box[1])), (int(box[2]), int(box[3]))
    lw = line_width or max(round(sum(img.shape) / 2 * 0.003), 2)
    cv2.rectangle(img, p1, p2, color, thickness=lw, lineType=cv2.LINE_AA)
    if label:
        tf = max(lw - 1, 1)  # font thickness
        w, h = cv2.getTextSize(label, 0, fontScale=lw / 3, thickness=tf)[0]  # text width, height
        outside = p1[1] - h >= 3
        p2 = p1[0] + w, p1[1] - h - 3 if outside else p1[1] + h + 3
        cv2.rectangle(img, p1, p2, color, -1, cv2.LINE_AA)  # filled
        cv2.putText(img,
                    label, (p1[0], p1[1] - 2 if outside else p1[1] + h + 2),
                    0,
                    lw / 3,
                    txt_color,
                    thickness=tf,
                    lineType=cv2.LINE_AA)
    return img



@smart_inference_mode()
def run(
        weights,  # model path or triton URL
        dataset,
        segment_id,
        data=None,  # dataset.yaml path
        imgsz=(640, 640),  # inference size (height, width)
        conf_thres=0.25,  # confidence threshold
        iou_thres=0.45,  # NMS IOU threshold
        max_det=1000,  # maximum detections per image
        device='',  # cuda device, i.e. 0 or 0,1,2,3 or cpu
        classes=None,  # filter by class: --class 0, or --class 0 2 3
        agnostic_nms=False,  # class-agnostic NMS
        augment=False,  # augmented inference
        visualize=False,  # visualize features
        line_thickness=3,  # bounding box thickness (pixels)
        half=False,  # use FP16 half-precision inference
        dnn=False,  # use OpenCV DNN for ONNX inference
):
    # Load model
    device = select_device(device)
    model = DetectMultiBackend(weights, device=device, dnn=dnn, data=data, fp16=half)
    stride, names= model.stride, model.names
    imgsz = check_img_size(imgsz, s=stride) 
    # dataset = loadData()
    seen, dt = 0, (Profile(), Profile(), Profile())
    result = list()


    for row in dataset:
        
        video_id, im, im0s = loadData(row)# infom[0], infom[1], infom[2]
        frame_id = uuid4()
        with dt[0]:
            im = torch.from_numpy(im).to(model.device)
            im = im.half() if model.fp16 else im.float()  # uint8 to fp16/32
            im /= 255  # 0 - 255 to 0.0 - 1.0
            if len(im.shape) == 3:
                im = im[None]  # expand for batch dim
        # Inference
        with dt[1]:
            pred = model(im, augment=augment, visualize=visualize)
        # NMS
        with dt[2]:
            pred = non_max_suppression(pred, conf_thres, iou_thres, classes, agnostic_nms, max_det=max_det)

        
        for det in pred:  # per image
            seen += 1
            im0 = im0s.copy()
            if len(det):
                det[:, :4] = scale_boxes(im.shape[2:], det[:, :4], im0.shape).round()
                for *xyxy, conf, cls in reversed(det):
                    c = int(cls)  # integer class
                    label = f'{names[c]} {conf:.2f}'
                    im0 = draw_box(im0, xyxy, label, colors(c, True), line_width=line_thickness)
                    # obj = {
                    #     'video.video_id': video_id,
                    #     'video.segment_id': str(segment_id),
                    #     'video.frame_id': str(frame_id),
                    #     'object.name': names[c],
                    #     #'video.frame': encode_frame(im0)
                    # }
                    obj = [
                        video_id,
                        str(segment_id),
                        str(frame_id),
                        names[c],
                        #'video.frame': encode_frame(im0)
                    ]
                    # obj = Row(video_id, str(segment_id), str(frame_id), names[c], encode_frame(im0))
                    result.append(obj)
    return result


# imgs = []
# cap = cv2.VideoCapture(0)
# for i in range(1,10):
#     _,frame = cap.read()
#     imgs.append(('lol', encode_frame(frame)))

# ckpt = torch.load('yolov5s.pt', map_location='cpu')
# run(ckpt, imgs, 'lol')
