from keras.models import load_model, model_from_json
from keras.preprocessing.image import load_img, img_to_array, array_to_img
import numpy as np
from numpy import expand_dims
import cv2
from PIL import Image as pil_image


class ObjectDetectModel:
    def __init__(self):
        self.cfg_path = 'ObjectDetection/model_cfg.json'
        self.h5_path = 'ObjectDetection/yolo.h5'

        with open(self.cfg_path) as f:
            cfg_json = f.read()
            self.model = model_from_json(cfg_json)

        self.model.load_weights(self.h5_path)

        with open('ObjectDetection/coco.names', mode='r') as f:
            names = f.read()
            self.labels = names.split('\n')
            self.labels.pop()

    def _sigmoid(self, x):
        return 1. / (1. + np.exp(-x))

    def decode_netout(self, netout, anchors, obj_thresh, net_h, net_w, width, height):
        list_box = list()
        list_conf = list()
        class_ids = list()
        grid_h, grid_w = netout.shape[:2]
        nb_box = 3
        netout = netout.reshape((grid_h, grid_w, nb_box, -1))
        nb_class = netout.shape[-1] - 5
        netout[..., :2] = self._sigmoid(netout[..., :2])
        netout[..., 4:] = self._sigmoid(netout[..., 4:])
        netout[..., 5:] = netout[..., 4][..., np.newaxis] * netout[..., 5:]
        netout[..., 5:] *= netout[..., 5:] > obj_thresh

        for i in range(grid_h * grid_w):
            row = i / grid_w
            col = i % grid_w
            for b in range(nb_box):
                detection = netout[int(row)][int(col)][b]
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]
                if confidence >= obj_thresh:  # continue
                    x, y, w, h = netout[int(row)][int(col)][b][:4]
                    center_x = int(((col + x) / grid_w) * width)  # center position, unit: image width
                    center_y = int(((row + y) / grid_h) * height)  # center position, unit: image height
                    w = int((anchors[2 * b + 0] * np.exp(w) / net_w) * width)  # unit: image width
                    h = int((anchors[2 * b + 1] * np.exp(h) / net_h) * height)  # unit: image height
                    x = int(center_x - w / 2)
                    y = int(center_y - h / 2)
                    # last elements are class probabilities
                    list_box.append([x, y, w, h])
                    list_conf.append((float(confidence)))
                    class_ids.append(class_id)
        return list_box, list_conf, class_ids

    def create_box(self, frame, boxes, classes, class_ids, confidences):
        indexes = cv2.dnn.NMSBoxes(boxes, confidences, 0.6, 0.1)
        font = cv2.FONT_HERSHEY_PLAIN
        colors = np.random.uniform(0, 255, size=(len(boxes), 3))
        labels = list()
        frame_things = list()
        for i in indexes.flatten():
            x, y, xm, ym = boxes[i]
            label = str(classes[class_ids[i]])
            labels.append(label)
            confidence = str(round(confidences[i], 2))
            color = colors[i]
            frame_things.append(frame[x:x+xm, y:y+ym])
            cv2.rectangle(frame, (x, y), (x+xm, y+ym), color, 2)
            cv2.putText(frame, label + " " + confidence, (x, y + 20), font, 2, (255, 255, 255), 2)
        return {
            'frame': frame,
            'labels': labels,
            'num_obj': len(labels),
            'frame_thing': frame_things
        }

    def load_image_pixels(self, frame, shape):
        image = frame
        image = array_to_img(image)
        width, height = image.size
        image = image.resize(shape, pil_image.NEAREST)
        image = img_to_array(image)
        image = image.astype('float32')
        image /= 255.0
        image = expand_dims(image, 0)
        return image, width, height

    def detect(self, frame):
        input_w, input_h = 416, 416
        image, image_w, image_h = self.load_image_pixels(frame, (input_w, input_h))
        anchors = [[116, 90, 156, 198, 373, 326], [30, 61, 62, 45, 59, 119], [10, 13, 16, 30, 33, 23]]
        yhat = self.model.predict(image)
        class_threshold = 0.6
        boxes = list()
        confidences = list()
        class_ids = list()
        for i in range(len(yhat)):
            list_box, list_conf, list_id = self.decode_netout(yhat[i][0],
                                                              anchors[i],
                                                              class_threshold,
                                                              input_h,
                                                              input_w,
                                                              image_w,
                                                              image_h)
            boxes += list_box
            confidences += list_conf
            class_ids += list_id
        result = self.create_box(frame, boxes, self.labels, class_ids, confidences)
        return result

    # def detect(self, frame):
    #     input_w, input_h = 416, 416
    #     image, image_w, image_h = self.load_image_pixels(frame, (input_w, input_h))
    #     anchors = [[116, 90, 156, 198, 373, 326], [30, 61, 62, 45, 59, 119], [10, 13, 16, 30, 33, 23]]
    #     yhat = self.model.predict(image)
    #     class_threshold = 0.6
    #     boxes = list()
    #     confidences = list()
    #     class_ids = list()
    #     for i in range(len(yhat)):
    #         list_box, list_conf, list_id = self.decode_netout(yhat[i][0], anchors[i], class_threshold, input_h, input_w, image_w, image_h)
    #         boxes += list_box
    #         confidences += list_conf
    #         class_ids += list_id
    #     result = self.create_box(frame, boxes, self.labels, class_ids, confidences)
    #     return result

