from keras.models import load_model, model_from_json
from keras.preprocessing.image import load_img, img_to_array
import numpy as np
from numpy import expand_dims
import cv2


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


    def decode_netout(self, netout, anchors, obj_thresh, net_h, net_w):
        list_box = list()
        list_conf = list()
        class_ids = list()
        grid_h, grid_w = netout.shape[:2]
        nb_box = 3
        netout = netout.reshape((grid_h, grid_w, nb_box, -1))
        netout[..., :2] = self._sigmoid(netout[..., :2])
        netout[..., 4:] = self._sigmoid(netout[..., 4:])
        netout[..., 5:] = netout[..., 4][..., np.newaxis] * netout[..., 5:]
        netout[..., 5:] *= netout[..., 5:] > obj_thresh

        for i in range(grid_h * grid_w):
            row = i / grid_w
            col = i % grid_w
            for b in range(nb_box):
                detection = netout[int(row)][col][b]
                classes = detection[5:]
                class_id = np.argmax(classes)
                conf = classes[class_id]
                if conf > obj_thresh:
                    x, y, w, h = netout[int(row)][int(col)][b][:4]

                    x = (col + x) / grid_w  # center position, unit: image width
                    y = (row + y) / grid_h  # center position, unit: image height
                    w = anchors[2 * b + 0] * np.exp(w) / net_w  # unit: image width
                    h = anchors[2 * b + 1] * np.exp(h) / net_h  # unit: image height
                    # last elements are class probabilities
                    list_box.append([x - w / 2, y - h / 2, x + w / 2, y + h / 2])
                    list_conf.append((float(conf)))
                    class_ids.append(class_id)
        return list_box, list_conf, class_ids


    def correct_yolo_boxes(self, boxes, image_h, image_w, net_h, net_w):
        new_w, new_h = net_w, net_h
        for i in range(len(boxes)):
            x_offset, x_scale = (net_w - new_w) / 2. / net_w, float(new_w) / net_w
            y_offset, y_scale = (net_h - new_h) / 2. / net_h, float(new_h) / net_h
            boxes[i][0] = int((boxes[i][0] - x_offset) / x_scale * image_w)
            boxes[i][1] = int((boxes[i][1] - y_offset) / y_scale * image_h)
            boxes[i][2] = int((boxes[i][2] - x_offset) / x_scale * image_w)
            boxes[i][3] = int((boxes[i][3] - y_offset) / y_scale * image_h)


    def create_box(self, frame, boxes, classes, class_ids, confidences):

        indexes = cv2.dnn.NMSBoxes(boxes, confidences, 0.6, 0.6)
        font = cv2.FONT_HERSHEY_PLAIN
        colors = np.random.uniform(0, 255, size=(len(boxes), 3))
        for i in indexes.flatten():
            x, y, xm, ym = boxes[i]
            label = str(classes[class_ids[i]])
            confidence = str(round(confidences[i], 2))
            color = colors[i]
            cv2.rectangle(frame, (x, y), (xm, ym), color, 2)
            cv2.putText(frame, label + " " + confidence, (x, y + 20), font, 2, (255, 255, 255), 2)
        return frame


    def load_image_pixels(self, frame, shape):
        image = frame
        width, height = image.shape[0], image.shape[1]
        image = cv2.resize(image, shape, interpolation=cv2.INTER_AREA)
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
            list_box, list_conf, list_ids = self.decode_netout(yhat[i][0], anchors[i], class_threshold, input_h, input_w)
            boxes += list_box
            confidences += list_conf
            class_ids += list_ids
        self.correct_yolo_boxes(boxes, image_h, image_w, input_h, input_w)
        result = self.create_box(frame, boxes, self.labels, class_ids, confidences)
        return result

