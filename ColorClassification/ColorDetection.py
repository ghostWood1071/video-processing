import numpy as np
import pandas as pd
import cv2


class DetectColorModel:
    def __init__(self):
        index = ["color", "color_name", "hex", "R", "G", "B"]
        self.csv = pd.read_csv('ColorClassification/colors.csv', names=index, header=None)
        r = g = b = 0

    def recognize_color(self, R, G, B):
        minimum = 10000
        color_name = ""
        for i in range(len(self.csv)):
            d = abs(R - int(self.csv.loc[i, "R"])) + abs(G - int(self.csv.loc[i, "G"])) + abs(B - int(self.csv.loc[i, "B"]))
            if d <= minimum:
                minimum = d
                color_name = self.csv.loc[i, "color_name"]
        return color_name

    def detect(self, img):
        (w, h) = img.shape[:2]
        b, g, r = img[w//2, h//2]
        b = int(b)
        g = int(g)
        r = int(r)
        text = self.recognize_color(r, g, b)
        if r + g + b > 600:
            text = 'black'
        return text
