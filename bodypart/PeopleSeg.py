import numpy as np
import cv2
import matplotlib.pyplot as plt
import torch
from torchvision.models.segmentation import deeplabv3_resnet101
from torchvision import transforms
from colorthief import ColorThief
from PIL import Image


class ColorDetector(ColorThief):
    def __init__(self, image):
        self.image = Image.fromarray(image)

def make_deeplab(device):
    deeplab = deeplabv3_resnet101(pretrained=True).to(device)
    deeplab.eval()
    return deeplab

def create_preprocess():
    return transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])

def apply_deeplab(deeplab, img, device, preprocess):
    input_tensor = preprocess(img)
    input_batch = input_tensor.unsqueeze(0)
    with torch.no_grad():
        output = deeplab(input_batch.to(device))['out'][0]
    output_predictions = output.argmax(0).cpu().numpy()
    return (output_predictions == 15)

def get_color_name(rgb):
    colors = {
        "black": (-32, -32, -32),
        "white": (255, 255, 255),
        "red": (255, 0, 0),
        "lime": (0,255,0),
        "yellow": (255, 255, 0),
        "blue": (0, 0, 255),
        "sky": (51,255,255),
        "cyan": (0, 255, 255),
        "fuchsia ": (255,0,255),
        "maroon": (128,0,0),
        "olive":(128,128,0),      
        "green": (0, 128, 0),       
        "purple": (128,0,128),
        "teal": (0,128,128),        
        "navy": (0,0,128),
        "orange": (255,165,0),
        "brown": (102,51,0)
    }
    min_distance = float("inf")
    closest_color = None
    for color, value in colors.items():
        distance = sum([(i - j) ** 2 for i, j in zip(rgb, value)])
        if distance < min_distance:
            min_distance = distance
            closest_color = color
    return closest_color

def get_color_from_image(image):
    color_thief = ColorDetector(image)
    colors = color_thief.get_palette(color_count=5,quality=5)
    # sorted_colors = sorted(colors, key=lambda x: -x[-1])
    # print(sorted_colors)   
    color = get_color_name(colors[0])
    print("Upper Color: ", color)
    return color 

def detect(model, preprocess, image, device):
    k = min(1.0, 1024/max(image.shape[0], image.shape[1]))
    img = cv2.resize(image, None, fx=k, fy=k, interpolation=cv2.INTER_LANCZOS4)
    mask = apply_deeplab(model, img, device, preprocess)
    new_img = image.copy()
    new_img[mask==False]=-1
    upper_img = new_img[int(new_img.shape[0]/8):int(new_img.shape[0]/2)-20, 0:int(new_img.shape[1])]
    lower_img = new_img[int(new_img.shape[0]/2)+20:int(new_img.shape[0]-20), 0:int(new_img.shape[1])]
    upper_color = get_color_from_image(upper_img)
    lower_color = get_color_from_image(lower_img)
    return (upper_color, lower_color)

# if __name__ == '__main__':
#     device = torch.device("cpu")
#     model = make_deeplab(device)
#     preprocess = create_preprocess()
#     img = cv2.imread('slave02-210.jpg')
#     print(detect(model, preprocess, img, device))
