import cv2
import time
import pycuda.autoinit
from .utils.yolo_with_plugins import TrtYOLO
from .utils.visualization import BBoxVisualization
from .utils.yolov3_classes import get_cls_dict

model_path = './api/yolov3_onnx/yolov4-tiny-416.trt'
trt_yolov4 = TrtYOLO(model_path, (416, 416), 80)

class Recognition():
    def __init__(self):

        self.personCount = 0

    def YoloV4(image):

        boxes, confs, clss = trt_yolov4.detect(self.__recognitionModel.frame.image, 0.4)
            for box,cls in zip(boxes,clss):
                if cls == 0:
                    self.personCount +=1

        return self.personCount