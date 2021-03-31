import copy
import base64  
import cv2
import time

class Stream():
    def __init__(self,streamPort):

        self.videoSource = cv2.VideoCapture(streamPort)

    def Camera(self,frame):


        while True:
            try:
                if self.videoSource.isOpened():
                    ret, frame = self.videoSource.read()
                    Height , Width = frame.shape[:2]
                    scale = None
                    if Height/640 > Width/960:
                        scale = Height/640
                    else:
                        scale = Width/960
                    frame = cv2.resize(frame, (int(Width/scale), int(Height/scale)), interpolation=cv2.INTER_CUBIC)
                    cv2.imshow("CSI",frame)
                    cv2.waitKey(1)
                    globals.frame = frame
                    if ret == False:
                        self.videoSource = cv2.VideoCapture(streamPort)
            except:
                print('Source video is unavailable! reconnecting ....')

        self.videoSource.release()
