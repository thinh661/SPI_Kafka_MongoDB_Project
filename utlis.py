import datetime
import io
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
from pymongo import MongoClient
from save_img_to_mongoDB import save_mongodb
import matplotlib as plt
plt.interactive(True)
import json
from json import JSONEncoder
from threading import Thread
import cv2, time
import gridfs
import numpy as np
import hashlib
import imutils
import base64
from datetime import date
from face_recog import FaceRecognition
from PIL import Image
import folium
from IPython.display import display
from ipyleaflet import (
    Map, Marker, MarkerCluster,
    TileLayer, ImageOverlay,
    Polyline, Polygon, Rectangle, Circle, CircleMarker,
    Popup,
    GeoJSON,
    DrawControl,
    basemaps,
    FullScreenControl
)
from ipywidgets import HTML
import IPython
from ipywidgets import widgets
from sidecar import Sidecar
import traceback
import pickle
from vidgear.gears import CamGear
import random

host = 'localhost'
port = 27017

def mkdir_if_none(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        
def get_marker_widget(img_path, name):
    
    try:
    
        image = IPython.display.Image(img_path, width = 300)

        widget = widgets.Image(
            value=image.data,
            format='jpg', 
            width=300,
            height=400,
        )

        return widget
    except Exception:
        print(traceback.format_exc())
        safe_message = HTML()
        safe_message.value = name
        return safe_message



class Utlis:
    
    def __init__(self, mode,
                       streaming_kafka_topic="spi_topic",
                       kafka_bootstrap_servers=["localhost:9092"],
                       json_requests_file_path=os.path.join(os.getcwd(), "json_requests.json"),
                       data_store_dir = os.path.join(os.getcwd(), "frame_data"),
                       face_store_dir = os.path.join(os.getcwd(), "faces")
                ):
        self.data_store_dir = data_store_dir
        mkdir_if_none(self.data_store_dir)
        self.face_store_dir = face_store_dir
        self.width = 640
        
        self.job_list = []
        self.results = []
        
        self.streaming_kafka_topic = streaming_kafka_topic
        
        self.producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
        
        self.consumer = KafkaConsumer(self.streaming_kafka_topic, bootstrap_servers=kafka_bootstrap_servers)
        
        
        self.json_requests_dict = self.read_json_data_into_dict(json_requests_file_path)
        if mode != "consumer":
            if self.json_requests_dict is None:
                print("<<<ERRROR: No video requests tag was found is provided json.>>>")
            else:
                print("Requested video sources are:")
                for v in self.json_requests_dict:
                    print(f"  - {self.json_requests_dict[v]['video_path']}")
                print("\n")
        
        if mode == "recognition":
            self.sfr = FaceRecognition()
            self.sfr.load_encoding_images(os.path.join(os.getcwd(), "faces"))
            
            us_center = [38.6252978589571, -97.3458993652344]
            zoom = 0
            self.spi_map = Map(center=us_center, zoom=zoom)
            self.spi_map.add_control(FullScreenControl())
            s = Sidecar(title='SPI Map')
            display(self.spi_map)
        
        self.recognized_faces_and_locations_set = set()

        
    def read_json_data_into_dict(self, json_file_path):
        with open(json_file_path) as json_data:
            data = json.load(json_data)
        if "video_requests" in data:
            return data["video_requests"]
        else:
            return None
        
    @staticmethod
    def encode_message(message):
        return json.dumps(message).encode('utf-8')
    
    @staticmethod
    def decode_message(message):
        message_string = message.decode('utf-8')       
        message_json = json.loads(message_string)
        return message_json
        
    def produce_kafka_messages(self):
        for vid_request in self.json_requests_dict:
            kafka_message = {
                             "video_path": self.json_requests_dict[vid_request]["video_path"],
                             "video_tag": vid_request,
                             "camera_id": self.json_requests_dict[vid_request]["camera_id"],
                             "camera_latitude": self.json_requests_dict[vid_request]["camera_latitude"],
                             "camera_longitude" : self.json_requests_dict[vid_request]["camera_longitude"]
                            }
            self.producer.send(self.streaming_kafka_topic, self.encode_message(kafka_message))
                               
    def consume_kafka_messages(self):
        for message in self.consumer:
            decoded_message = self.decode_message(message.value)
            yield decoded_message
                
        
        
    def run_video_submission_job(self, message):
        src = message["video_path"]
        cam_id = message["camera_id"]
        cam_latitude = message["camera_latitude"]
        cam_longitude = message["camera_longitude"]
        threaded_camera = ThreadedCamera(self.data_store_dir, self.width, cam_id, cam_latitude, cam_longitude, src)
        print(f"Started processing: {src}")
        
            
            
    def frame_transmitter(self):
        kafka_consumer_w_messg_decoder = self.consume_kafka_messages()
        for decoded_message in kafka_consumer_w_messg_decoder:
            print("decoded_message:", decoded_message)
            self.run_video_submission_job(decoded_message)
            
        
    def perform_spark_streaming_and_processing(self, patience=200):
        
        processed_data = []
        frame_counter = 0
         
        waited_for = 0
        
        while True:
            
            time.sleep(2)
            
            ongoing_files = os.listdir(self.data_store_dir)
            random.shuffle(ongoing_files)
            new_files = [i for i in ongoing_files if i not in processed_data]
            print(f"New files #: {len(new_files)}")
            
            # setup auto-shutdown:
            if new_files == []:
                waited_for+=1
            else:
                waited_for = 0
            if waited_for >= patience:
                print("Quá thời gian chờ frame. Tiến hành thoát khỏi chương trình!")
                
                break
            start = time.time()

            for f in new_files:
                try:
                    
                    with open(os.path.join(self.data_store_dir, f), "rb") as input_file:
                        frame_data = pickle.load(input_file)
                    frame_counter+=1
                    frame = Image.fromarray(np.array(frame_data["frame"]).astype(np.uint8))
                    cam_id = frame_data["cam_id"]
                    cam_latitude = frame_data["cam_latitude"]
                    cam_longitude = frame_data["cam_longitude"]

                    face_locations, face_names = self.sfr.detect_known_faces(np.asarray(frame))
                    for face_loc, name in zip(face_locations, face_names):
                        if name != "Unknown":
                            current_poi_img_path = os.path.join(self.face_store_dir, f"{name}.jpg")
                            current_poi_img_marker = get_marker_widget(current_poi_img_path, name)
                            found_name_location = f"{name}\n{cam_latitude}, {cam_longitude}\ncam_id={cam_id}"
                            if not (found_name_location in self.recognized_faces_and_locations_set):
                                self.recognized_faces_and_locations_set.add(found_name_location)
                                mark = Marker(location=[cam_latitude, cam_longitude], title=found_name_location, draggable=False)
                                mark.popup = current_poi_img_marker
                                self.spi_map+=mark
                                mark.interact(opacity=(0.0, 1.0, 0.01))
                                self.recognized_faces_and_locations_set.add(found_name_location)
                            
                                print(f"Phát hiện {name} ở cam_id = {cam_id}, frame_num = {frame_counter} và vĩ/kinh độ = {cam_latitude}/{cam_longitude}")
                                self.sm = save_mongodb()
                                self.sm.push_data_to_mongodb(frame, cam_id, frame_counter, name,cam_latitude, cam_longitude)
                                frame.save(f".\\det\\{cam_id}_{frame_counter}.jpg")
                    processed_data.append(f)
                except Exception:
                    print(f"<<<ERROR with file: {f}>>>")
                    print(traceback.format_exc())
                    print("\n")
            end = time.time()
            total_time = end-start
            if total_time == 0:
                total_time = 0.00001
            fps = frame_counter/total_time
            print(f"FPS: {frame_counter}/{total_time} = {fps} FPS")
                
                
            
        
        
# encode frame when saving into json file:
class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)
    

class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        return json.JSONEncoder.default(self, obj)    

        

class ThreadedCamera(object):
    def __init__(self, data_store_dir, width, cam_id, cam_latitude, cam_longitude, src=0, fps=30):
        
        self.stream_type = "CamGear"            
        try:
            self.stream = CamGear(source=src, stream_mode = True, logging=True).start() # YouTube Video URL as input
            blank = self.stream.read()
            self.stream_type = "CamGear"
        except Exception:
            try:
                self.capture = cv2.VideoCapture(src)
                if self.capture.isOpened():
                    self.status, self.frame = self.capture.read()
                if self.status:
                    self.stream_type = "OpenCV"
                else:
                    self.stream_type = "None"
            except Exception:
                print("Could not read any.")
                self.stream_type = "None"
        
        self.data_store_dir = data_store_dir
        self.base_name = hashlib.sha256(src.encode('utf-8')).hexdigest()
        self.frame_count = 0
        self.fps = fps
        
        self.width = width
        self.cam_id = cam_id
        self.cam_latitude = cam_latitude
        self.cam_longitude = cam_longitude
        
        if self.stream_type != "None":
            self.thread = Thread(target=self.update, args=())
            self.thread.daemon = True
            self.thread.start()
        
        
        
        
    def update(self):
        while True:
            
            # OPENCV APPROACH:
            if self.stream_type == "OpenCV":
                if self.capture.isOpened():
                    self.status, self.frame = self.capture.read()
                else:
                    try:
                        cap.release()
                    except Exception:
                        pass
                    break
                    
            # CamGEAR APPROACH:
            elif self.stream_type == "CamGear":
                self.frame = self.stream.read()
            else:
                print("<<<Provided link format is not supported. Skipping.>>>")
                break
                
            if self.frame is None:
                print("<<<No frame read. Skipping.>>>")
                # Turn off OpenCV module:
                try:
                    cap.release()
                except Exception:
                    pass
                # Turn off CamGear:
                try:
                    self.stream.stop()
                except Exception:
                    pass
                break
            else:
                self.status = True
            
            if self.status:
                self.frame = imutils.resize(self.frame, width=self.width)
                self.frame_count+=1
                
                # for encoding:
                save_frame_as = os.path.join(self.data_store_dir, f"{self.base_name}_{self.frame_count}.pickle")
                
                # json file from image
                data = {
                        'frame' : self.frame,
                        'shape' : str(self.frame.shape),
                        'dtype' : str(self.frame.dtype),
                        'processed': str("False"),
                        'timestamp': str(date.today()),
                        'cam_id' : self.cam_id,
                        'cam_latitude' : self.cam_latitude,
                        'cam_longitude' : self.cam_longitude
                }
                with open(save_frame_as, 'wb') as f:
                    # Pickle the 'data' dictionary using the highest protocol available.
                    pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)