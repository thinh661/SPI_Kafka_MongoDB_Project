
import datetime
import io
import numpy as np
from pymongo import MongoClient
import gridfs

class save_mongodb():
    def __init__(self):
        host = 'localhost'
        port = 27017

        self.client = MongoClient(host=host,port=port)
        self.db = self.client.results_detec
        self.data_collection = self.db.infor_detec
        self.fs = gridfs.GridFS(self.db)
        
    def push_data_to_mongodb(self,frame, cam_id, frame_id, name,cam_latitude, cam_longitude):
        img_byte_array = io.BytesIO()
        frame.save(img_byte_array, format='JPEG')
        img_byte_array = img_byte_array.getvalue()
        data = {
            'image': img_byte_array,
            'name': name,
            'cam_id': cam_id,
            'frame_id': frame_id,
            'timestamp': datetime.datetime.now(),
            'camera_latitude': cam_latitude,
            'camera_longitude': cam_longitude
        }
        result = self.data_collection.insert_one(data)
        print(f"Image saved to MongoDB with ObjectId: {result.inserted_id}")
            

    def get_data_from_mongodb(self):
        image = self.data_collection.find_one({'name': 'spi_frame'})['frame']
        gout = self.fs.get(image['image_encoded'])
        img = np.frombuffer(gout.read(), dtype=np.uint8)
        frame = np.reshape(img, image['shape'])
            
        return frame
    
# save = save_mongodb()
# if save:
#     print("Đã kết nối!")
# save.push_data_to_mongodb(1,1,1,"thinh",106,108)