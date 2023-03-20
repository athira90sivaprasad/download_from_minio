import os
from minio import Minio

from config.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_NAME, temp_dir_name, file_type


def get_file_from_minio(bucket_name: str):
    client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    for objects in client.list_objects(BUCKET_NAME, recursive=True):
        for f_type in file_type:
            if objects.object_name.endswith(f_type):
                file_data = client.get_object(bucket_name=bucket_name, object_name=objects.object_name).read()
                # files saved based on the folder name . This varies based on each bucket.
                obj_split = objects.object_name.split("/")
                file_name = obj_split[-1]
                obj_split[-1] = ""
                if "OCR" in obj_split:
                    obj_split.remove("OCR")
                path = temp_dir_name+"/"+"/".join(obj_split)
                if not os.path.exists(path):
                    os.makedirs(path)
                with open(f"{path}/{file_name}", "w") as writer:
                    writer.write(file_data.decode('utf-8'))

get_file_from_minio(BUCKET_NAME)
