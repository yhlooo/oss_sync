import logging
from typing import Dict
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
import sys

from abstract_oss import OSSBucket


logging.basicConfig(level=logging.ERROR, stream=sys.stdout)


class TencentCOSBucket(OSSBucket):
    def __init__(self, config: Dict[str, str]):
        self.bucket = config.get('bucket')
        oss_config = CosConfig(
            Region=config.get('region'),
            SecretId=config.get('secret_id'),
            SecretKey=config.get('secret_key'),
            Token=config.get('token'),
            Scheme=config.get('scheme')
        )
        self.client = CosS3Client(oss_config)

    def list_objects(self) -> list:
        res = []
        marker = '/'
        while marker:
            response = self.client.list_objects(Bucket=self.bucket, Marker=marker)
            res.extend([
                (obj.get('Key'), obj.get('ETag')[1:-1])
                for obj in response.get('Contents', [])
            ])
            marker = response.get('NextMarker')
        return res

    def put_object(self, obj_name: str, data: bytes) -> bool:
        try:
            self.client.put_object(self.bucket, data, obj_name, EnableMD5=True)
        except Exception as e:
            logging.ERROR(e)
            return False
        return True

    def get_object(self, obj_name: str) -> bytes:
        try:
            response = self.client.get_object(self.bucket, obj_name)
            file = response['Body'].get_raw_stream().read()
        except Exception as e:
            logging.ERROR(e)
            return b''
        return file

    def del_object(self, obj_name: str) -> bool:
        try:
            self.client.delete_object(self.bucket, obj_name)
        except Exception as e:
            logging.ERROR(e)
            return False
        return True
