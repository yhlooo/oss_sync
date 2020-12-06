# -*- coding: utf-8 -*-

"""腾讯云 COS Bucket

基于腾讯云 COS 的 API 实现的 .abstract_oss.OSSBucket 的子类
"""

import logging
from typing import Dict, List, Optional, Tuple

from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from .abstract_oss import OssBucket


logger = logging.getLogger(__name__)


class QcloudCosBucket(OssBucket):
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

    def list_objects(self) -> List[Tuple[str, str]]:
        """列出对象

        列出 Bucket 中的对象

        Returns:
            [
                (obj_key_1, obj_md5_1),
                (obj_key_2, obj_md5_2),
                (obj_key_3, obj_md5_3),
                # ...
            ]

        """

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

    def put_object(self, obj_key: str, data: bytes) -> bool:
        """上传对象

        上传对象到 Bucket

        Args:
            obj_key: 对象 Key
            data: 对象内容

        Returns:
            是否成功

        """

        try:
            self.client.put_object(self.bucket, data, obj_key, EnableMD5=True)
        except Exception as e:
            logger.error(e)
            return False
        return True

    def get_object(self, obj_key: str) -> Optional[bytes]:
        """下载对象

        下载 Bucket 中的对象

        Args:
            obj_key: 对象 Key

        Returns:
            如果成功返回对象内容，否则返回 None

        """

        try:
            response = self.client.get_object(self.bucket, obj_key)
            file = response['Body'].get_raw_stream().read()
        except Exception as e:
            logger.error(e)
            return None
        return file

    def del_object(self, obj_key: str) -> bool:
        """删除对象

        删除 Bucket 中的对象

        Args:
            obj_key: 对象 Key

        Returns:
            是否成功

        """

        try:
            self.client.delete_object(self.bucket, obj_key)
        except Exception as e:
            logger.error(e)
            return False
        return True
