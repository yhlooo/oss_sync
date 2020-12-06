# -*- coding: utf-8 -*-

"""腾讯云 COS Bucket

基于腾讯云 COS 的 API 实现的 .abstract_oss.OSSBucket 的子类
"""

import logging
from typing import Dict, List, Optional, Tuple

from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosClientError, CosServiceError

from .abstract_oss import OssBucket


logger: logging.Logger = logging.getLogger(__name__)


class QcloudCosBucket(OssBucket):
    def __init__(self, config: Dict[str, str]):
        """初始化

        Args:
            config: 腾讯云 COS 初始化配置

        """

        self.bucket: str = config.get('bucket')

        oss_config: CosConfig = CosConfig(
            Region=config.get('region'),
            SecretId=config.get('secret_id'),
            SecretKey=config.get('secret_key'),
            Token=config.get('token'),
            Scheme=config.get('scheme')
        )

        self.client: CosS3Client = CosS3Client(oss_config)

    def list_objects(self) -> Optional[List[Tuple[str, str]]]:
        """列出对象

        列出 Bucket 中的对象

        Returns:
            正常的话返回以下格式内容

            [
                (obj_key_1, obj_md5_1),
                (obj_key_2, obj_md5_2),
                (obj_key_3, obj_md5_3),
                # ...
            ]

            查询失败的话返回 None

        """

        # 结果对象列表
        objs_list = []

        # 开始的 Key （用于标记分页）
        marker = '/'

        while marker:

            logger.debug(f'Marker = \'{marker}\'')

            try:
                ret = self.client.list_objects(Bucket=self.bucket, Marker=marker)
                logger.debug(f'ret = {ret}')

            except (CosClientError, CosServiceError) as err:
                logger.error(f'请求腾讯云 COS 失败： {type(err).__name__}: {err}')
                return None

            objs_list.extend([
                (obj.get('Key'), obj.get('ETag')[1:-1])
                for obj
                in ret.get('Contents', [])
            ])
            marker = ret.get('NextMarker')

            logger.debug(f'NextMarker = \'{marker}\'')

        return objs_list

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
            ret = self.client.put_object(
                Bucket=self.bucket,
                Key=obj_key,
                Body=data,
                EnableMD5=True
            )
            logger.debug(f'ret = {ret}')

        except (CosClientError, CosServiceError) as err:
            logger.error(f'{type(err).__name__}: {err}')
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
            ret = self.client.get_object(Bucket=self.bucket, Key=obj_key)
            logger.debug(f'ret = {ret}')

            file_content = ret['Body'].get_raw_stream().read()

        except (CosClientError, CosServiceError) as err:
            logger.error(f'{type(err).__name__}: {err}')
            return None

        return file_content

    def del_object(self, obj_key: str) -> bool:
        """删除对象

        删除 Bucket 中的对象

        Args:
            obj_key: 对象 Key

        Returns:
            是否成功

        """

        try:
            ret = self.client.delete_object(Bucket=self.bucket, Key=obj_key)
            logger.debug(f'ret = {ret}')

        except (CosClientError, CosServiceError) as err:
            logger.error(f'{type(err).__name__}: {err}')
            return False

        return True
