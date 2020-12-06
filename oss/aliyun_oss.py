# -*- coding: utf-8 -*-

"""阿里云 OSS Bucket

基于阿里云 OSS 的 API 实现的 .abstract_oss.OSSBucket 的子类
"""

import base64
import hmac
import logging
import time
from hashlib import sha1, md5
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from xml.etree import ElementTree

import requests

from .abstract_oss import OssBucket


logger: logging.Logger = logging.getLogger(__name__)


class AliyunOssBucket(OssBucket):
    def __init__(self, config: Dict[str, str]):
        """初始化

        Args:
            config: 阿里云 OSS 初始化相关配置

        """

        self.host: str = config.get('host')
        self.bucket: str = config.get('bucket')
        self.access_key_id: str = config.get('access_key_id')
        self.access_key_secret: str = config.get('access_key_secret')

        assert self.host, 'host 参数的值不能为空'
        assert self.bucket, 'bucket 参数的值不能为空'
        assert self.access_key_id, 'access_key_id 参数的值不能为空'
        assert self.access_key_secret, 'access_key_secret 参数的值不能为空'

    def make_auth(self, auth_info: dict) -> str:
        """计算签名

        Args:
            auth_info: 与签名相关的信息

        Returns:
            签名结果

        """

        verb = auth_info.get('verb')
        content_md5 = auth_info.get('content-md5') or ''
        content_type = auth_info.get('content-type') or ''
        date = time.strftime('%a, %d %b %Y %H:%M:%S GMT')
        canonicalized_oss_headers = auth_info.get('canonicalized_oss_headers') or ''
        canonicalized_resource = auth_info.get('canonicalized_resource') or f'/{self.bucket}/'

        logger.debug(f'verb = \'{verb}\'')
        logger.debug(f'content_md5 = \'{content_md5}\'')
        logger.debug(f'content_type = \'{content_type}\'')
        logger.debug(f'date = \'{date}\'')
        logger.debug(f'canonicalized_oss_headers = \'{canonicalized_oss_headers}\'')
        logger.debug(f'canonicalized_resource = \'{canonicalized_resource}\'')

        string_to_sign = (
            f'{verb}\n'
            f'{content_md5}\n'
            f'{content_type}\n'
            f'{date}\n'
            f'{canonicalized_oss_headers}{canonicalized_resource}'
        )
        logger.debug(f'string_to_sign = \'{string_to_sign}\'')

        signature = base64.b64encode(
            hmac.new(
                key=self.access_key_secret.encode('utf-8'),
                msg=string_to_sign.encode('utf-8'),
                digestmod=sha1
            ).digest()
        ).decode('utf-8')

        auth_header = f'OSS {self.access_key_id}:{signature}'
        logger.debug(f'auth_header = \'{auth_header}\'')

        return auth_header

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
        marker = None

        while True:

            logger.debug(f'marker = \'{marker}\'')

            headers = {
                'Host': self.host,
                'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT'),
                'Authorization': self.make_auth({
                    'verb': 'GET',
                })
            }

            ret = requests.get(
                f'https://{self.host}/',
                headers=headers,
                params={'marker': marker} if marker else None
            )
            logger.debug(f'ret = {ret}')

            if ret.status_code != 200:
                logger.error(f'请求阿里云 OSS 失败： [{ret.status_code}] {ret.headers} - {ret.text}')
                return None

            etree = ElementTree.fromstring(ret.text)

            for content in etree.findall('Contents'):
                objs_list.append((content.find('Key').text, content.find('ETag').text[1:-1]))

            marker = etree.findall('NextMarker')
            if marker:
                marker = marker[0].text
            else:
                break

            logger.debug(f'next_marker = \'{marker}\'')

        logger.debug(f'Remote Objects:')
        for i in objs_list:
            logger.debug(f'  - {i}')

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

        content_type = self.get_content_type(obj_key)

        # 计算Content-MD5
        content_md5 = base64.b64encode(md5(data).digest()).decode('ascii')

        headers = {
            'Host': self.host,
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT'),
            'Content-Type': content_type,
            'Content-MD5': content_md5,
            'Content-Disposition': 'inline',
            'Authorization': self.make_auth({
                'verb': 'PUT',
                'content-md5': content_md5,
                'content-type': content_type,
                'canonicalized_resource': f'/{self.bucket}/{obj_key}'
            })
        }

        ret = requests.put(f'https://{self.host}/{quote(obj_key)}', data=data, headers=headers)
        logger.debug(f'ret = {ret}')

        if ret.status_code != 200:
            logger.error(f'请求阿里云 OSS 失败： [{ret.status_code}] {ret.headers} - {ret.text}')
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

        headers = {
            'Host': self.host,
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT'),
            'Authorization': self.make_auth({
                'verb': 'GET',
                'canonicalized_resource': f'/{self.bucket}/{obj_key}'
            })
        }

        ret = requests.get(f'https://{self.host}/{quote(obj_key)}', headers=headers)
        logger.debug(f'ret = {ret}')

        if ret.status_code != 200:
            logger.error(f'请求阿里云 OSS 失败： [{ret.status_code}] {ret.headers} - {ret.text}')
            return None

        return ret.content

    def del_object(self, obj_key: str) -> bool:
        """删除对象

        删除 Bucket 中的对象

        Args:
            obj_key: 对象 Key

        Returns:
            是否成功

        """

        headers = {
            'Host': self.host,
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT'),
            'Authorization': self.make_auth({
                'verb': 'DELETE',
                'canonicalized_resource': f'/{self.bucket}/{obj_key}'
            })
        }

        ret = requests.delete(f'https://{self.host}/{quote(obj_key)}', headers=headers)
        logger.debug(f'ret = {ret}')

        if ret.status_code != 204:
            logger.error(f'请求阿里云 OSS 失败： [{ret.status_code}] {ret.headers} - {ret.text}')
            return False

        return True
