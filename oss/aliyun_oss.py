# -*- coding: utf-8 -*-

"""阿里云 OSS Bucket

基于阿里云 OSS 的 API 实现的 .abstract_oss.OSSBucket 的子类
"""

import base64
import hmac
import time
from hashlib import sha1, md5
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from xml.etree import ElementTree

import requests

from .abstract_oss import OssBucket


class AliyunOssBucket(OssBucket):
    def __init__(self, config: Dict[str, str]):
        """初始化

        Args:
            config:
        """
        self.host = config.get('host')
        self.bucket = config.get('bucket')
        self.access_key_id = config.get('access_key_id')
        self.access_key_secret = config.get('access_key_secret')

        if not self.host or not self.bucket or not self.access_key_id or not self.access_key_secret:
            raise TypeError('缺少必要的初始化参数')

    def make_auth(self, auth_info: dict) -> str:
        """计算签名

        Args:
            auth_info: 与签名相关的信息

        Returns:
            签名结果
        """

        verb = auth_info.get('verb')
        content_md5 = auth_info.get('content-md5') if auth_info.get('content-md5') else ''
        content_type = auth_info.get('content-type') if auth_info.get('content-type') else ''
        date = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())
        canonicalized_oss_headers = (
            auth_info.get('canonicalized_oss_headers')
            if auth_info.get('canonicalized_oss_headers')
            else ''
        )
        canonicalized_resource = (
            auth_info.get('canonicalized_resource')
            if auth_info.get('canonicalized_resource')
            else '/' + self.bucket + '/'
        )

        string_to_sign = (
            f'{verb}\n'
            f'{content_md5}\n'
            f'{content_type}\n'
            f'{date}\n'
            f'{canonicalized_oss_headers}{canonicalized_resource}'
        )
        print(string_to_sign)

        signature = base64.b64encode(
            hmac.new(
                self.access_key_secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                sha1
            ).digest()
        ).decode('utf-8')

        return f'OSS {self.access_key_id}:{signature}'

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

        objs = []
        marker = None

        while True:
            headers = {
                'Host': self.host,
                'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
                'Authorization': self.make_auth({
                    'verb': 'GET',
                })
            }
            res = requests.get(
                'https://' + self.host + '/{marker}'.format(marker=('?marker=' + marker) if marker else ''),
                headers=headers
            )
            etree = ElementTree.fromstring(res.text)
            for content in etree.findall('Contents'):
                objs.append((content.find('Key').text, content.find('ETag').text[1:-1]))

            marker = etree.findall('NextMarker')
            if marker:
                marker = marker[0].text
            else:
                break

        return objs

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
        content_md5 = base64.b64encode(md5(data).digest()).decode()

        headers = {
            'Host': self.host,
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'Content-Type': content_type,
            'Content-MD5': content_md5,
            'Content-Disposition': 'inline',
            'Authorization': self.make_auth({
                'verb': 'PUT',
                'content-md5': content_md5,
                'content-type': content_type,
                'canonicalized_resource': '/' + self.bucket + '/' + obj_key
            })
        }
        res = requests.put('https://' + self.host + '/' + quote(obj_key), data=data, headers=headers)
        print(res.status_code, res.text, res.headers)
        return res.status_code == 200

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
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'Authorization': self.make_auth({
                'verb': 'GET',
                'canonicalized_resource': '/' + self.bucket + '/' + obj_key
            })
        }
        res = requests.get('https://' + self.host + '/' + quote(obj_key), headers=headers)

        if res.status_code != 200:
            return None

        return res.content

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
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'Authorization': self.make_auth({
                'verb': 'DELETE',
                'canonicalized_resource': '/' + self.bucket + '/' + obj_key
            })
        }
        res = requests.delete('https://' + self.host + '/' + quote(obj_key), headers=headers)
        return res.status_code == 204
