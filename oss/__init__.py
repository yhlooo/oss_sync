# -*- coding: utf-8 -*-

from .abstract_oss import OssBucket
from .aliyun_oss import AliyunOssBucket
from .tencent_cos import QcloudCosBucket

__all__ = [
    'OssBucket',
    'AliyunOssBucket',
    'QcloudCosBucket',
]
