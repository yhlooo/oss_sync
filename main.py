import json

from tencent_cos import TencentCOSBucket
from aliyun_oss import AliyunOSSBucket
from utils import FileManager, OSSSynchronizer


if __name__ == '__main__':
    with open('config/blog-assets-cos.json', 'rt', encoding='utf-8') as fp:
        tencent_cos = TencentCOSBucket(json.load(fp))
    with open('config/blog-assets-oss.json', 'rt', encoding='utf-8') as fp:
        aliyun_oss = AliyunOSSBucket(json.load(fp))
    file_manager = FileManager('oss_assets')
    OSSSynchronizer(file_manager, tencent_cos).sync_from_local_to_oss()
    OSSSynchronizer(file_manager, aliyun_oss).sync_from_local_to_oss()
