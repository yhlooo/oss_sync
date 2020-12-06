# -*- coding: utf-8 -*-

"""文件同步

该模块定义了与文件同步相关的类和方法
"""

import logging
import threading
from hashlib import md5
from typing import Callable, List, Optional, Tuple

from oss import OssBucket
from .file_manager import FileManager


logger: logging.Logger = logging.getLogger(__name__)


# 定义一些常用类型别名
SyncList = List[Tuple[str, bool, Optional[str]]]


class OSSSynchronizer(object):

    def __init__(self, local_dir: FileManager, oss_bucket: OssBucket, threads_num: int = 32) -> None:
        """初始化

        Args:
            local_dir: 本地文件夹
            oss_bucket: OSS Bucket
            threads_num: 同步线程数
        """

        self.local_dir: FileManager = local_dir
        self.oss_bucket: OssBucket = oss_bucket
        self.threads_num: int = threads_num

        assert self.local_dir, 'local_dir 参数不能为空'
        assert self.oss_bucket, 'oss_bucket 参数不能为空'
        assert self.threads_num > 0, '同步线程数至少为 1'

    def sync_checking(self) -> SyncList:
        """检查同步情况

        Returns:
            (文件名或对象 Key, 是否在本地, 对象 ETag) 三元组的列表

            返回格式如下

            [
                ('file_or_obj1_name', True, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
                ('file_or_obj2_name', False, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
                ('file_or_obj3_name', True, None),
                # ...
            ]

        """

        files_list = self.local_dir.list_file()
        objs_list = self.oss_bucket.list_objects()

        # 将 (obj_key, obj_etag) 二元组列表转为键值映射字典
        objs_map = {}
        for obj in objs_list:
            objs_map[obj[0]] = obj[1]

        # 同步列表
        sync_list = []

        # 从本地文件列表更新同步列表
        for file_name in files_list:
            sync_list.append((file_name, True, objs_map.get(file_name)))
            if file_name in objs_map:
                objs_map.pop(file_name)

        # 从 OSS 对象字典更新同步列表
        for obj_key, obj_etag in objs_map.items():
            sync_list.append((obj_key, False, obj_etag))

        logger.debug(f'Sync List:')
        for i in sync_list:
            logger.debug(f'  - {i}')

        return sync_list

    def sync_in_multi_threads(self, sync_func: Callable[[SyncList], None]) -> None:
        """使用多线程同步

        Args:
            sync_func: 同步方法

        """

        sync_lists = self.sync_checking()
        threads_num = len(sync_lists) if len(sync_lists) < self.threads_num else self.threads_num
        target_num = len(sync_lists) // threads_num + (1 if len(sync_lists) % threads_num != 0 else 0)

        # 生成同步线程
        threads = []
        for i in range(threads_num):
            threads.append(threading.Thread(
                target=sync_func,
                args=(sync_lists[(target_num + 1) * i:(target_num + 1) * (i + 1)], )
            ))

        # 启动所有同步线程
        for t in threads:
            t.start()

        # 等待所有同步线程退出
        for t in threads:
            t.join()

    def sync_from_local_to_oss(self) -> None:
        """从本地同步到OSS
        """

        # 进行同步
        def sync(sync_list: SyncList):

            for thing in sync_list:

                # 文件在本地
                if thing[1]:

                    # 本地和 OSS 各有一份
                    if thing[2] is not None:
                        data = self.local_dir.read_file(thing[0])
                        file_md5 = md5(data).hexdigest().lower()

                        # 内容不一致，上传本地文件到 OSS
                        if file_md5 != thing[2].lower():
                            ret = self.oss_bucket.put_object(thing[0], data)
                            logger.info(f'{"OK  " if ret else "Fail"} [M] {thing[0]}')

                        # 内容一致，跳过
                        else:
                            logger.info(f'Skip [S] {thing[0]}')

                    # 文件不在 OSS ，上传本地文件到 OSS
                    else:
                        data = self.local_dir.read_file(thing[0])
                        ret = self.oss_bucket.put_object(thing[0], data)
                        logger.info(f'{"OK  " if ret else "Fail"} [+] {thing[0]}')

                # 文件不在本地，删除 OSS 上的对应对象
                else:
                    ret = self.oss_bucket.del_object(thing[0])
                    logger.info(f'{"OK  " if ret else "Fail"} [-] {thing[0]}')

        self.sync_in_multi_threads(sync)

    def sync_from_oss_to_local(self) -> None:
        """从 OSS 同步到本地
        """

        # 进行同步
        def sync(sync_list: SyncList):

            for thing in sync_list:

                # 文件在本地
                if thing[1]:

                    # 本地和OSS各有一份
                    if thing[2] is not None:
                        data = self.local_dir.read_file(thing[0])
                        file_md5 = md5(data).hexdigest().lower()

                        # 内容不一致，下载 OSS 对应文件
                        if file_md5 != thing[2].lower():
                            ret = self.oss_bucket.get_object(thing[0])
                            if ret:
                                self.local_dir.write_file(thing[0], ret)
                            logger.debug(f'{"OK  " if ret else "Fail"} [M] {thing[0]}')

                        # 内容一致，跳过
                        else:
                            logger.info(f'Skip [S] {thing[0]}')

                    # 文件不在OSS，删除本地文件
                    else:
                        self.local_dir.del_file(thing[0])
                        logger.info(f'{"OK  "} [-] {thing[0]}')

                # 文件不在本地，下载 OSS 上的对应对象
                else:
                    ret = self.oss_bucket.get_object(thing[0])
                    if ret:
                        self.local_dir.write_file(thing[0], ret)
                    logger.info(f'{"OK  " if ret else "Fail"} [+] {thing[0]}')

        self.sync_in_multi_threads(sync)

        # 清理空文件夹
        self.local_dir.clear_empty_folder()
