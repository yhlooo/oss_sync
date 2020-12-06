# -*- coding: utf-8 -*-

"""本地文件管理

该模块定义了一些与本地文件管理相关的类和方法
"""

import logging
import os
from typing import List


logger: logging.Logger = logging.getLogger(f'oss_sync.{__name__}')


class FileManager(object):
    def __init__(self, root_dir: str) -> None:
        """初始化

        Args:
            root_dir: 文件根文件夹

        """

        self.root_dir: str = root_dir

    def list_file(self) -> List[str]:
        """列出文件

        遍历根目录下所有文件

        Returns:
            返回格式如下：

            [
                'file1_path',
                'file2_path',
                'file3_path',
                # ...
            ]

        """
        logger.debug(f'ls \'{self.root_dir}\'')

        root = self.root_dir
        if root[-1] in ['/', '\\']:
            root = root[:-1]

        root_len = len(self.root_dir) + 1
        files_list = []

        for path in os.walk(root):
            if path[2]:
                for file in path[2]:
                    files_list.append((path[0].replace('\\', '/') + '/' + file)[root_len:])

        logger.debug('Local Files:')
        for i in files_list:
            logger.debug(f'  - {i}')

        return files_list

    def read_file(self, file_name: str) -> bytes:
        """读文件

        Args:
            file_name: 读取的文件基于根目录的文件路径

        Returns:
            读取的内容

        """
        path = os.path.join(self.root_dir, file_name)

        logger.debug(f'read \'{path}\'')
        with open(path, 'rb') as file:
            data = file.read()

        return data

    def write_file(self, file_name: str, data: bytes) -> None:
        """写文件

        Args:
            file_name: 写入的文件基于根目录的文件路径
            data: 写入的数据

        """
        path = os.path.join(self.root_dir, file_name)

        if not os.path.isdir(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError as err:
                logger.debug(f'正在创建的文件夹已存在： {err}')
                pass

        logger.debug(f'write \'{path}\'')
        with open(path, 'wb') as file:
            file.write(data)

    def del_file(self, file_name: str) -> None:
        """删除文件

        Args:
            file_name: 删除的文件基于根目录的文件路径

        """
        path = os.path.join(self.root_dir, file_name)

        if os.path.isfile(path):
            logger.debug(f'rm \'{path}\'')
            os.remove(path)

    def clear_empty_folder(self) -> None:
        """清理空文件夹

        Notes:
            - 只要一个文件夹中的所有子文件夹都不含文件，则该文件夹为空文件夹

        """

        for path in os.walk(self.root_dir, False):

            if path[0] == self.root_dir:
                continue

            if not path[1] and not path[2]:
                logger.debug(f'rmdir \'{path[0]}\'')
                os.rmdir(path[0])
