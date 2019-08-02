import threading
from hashlib import md5
from abstract_oss import OSSBucket
from .file_manager import FileManager


class OSSSynchronizer(object):
    def __init__(self, local_dir: FileManager, oss_bucket: OSSBucket):
        self.local_dir = local_dir
        self.oss_bucket = oss_bucket
        self.threads_num = 64

    # 检查同步情况
    def sync_checking(self) -> list:
        file_list = self.local_dir.list_file()
        obj_list = self.oss_bucket.list_objects()

        # 将(文件名, ETag)二元组列表转为键值映射字典
        obj_map = dict()
        for obj in obj_list:
            obj_map[obj[0]] = obj[1]

        # 同步列表，三元组(文件名, 是否在本地, ETag)的列表
        sync_list = []

        # 从本地文件列表更新同步列表
        for file in file_list:
            sync_list.append((file, True, obj_map.get(file)))
            if obj_map.get(file):
                obj_map.pop(file)

        # 从OSS对象字典更新同步列表
        for obj, etag in obj_map.items():
            sync_list.append((obj, False, etag))

        return sync_list

    # 从本地同步到OSS
    def sync_from_local_to_oss(self):
        # 进行同步
        def sync(target_list: list):
            for thing in target_list:
                if thing[1]:  # 文件在本地
                    if thing[2] is not None:  # 本地和OSS各有一份
                        data = self.local_dir.read_file(thing[0])
                        file_md5 = md5(data).hexdigest().lower()
                        if file_md5 != thing[2].lower():  # 内容不一致，上传本地文件到OSS
                            res = self.oss_bucket.put_object(thing[0], data)
                            print('{status} [M] {filename}'.format(status='OK  ' if res else 'Fail', filename=thing[0]))
                    else:  # 文件不在OSS，上传本地文件到OSS
                        data = self.local_dir.read_file(thing[0])
                        res = self.oss_bucket.put_object(thing[0], data)
                        print('{status} [+] {filename}'.format(status='OK  ' if res else 'Fail', filename=thing[0]))
                else:  # 文件不在本地，删除OSS上的对应对象
                    res = self.oss_bucket.del_object(thing[0])
                    print('{status} [-] {filename}'.format(status='OK  ' if res else 'Fail', filename=thing[0]))

        sync_lists = self.sync_checking()
        threads_num = self.threads_num
        if len(sync_lists) < self.threads_num:
            threads_num = len(sync_lists)
        target_num = len(sync_lists) // threads_num + (1 if len(sync_lists) % threads_num != 0 else 0)
        threads = []
        for i in range(threads_num):
            threads.append(threading.Thread(
                target=sync,
                args=(sync_lists[(target_num + 1) * i:(target_num + 1) * (i + 1)],)
            ))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

    # 从OSS同步到本地
    def sync_from_oss_to_local(self):
        # 进行同步
        def sync(target_list: list):
            for thing in target_list:
                if thing[1]:  # 文件在本地
                    if thing[2] is not None:  # 本地和OSS各有一份
                        data = self.local_dir.read_file(thing[0])
                        file_md5 = md5(data).hexdigest().lower()
                        if file_md5 != thing[2].lower():  # 内容不一致，下载OSS对应文件
                            res = self.oss_bucket.get_object(thing[0])
                            if res:
                                self.local_dir.write_file(thing[0], res)
                            print('{status} [M] {filename}'.format(status='OK  ' if res else 'Fail', filename=thing[0]))
                    else:  # 文件不在OSS，删除本地文件
                        self.local_dir.del_file(thing[0])
                        print('{status} [-] {filename}'.format(status='OK  ', filename=thing[0]))
                else:  # 文件不在本地，下载OSS上的对应对象
                    res = self.oss_bucket.get_object(thing[0])
                    if res:
                        self.local_dir.write_file(thing[0], res)
                    print('{status} [+] {filename}'.format(status='OK  ' if res else 'Fail', filename=thing[0]))

        sync_lists = self.sync_checking()
        threads_num = self.threads_num
        if len(sync_lists) < self.threads_num:
            threads_num = len(sync_lists)
        target_num = len(sync_lists) // threads_num + (1 if len(sync_lists) % threads_num != 0 else 0)
        threads = []
        for i in range(threads_num):
            threads.append(threading.Thread(
                target=sync,
                args=(sync_lists[(target_num + 1) * i:(target_num + 1) * (i + 1)], )
            ))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 清理空文件夹
        self.local_dir.clear_empty_folder()
