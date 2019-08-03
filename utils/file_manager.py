import os


class FileManager(object):
    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    def list_file(self) -> list:
        root = self.root_dir
        if root[-1] in ['/', '\\']:
            root = root[:-1]

        root_len = len(self.root_dir) + 1
        res = []
        for path in os.walk(root):
            if path[2]:
                for file in path[2]:
                    res.append((path[0].replace('\\', '/') + '/' + file)[root_len:])
        return res

    def read_file(self, file_name: str) -> bytes:
        with open(os.path.join(self.root_dir, file_name), 'rb') as file:
            data = file.read()
        return data

    def write_file(self, file_name: str, data: bytes):
        path = os.path.join(self.root_dir, file_name)
        if not os.path.isdir(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError as e:
                if isinstance(e, Exception):
                    pass

        with open(path, 'wb') as file:
            file.write(data)

    def del_file(self, file_name: str):
        path = os.path.join(self.root_dir, file_name)
        if os.path.isfile(path):
            os.remove(path)

    def clear_empty_folder(self):
        for path in os.walk(self.root_dir, False):
            if path[0] == self.root_dir:
                continue
            if not path[1] and not path[2]:
                os.rmdir(path[0])
