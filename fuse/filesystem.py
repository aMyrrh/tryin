import os
import stat
import time
import tempfile
import sys
from fuse import FUSE, FuseOSError, Operations

from cloud.manager import CloudManager


class SimpleCloudFS(Operations):
    def __init__(self, manager):
        self.manager = manager
        self.file_cache = {}
        self.write_buffers = {}
        self.open_files = {}
        self.fd_counter = 0

    def _parse_path(self, path):
        if path == '/' or path == '':
            return None, '/'

        normalized = path.rstrip('/')
        parts = normalized.strip('/').split('/', 1)
        service_label = parts[0]
        service_key = self.manager.get_service_key_by_label(service_label)
        if not service_key:
            return None, None

        if len(parts) == 1:
            return service_key, '/'

        return service_key, '/' + parts[1]

    def _find_entry(self, service_key, cloud_path):
        if cloud_path == '/':
            return {'name': '/', 'type': 'dir', 'size': 0}

        directory = os.path.dirname(cloud_path)
        if directory == '':
            directory = '/'

        items = self.manager.list_files(service_key, directory)
        name = os.path.basename(cloud_path)
        for item in items:
            if item['name'] == name:
                return item
        return None

    def getattr(self, path, fh=None):
        if path == '/':
            return {
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': time.time(),
                'st_mtime': time.time(),
                'st_atime': time.time(),
            }

        service_key, cloud_path = self._parse_path(path)
        if service_key is None or cloud_path is None:
            raise FuseOSError(2)

        if cloud_path == '/':
            return {
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': time.time(),
                'st_mtime': time.time(),
                'st_atime': time.time(),
            }

        entry = self._find_entry(service_key, cloud_path)
        if entry is None:
            raise FuseOSError(2)

        if entry['type'] == 'dir':
            return {
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': 2,
                'st_size': 4096,
                'st_ctime': time.time(),
                'st_mtime': time.time(),
                'st_atime': time.time(),
            }

        return {
            'st_mode': (stat.S_IFREG | 0o644),
            'st_nlink': 1,
            'st_size': entry.get('size', 0),
            'st_ctime': time.time(),
            'st_mtime': time.time(),
            'st_atime': time.time(),
        }

    def readdir(self, path, fh):
        if path == '/':
            entries = ['.', '..']
            entries.extend([self.manager.get_service_label(key) for key in self.manager.get_connected_services()])
            return entries

        service_key, cloud_path = self._parse_path(path)
        if service_key is None or cloud_path is None:
            raise FuseOSError(2)

        entries = ['.', '..']
        if cloud_path == '/':
            items = self.manager.list_files(service_key, '/')
        else:
            items = self.manager.list_files(service_key, cloud_path)

        entries.extend([item['name'] for item in items])
        return entries

    def open(self, path, flags):
        service_key, cloud_path = self._parse_path(path)
        if service_key is None or cloud_path is None:
            raise FuseOSError(2)

        entry = self._find_entry(service_key, cloud_path)
        if entry is None or entry['type'] != 'file':
            raise FuseOSError(2)

        self._ensure_cached(path, service_key, cloud_path)
        fh = self.fd_counter
        self.fd_counter += 1
        self.open_files[fh] = path
        return fh

    def _ensure_cached(self, path, service_key, cloud_path):
        if path in self.file_cache:
            return

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name

        if not self.manager.download_file(service_key, cloud_path, local_path):
            os.unlink(local_path)
            raise FuseOSError(2)

        with open(local_path, 'rb') as f:
            self.file_cache[path] = f.read()
        os.unlink(local_path)

    def read(self, path, size, offset, fh):
        if path not in self.file_cache:
            service_key, cloud_path = self._parse_path(path)
            if service_key is None or cloud_path is None:
                raise FuseOSError(2)
            self._ensure_cached(path, service_key, cloud_path)

        data = self.file_cache[path]
        return data[offset:offset + size]

    def create(self, path, mode, fi=None):
        self.write_buffers[path] = bytearray()
        fh = self.fd_counter
        self.fd_counter += 1
        self.open_files[fh] = path
        return fh

    def write(self, path, data, offset, fh):
        buffer = self.write_buffers.setdefault(path, bytearray())
        end = offset + len(data)
        if len(buffer) < end:
            buffer.extend(b'\x00' * (end - len(buffer)))
        buffer[offset:end] = data
        return len(data)

    def truncate(self, path, length, fh=None):
        buffer = self.write_buffers.setdefault(path, bytearray())
        if len(buffer) > length:
            del buffer[length:]
        else:
            buffer.extend(b'\x00' * (length - len(buffer)))
        return 0

    def flush(self, path, fh):
        if path in self.write_buffers:
            service_key, cloud_path = self._parse_path(path)
            if service_key is not None and cloud_path is not None:
                self._flush_buffer(path, service_key, cloud_path)
        return 0

    def release(self, path, fh):
        if fh in self.open_files:
            del self.open_files[fh]
        if path in self.write_buffers:
            service_key, cloud_path = self._parse_path(path)
            if service_key is not None and cloud_path is not None:
                self._flush_buffer(path, service_key, cloud_path)
        return 0

    def _flush_buffer(self, path, service_key, cloud_path):
        data = self.write_buffers.get(path)
        if data is None:
            return False

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(data)
            local_path = tmp.name

        result = self.manager.upload_file(service_key, local_path, cloud_path)
        os.unlink(local_path)
        if result:
            self.file_cache[path] = bytes(data)
            return True
        return False

    def unlink(self, path):
        service_key, cloud_path = self._parse_path(path)
        if service_key is None or cloud_path is None:
            raise FuseOSError(2)

        if not self.manager.delete_file(service_key, cloud_path):
            raise FuseOSError(2)

        self.file_cache.pop(path, None)
        self.write_buffers.pop(path, None)
        return 0

    def mkdir(self, path, mode):
        service_key, cloud_path = self._parse_path(path)
        if service_key is None or cloud_path is None:
            raise FuseOSError(2)

        if not self.manager.make_directory(service_key, cloud_path):
            raise FuseOSError(2)
        return 0

    def rmdir(self, path):
        return self.unlink(path)

    def rename(self, old, new):
        old_service, old_cloud = self._parse_path(old)
        new_service, new_cloud = self._parse_path(new)
        if old_service is None or new_service is None:
            raise FuseOSError(2)

        if old_service == new_service:
            if not self.manager.move_file(old_service, old_cloud, new_cloud):
                raise FuseOSError(2)
            self.file_cache.pop(old, None)
            self.file_cache.pop(new, None)
            return 0

        # Copy between services if move is not supported across providers
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name

        if not self.manager.download_file(old_service, old_cloud, local_path):
            os.unlink(local_path)
            raise FuseOSError(2)

        if not self.manager.upload_file(new_service, local_path, new_cloud):
            os.unlink(local_path)
            raise FuseOSError(2)

        os.unlink(local_path)
        if not self.manager.delete_file(old_service, old_cloud):
            raise FuseOSError(2)

        self.file_cache.pop(old, None)
        self.file_cache.pop(new, None)
        return 0

    def access(self, path, mode):
        return 0

    def statfs(self, path):
        return {
            'f_bsize': 4096,
            'f_blocks': 1000000,
            'f_bavail': 500000,
            'f_files': 10000,
            'f_ffree': 5000,
        }

    def release(self, path, fh):
        if fh in self.open_files:
            del self.open_files[fh]
        return 0


def main():
    if len(sys.argv) != 2:
        print(f"Использование: {sys.argv[0]} <точка_монтирования>")
        sys.exit(1)

    mount_point = sys.argv[1]
    os.makedirs(mount_point, exist_ok=True)
    print(f"🔧 Монтируем виртуальную файловую систему в {mount_point}")
    print("🛑 Для отмонтирования нажмите Ctrl+C")
    FUSE(SimpleCloudFS(CloudManager()), mount_point, foreground=True, nothreads=False)


if __name__ == "__main__":
    main()
