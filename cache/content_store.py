import os
import shutil
import tempfile
import hashlib
from pathlib import Path
from typing import Optional, BinaryIO, Callable
from .config import CacheConfig
from .exceptions import DiskFullError


class ContentStore:
    """Управляет физическими файлами в кэше"""
    def __init__(self):
        """Инициализация: создаём папку content/"""
        self.content_dir = CacheConfig.get_content_dir()
        self.content_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, file_id: str) -> Path:
        """Превращает file_id в путь внутри content/."""
        hash_name = hashlib.md5(file_id.encode()).hexdigest()
        return self.content_dir / hash_name

    def get(self, file_id: str) -> Optional[Path]:
        """Вернёт путь к файлу в кэше, если он существует"""
        cache_path = self._get_cache_path(file_id)
        return cache_path if cache_path.exists() else None

    def put(self, file_id: str, source_path: Path) -> Path:
        """Поместить существующий файл в кэш."""
        dest_path = self._get_cache_path(file_id)
        # Копируем, а не перемещаем, потому что source_path может быть нужен
        shutil.copy2(source_path, dest_path)
        return dest_path

    def put_from_stream(self, file_id: str, writer: Callable[[BinaryIO], None]) -> Path:
        """Сохранить данные из потока в кэш."""
        dest_path = self._get_cache_path(file_id)
        tmp_path = dest_path.with_suffix('.tmp')

        try:
            # Открываем временный файл для записи
            with open(tmp_path, 'wb') as tmp_file:
                writer(tmp_file)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())  # Принудительно сбрасываем на диск

            # Переименовываем (атомарно в POSIX)
            shutil.move(str(tmp_path), str(dest_path))

            return dest_path

        except Exception:
            # При любой ошибке удаляем временный файл
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    def delete(self, file_id: str):
        """Удалить файл из кэша"""
        cache_path = self._get_cache_path(file_id)
        if cache_path.exists():
            cache_path.unlink()

    def read(self, file_id: str, offset: int = 0, size: int = -1) -> bytes:
        cache_path = self.get(file_id)
        if cache_path is None:
            raise FileNotFoundError(f"File {file_id} not in cache")

        with open(cache_path, 'rb') as f:
            f.seek(offset)
            if size == -1:
                return f.read()
            else:
                return f.read(size)
       
    def get_size(self, file_id: str) -> int:
        """Вернуть размер файла в кэше в байтах"""
        cache_path = self.get(file_id)
        if cache_path is None:
            raise FileNotFoundError(f"File {file_id} not in cache")
        return cache_path.stat().st_size

    def exists(self, file_id: str) -> bool:
        """Проверить, есть ли файл в кэше"""
        return self.get(file_id) is not None
