import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Callable, BinaryIO
from .config import CacheConfig
from .metadata_db import MetadataDB
from .content_store import ContentStore
from .lru import LRU
from .exceptions import FileNotCachedError, DiskFullError, PinnedFileError


class CacheManager:
    def __init__(self, max_size_mb: Optional[int] = None):
        if max_size_mb is None:
            max_size_mb = CacheConfig.DEFAULT_MAX_SIZE_MB
        self.max_size_bytes = max_size_mb * 1024 * 1024

        # Инициализируем компоненты
        self.metadata = MetadataDB()
        self.content = ContentStore()
        self.lru = LRU(self.metadata)

        # Блокировка для потокобезопасности
        self._lock = threading.RLock()

        # Статистика (для отладки)
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'downloads': 0,
            'evictions': 0
        }

    def get_metadata(self, path: str) -> Optional[Dict[str, Any]]:
        """Получить метаданные файла"""
        with self._lock:
            return self.metadata.get(path)

    def update_metadata(self, path: str, file_id: str, size: int, 
                        modified: int, etag: str = "", is_dir: bool = False):
        """Обновить метаданные файла"""
        with self._lock:
            self.metadata.set(path, file_id, size, modified, etag, is_dir)

    def is_cached(self, path: str) -> bool:
        """Проверить, скачан ли файл локально"""
        with self._lock:
            meta = self.metadata.get(path)
            if not meta:
                return False
            if meta.get('cached'):
                return self.content.exists(meta['file_id'])
            return False

    def download(self, path: str, download_callback: Callable[[BinaryIO], None]):
        """Скачать файл в кэш."""
        with self._lock:
            meta = self.metadata.get(path)
            if not meta:
                raise ValueError(f"No metadata for {path}")

            if meta.get('cached'):
                return  # уже есть

            if meta['size'] > self.get_free_space():
                self._free_space(meta['size'])

            cache_path = self.content.put_from_stream(meta['file_id'], download_callback)

            self.metadata.mark_cached(path, str(cache_path))

            self.stats['downloads'] += 1

    def read(self, path: str, offset: int = 0, size: int = -1) -> bytes:
        with self._lock:
            meta = self.metadata.get(path)
            if not meta:
                raise FileNotCachedError(f"No metadata for {path}")

            if not meta.get('cached'):
                self.stats['cache_misses'] += 1
                raise FileNotCachedError(f"{path} not in cache")

            try:
                data = self.content.read(meta['file_id'], offset, size)
                self.stats['cache_hits'] += 1

                self.metadata.update_access_time(path)

                return data
            except FileNotFoundError:
                self.metadata.mark_uncached(path)
                raise FileNotCachedError(f"{path} missing from cache")
 
    def get_cache_size(self) -> int:
        """Вернуть текущий размер кэша в байтах"""
        with self._lock:
            return self.metadata.get_total_cached_size()

    def get_free_space(self) -> int:
        """Вернуть свободное место в кэше (лимит - текущий размер)"""
        return self.max_size_bytes - self.get_cache_size()

    def _free_space(self, needed_bytes: int):
        """Освободить место, удаляя старые файлы."""
        candidates = self.lru.get_eviction_candidates(needed_bytes)

        freed = 0
        for path in candidates:
            if self.metadata.get_pinned_status(path):
                continue

            self.evict(path)
            freed += self.metadata.get(path)['size']
            self.stats['evictions'] += 1  
            if freed >= needed_bytes:
                break

        if freed < needed_bytes:
            raise DiskFullError(f"Need {needed_bytes} bytes, only {freed} freed")

    def evict(self, path: str):
        """Принудительно удалить файл из кэша"""
        with self._lock:
            meta = self.metadata.get(path)
            if not meta:
                return
            if meta.get('pinned'):
                raise PinnedFileError(f"Cannot evict pinned file {path}")
            if meta.get('cached'):
                self.content.delete(meta['file_id'])
            self.metadata.mark_uncached(path)

    def pin(self, path: str):
        """Закрепить файл (никогда не удалять автоматически)"""
        with self._lock:
            # Если файл ещё не в кэше — скачиваем
            if not self.is_cached(path):
                # Нужно будет вызвать download извне
                pass
            self.metadata.set_pinned(path, True)

    def unpin(self, path: str):
        """Открепить файл (можно удалять при нехватке места)"""
        with self._lock:
            self.metadata.set_pinned(path, False)

    def cleanup(self):
        """Очистка кэша: удаляем битые файлы"""
        with self._lock:
            # Получаем все файлы, которые по БД должны быть в кэше
            cached_paths = self.metadata.get_all_cached_paths()
     
            for path in cached_paths:
                meta = self.metadata.get(path)
                if meta and meta.get('cached'):
                    # Проверяем, существует ли физический файл
                    if not self.content.exists(meta['file_id']):
                        # Файл потерян — восстанавливаем БД
                        self.metadata.mark_uncached(path)

    def get_stats(self) -> Dict[str, int]:
        """Вернуть статистику использования кэша"""
        with self._lock:
            stats = self.stats.copy()
            stats['total_size_bytes'] = self.get_cache_size()
            stats['max_size_bytes'] = self.max_size_bytes
            stats['free_bytes'] = self.get_free_space()
            stats['total_files'] = len(self.metadata.get_all_cached_paths())
            return stats

    def close(self):
        """Закрыть все соединения"""
        with self._lock:
            self.metadata.close()
