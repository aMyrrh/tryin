import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from .config import CacheConfig
from .exceptions import CacheCorruptedError


class MetadataDB:
    def __init__(self):
        """Инициализация: создаём папку и БД, если их нет"""
        self.db_path = CacheConfig.get_db_path()
        self._init_database()

    def _init_database(self):
        """Создаёт папку для БД и таблицы, если они не существуют"""
        CacheConfig.get_cache_dir().mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    name TEXT,
                    size INTEGER DEFAULT 0,
                    modified INTEGER NOT NULL,
                    etag TEXT,
                    is_dir INTEGER DEFAULT 0,
                    parent_path TEXT,
                    cached INTEGER DEFAULT 0,
                    pinned INTEGER DEFAULT 0,
                    last_access INTEGER DEFAULT 0,
                    cache_path TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_parent ON files(parent_path);
                CREATE INDEX IF NOT EXISTS idx_last_access ON files(last_access);
                CREATE INDEX IF NOT EXISTS idx_cached ON files(cached);
                -- Таблица версии схемы (для будущих обновлений)
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER
                );
            ''')

            cursor = conn.execute("PRAGMA integrity_check")
            if cursor.fetchone()[0] != 'ok':
                raise CacheCorruptedError("SQLite integrity check failed")

    def get(self, path: str) -> Optional[Dict[str, Any]]:
        """Получить метаданные файла по пути"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM files WHERE path = ?",
                (path,)
            )
            row = cur.fetchone()

        if row is None:
            return None
        
        return dict(row)
    
    def set(self, path: str, file_id: str, size: int, modified: int,
            etag: str = "", is_dir: bool = False, pinned: bool = False):
        """Сохранить или обновить метаданные файла"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                         INSERT OR REPLACE INTO files 
                (path, file_id, name, size, modified, etag, is_dir, pinned, last_access)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                path,
                file_id,
                Path(path).name,  # извлекаем имя файла из пути
                size,
                modified,
                etag,
                1 if is_dir else 0,
                1 if pinned else 0,
                int(time.time())  # last_access = текущее время
            ))

    def delete(self, path: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM files WHERE path = ?", (path,))

    def mark_cached(self, path: str, cache_path: str):
        """Отметить, что файл скачан в кэш"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE files 
                SET cached = 1, cache_path = ?
                WHERE path = ?
            """, (cache_path, path))

    def mark_uncached(self, path: str):
        """Отметить, что файл удалён из кэша"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE files 
                SET cached = 0, cache_path = NULL
                WHERE path = ?
            """, (path,))

    def update_access_time(self, path: str):
        """Обновить время последнего доступа (для LRU)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE files SET last_access = ? WHERE path = ?",
                (int(time.time()), path)
            )

    def get_pinned_status(self, path: str) -> bool:
        """Вернуть True, если файл закреплён (pinned)"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT pinned FROM files WHERE path = ?",
                (path,)
            )
            row = cur.fetchone()
            return row is not None and row[0] == 1

    def set_pinned(self, path: str, pinned: bool):
        """Закрепить или открепить файл"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE files SET pinned = ? WHERE path = ?",
                (1 if pinned else 0, path)
            )

    def get_all_cached_paths(self) -> List[str]:
        """Вернуть список путей всех закэшированных файлов"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT path FROM files WHERE cached = 1 AND is_dir = 0"
            )
            return [row[0] for row in cur.fetchall()]

    def get_total_cached_size(self) -> int:
        """Вернуть суммарный размер всех закэшированных файлов в байтах"""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT SUM(size) FROM files WHERE cached = 1 AND is_dir = 0"
            )
            result = cur.fetchone()[0]
            return result if result is not None else 0

    def close(self):
        pass
