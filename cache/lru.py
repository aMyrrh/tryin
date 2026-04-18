import time
import sqlite3
from typing import List, Tuple
from .metadata_db import MetadataDB


class LRU:
    def __init__(self, metadata_db: MetadataDB):
        self.db = metadata_db

    def get_eviction_candidates(self, needed_bytes: int) -> List[str]:
        """Найти файлы для удаления."""
        with sqlite3.connect(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("""
                SELECT path, size, last_access
                FROM files 
                WHERE cached = 1 AND pinned = 0 AND is_dir = 0
                ORDER BY last_access ASC
            """)

            candidates = []
            freed = 0

            for row in cur:
                candidates.append(row['path'])
                freed += row['size']
                if freed >= needed_bytes:
                    break

            return candidates

    def get_oldest_file(self) -> Tuple[str, int]:
        """Вернуть самый старый (по доступу) файл"""
        with sqlite3.connect(self.db.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("""
                SELECT path, last_access
                FROM files 
                WHERE cached = 1 AND pinned = 0 AND is_dir = 0
                ORDER BY last_access ASC
                LIMIT 1
            """)
            row = cur.fetchone()

        if row:
            return (row['path'], row['last_access'])
        return ("", 0)
