import os


from pathlib import Path


class CacheConfig:
    """Все настройки кэша в одном месте"""

    # Размер кэша по умолчанию (5 ГБ)
    DEFAULT_MAX_SIZE_MB = 5120

    # Путь к кэшу (относительно домашней директории)
    CACHE_DIR = "~/.cache/discohack"

    # Имя файла базы данных
    DB_FILENAME = "metadata.db"

    # Имя папки для содержимого файлов
    CONTENT_DIRNAME = "content"

    # Имя файла очереди синхронизации
    SYNC_QUEUE_FILENAME = "sync_queue.db"

    @classmethod
    def get_cache_dir(cls):
        """Вернуть абсолютный путь к папке кэша"""
        return Path(cls.CACHE_DIR).expanduser().resolve()

    @classmethod
    def get_db_path(cls):
        """Вернуть путь к файлу базы данных метаданных"""
        return cls.get_cache_dir() / cls.DB_FILENAME

    @classmethod
    def get_content_dir(cls):
        """Вернуть путь к папке с содержимым файлов"""
        return cls.get_cache_dir() / cls.CONTENT_DIRNAME

    @classmethod
    def get_sync_queue_path(cls):
        """Вернуть путь к базе очереди синхронизации"""
        return cls.get_cache_dir() / cls.SYNC_QUEUE_FILENAME
