class CacheError(Exception):
    """Базовое исключение для всех ошибок кэша"""
    pass


class FileNotCachedError(CacheError):
    """Файл не найден в кэше (нужно скачать из облака)"""
    pass


class CacheCorruptedError(CacheError):
    """Кэш повреждён (например, битая SQLite-база)"""
    pass


class DiskFullError(CacheError):
    """Недостаточно места на диске для кэширования"""
    pass


class StaleCacheError(CacheError):
    """Кэш устарел (файл изменился в облаке)"""
    pass


class PinnedFileError(CacheError):
    """Попытка удалить закреплённый (pinned) файл"""
    pass
