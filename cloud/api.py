import os
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yadisk  # type: ignore[import-not-found]
from webdav3.client import Client as WebDAVClient  # type: ignore[import-untyped]

from cloud.config import NEXTCLOUD_CONFIG, YANDEX_TOKEN


class CloudAPI:
    def __init__(
        self,
        config: dict | None = None,
        yandex_token: str | None = None,
        nextcloud_config: dict | None = None,
    ):
        self.yandex: Any | None = None
        self.nextcloud: Any | None = None
        self.config = config or {}
        config_provided = config is not None

        yandex_token = (
            yandex_token
            or self.config.get("disks", {}).get("yandex", {}).get("token")
            or (YANDEX_TOKEN if not config_provided else None)
        )
        if nextcloud_config is not None:
            nextcloud_cfg = nextcloud_config
        elif config_provided:
            nextcloud_cfg = self.config.get("disks", {}).get("nextcloud") or {}
        else:
            nextcloud_cfg = NEXTCLOUD_CONFIG or {}

        if yandex_token:
            self.connect_yandex(yandex_token)
        if nextcloud_cfg:
            self.connect_nextcloud(nextcloud_cfg)

    @staticmethod
    def _normalize_remote_path(path: str) -> str:
        normalized = "/" + path.strip("/")
        return normalized.replace("//", "/")

    @staticmethod
    def _ensure_parent(path: str) -> str:
        remote = CloudAPI._normalize_remote_path(path)
        parent = str(Path(remote).parent)
        if not parent.startswith("/"):
            parent = "/" + parent
        return parent

    def connect_yandex(self, token: str) -> bool:
        try:
            client = yadisk.YaDisk(token=token)
            if client.check_token():
                self.yandex = client
                return True
        except Exception as exc:
            print(f"Яндекс.Диск: ошибка - {exc}")
        self.yandex = None
        return False

    def connect_nextcloud(self, cfg: dict) -> bool:
        try:
            parsed = urlsplit(cfg["url"])
            hostname = f"{parsed.scheme}://{parsed.netloc}"
            client = WebDAVClient(
                {
                    "webdav_hostname": hostname,
                    "webdav_root": parsed.path or "/",
                    "webdav_login": cfg["login"],
                    "webdav_password": cfg["password"],
                }
            )
            client.list("/")
            self.nextcloud = client
            return True
        except Exception as exc:
            print(f"NextCloud: ошибка - {exc}")
        self.nextcloud = None
        return False

    def is_connected(self, disk_id: str) -> bool:
        if disk_id == "yandex":
            return self.yandex is not None
        if disk_id == "nextcloud":
            return self.nextcloud is not None
        return False

    def list_dir(self, disk_id: str, path: str = "/") -> list[dict]:
        remote = self._normalize_remote_path(path)

        if disk_id == "yandex" and self.yandex:
            result = []
            for item in self.yandex.listdir(remote):
                result.append(
                    {
                        "name": item.name,
                        "path": item.path.replace("disk:", "", 1),
                        "is_dir": bool(item.is_dir),
                    }
                )
            return result

        if disk_id == "nextcloud" and self.nextcloud:
            try:
                items = self.nextcloud.list(remote, get_info=True)
            except TypeError:
                items = self.nextcloud.list(remote)

            result = []
            for item in items:
                if isinstance(item, dict):
                    name = item.get("name", "").rstrip("/")
                    if not name or name in (".", ".."):
                        continue
                    item_path = f"{remote.rstrip('/')}/{name}".replace("//", "/")
                    result.append(
                        {
                            "name": name,
                            "path": item_path,
                            "is_dir": bool(item.get("isdir", False)),
                        }
                    )
                elif isinstance(item, str):
                    name = item.strip("/").split("/")[-1]
                    if not name or name in (".", ".."):
                        continue
                    is_dir = item.endswith("/")
                    item_path = f"{remote.rstrip('/')}/{name}".replace("//", "/")
                    result.append({"name": name, "path": item_path, "is_dir": is_dir})
            return result

        return []

    def download_file(self, disk_id: str, remote_path: str, local_path: str) -> bool:
        remote = self._normalize_remote_path(remote_path)
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            if disk_id == "yandex" and self.yandex:
                self.yandex.download(remote, local_path)
                return True
            if disk_id == "nextcloud" and self.nextcloud:
                self.nextcloud.download_file(remote, local_path)
                return True
        except Exception as exc:
            print(f"Ошибка скачивания {disk_id} {remote}: {exc}")
        return False

    def upload_file(self, disk_id: str, local_path: str, remote_path: str) -> bool:
        remote = self._normalize_remote_path(remote_path)
        try:
            self.create_folder(disk_id, self._ensure_parent(remote))
            if disk_id == "yandex" and self.yandex:
                self.yandex.upload(local_path, remote, overwrite=True)
                return True
            if disk_id == "nextcloud" and self.nextcloud:
                self.nextcloud.upload_file(local_path, remote)
                return True
        except Exception as exc:
            print(f"Ошибка загрузки {disk_id} {remote}: {exc}")
        return False

    def create_folder(self, disk_id: str, remote_path: str) -> bool:
        remote = self._normalize_remote_path(remote_path)
        if remote == "/":
            return True
        try:
            if disk_id == "yandex" and self.yandex:
                if not self.yandex.exists(remote):
                    self.yandex.mkdir(remote)
                return True
            if disk_id == "nextcloud" and self.nextcloud:
                if not self.nextcloud.check(remote):
                    self.nextcloud.mkdir(remote)
                return True
        except Exception as exc:
            print(f"Ошибка создания папки {disk_id} {remote}: {exc}")
        return False

    def delete_path(self, disk_id: str, remote_path: str) -> bool:
        remote = self._normalize_remote_path(remote_path)
        try:
            if disk_id == "yandex" and self.yandex:
                if self.yandex.exists(remote):
                    self.yandex.remove(remote, permanently=True)
                return True
            if disk_id == "nextcloud" and self.nextcloud:
                if self.nextcloud.check(remote):
                    self.nextcloud.clean(remote)
                return True
        except Exception as exc:
            print(f"Ошибка удаления {disk_id} {remote}: {exc}")
        return False

    def yandex_list_files(self, path: str) -> list[dict]:
        return self.list_dir("yandex", path)

    def nextcloud_list_files(self, path: str) -> list[dict]:
        return self.list_dir("nextcloud", path)

    def yandex_download(self, remote_path: str, local_path: str) -> bool:
        return self.download_file("yandex", remote_path, local_path)

    def nextcloud_download(self, remote_path: str, local_path: str) -> bool:
        return self.download_file("nextcloud", remote_path, local_path)

    def yandex_upload(self, local_path: str, remote_path: str) -> bool:
        return self.upload_file("yandex", local_path, remote_path)

    def nextcloud_upload(self, local_path: str, remote_path: str) -> bool:
        return self.upload_file("nextcloud", local_path, remote_path)

    def yandex_delete(self, remote_path: str) -> bool:
        return self.delete_path("yandex", remote_path)

    def nextcloud_delete(self, remote_path: str) -> bool:
        return self.delete_path("nextcloud", remote_path)

    def yandex_mkdir(self, remote_path: str) -> bool:
        return self.create_folder("yandex", remote_path)

    def nextcloud_mkdir(self, remote_path: str) -> bool:
        return self.create_folder("nextcloud", remote_path)

    def yandex_move(self, source_path: str, destination_path: str) -> bool:
        if not self.yandex:
            return False
        try:
            if hasattr(self.yandex, "move"):
                self.yandex.move(source_path, destination_path)
                return True
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                local_path = tmp.name
            if not self.download_file("yandex", source_path, local_path):
                os.unlink(local_path)
                return False
            if not self.upload_file("yandex", local_path, destination_path):
                os.unlink(local_path)
                return False
            os.unlink(local_path)
            return self.delete_path("yandex", source_path)
        except Exception as exc:
            print(f"Ошибка перемещения Яндекс.Диск {source_path} -> {destination_path}: {exc}")
        return False

    def nextcloud_move(self, source_path: str, destination_path: str) -> bool:
        if not self.nextcloud:
            return False
        try:
            if hasattr(self.nextcloud, "move"):
                self.nextcloud.move(source_path, destination_path)
                return True
            if hasattr(self.nextcloud, "move_file"):
                self.nextcloud.move_file(source_path, destination_path)
                return True
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                local_path = tmp.name
            if not self.download_file("nextcloud", source_path, local_path):
                os.unlink(local_path)
                return False
            if not self.upload_file("nextcloud", local_path, destination_path):
                os.unlink(local_path)
                return False
            os.unlink(local_path)
            return self.delete_path("nextcloud", source_path)
        except Exception as exc:
            print(f"Ошибка перемещения NextCloud {source_path} -> {destination_path}: {exc}")
        return False
