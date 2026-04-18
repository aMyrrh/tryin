import os
import shutil
from pathlib import Path

from watchdog.events import FileSystemEventHandler  # type: ignore[import-not-found]
from watchdog.observers import Observer  # type: ignore[import-not-found]

from cloud.api import CloudAPI
from utils.config import load_config, save_config


DISK_TITLES = {
    "yandex": "Яндекс.Диск",
    "nextcloud": "NextCloud",
}

TITLE_TO_DISK = {v: k for k, v in DISK_TITLES.items()}


class RediskService:
    def __init__(self):
        self.root_dir = Path.home() / "Redisk"
        self.root_dir.mkdir(parents=True, exist_ok=True)

        self.config = load_config()
        self.config.setdefault("disks", {})

        self.api = CloudAPI(config=self.config)
        self.events_paused = False

        self.observer = Observer()
        self.handler = RediskWatchHandler(self)
        self.observer.schedule(
            self.handler,
            str(self.root_dir),
            recursive=True,
        )
        self.observer.start()

        for disk_id in self.get_connected_disks():
            self.ensure_disk_local_dir(disk_id)
            self.pull_from_cloud(disk_id)

    def shutdown(self):
        self.observer.stop()
        self.observer.join(timeout=2)

    def get_connected_disks(self) -> list[str]:
        disks = self.config.get("disks", {})
        result = []
        for disk_id in ("yandex", "nextcloud"):
            if (
                disks.get(disk_id, {}).get("enabled")
                and self.api.is_connected(disk_id)
            ):
                result.append(disk_id)
        return result

    def ensure_disk_local_dir(self, disk_id: str):
        (self.root_dir / DISK_TITLES[disk_id]).mkdir(parents=True, exist_ok=True)

    def _save(self):
        save_config(self.config)

    def connect_yandex(self, token: str) -> bool:
        ok = self.api.connect_yandex(token)
        if not ok:
            return False
        self.config["disks"].setdefault("yandex", {})
        self.config["disks"]["yandex"]["token"] = token
        self.config["disks"]["yandex"]["enabled"] = True
        self._save()
        self.ensure_disk_local_dir("yandex")
        self.pull_from_cloud("yandex")
        return True

    def connect_nextcloud(self, url: str, login: str, password: str) -> bool:
        cfg = {"url": url, "login": login, "password": password}
        ok = self.api.connect_nextcloud(cfg)
        if not ok:
            return False
        self.config["disks"].setdefault("nextcloud", {})
        self.config["disks"]["nextcloud"].update(cfg)
        self.config["disks"]["nextcloud"]["enabled"] = True
        self._save()
        self.ensure_disk_local_dir("nextcloud")
        self.pull_from_cloud("nextcloud")
        return True

    def disconnect_disk(self, disk_id: str):
        self.config["disks"].setdefault(disk_id, {})
        self.config["disks"][disk_id]["enabled"] = False
        self._save()

        local_dir = self.root_dir / DISK_TITLES[disk_id]
        if local_dir.exists():
            self.events_paused = True
            try:
                shutil.rmtree(local_dir)
            finally:
                self.events_paused = False

    def pull_from_cloud(self, disk_id: str):
        if not self.api.is_connected(disk_id):
            return

        disk_root = self.root_dir / DISK_TITLES[disk_id]
        disk_root.mkdir(parents=True, exist_ok=True)

        self.events_paused = True
        try:
            self._sync_dir_from_cloud(disk_id, "/", disk_root)
        finally:
            self.events_paused = False

    def _sync_dir_from_cloud(
        self,
        disk_id: str,
        remote_path: str,
        local_dir: Path,
    ):
        for item in self.api.list_dir(disk_id, remote_path):
            name = item["name"]
            if not name:
                continue
            if item["is_dir"]:
                child_local = local_dir / name
                child_local.mkdir(parents=True, exist_ok=True)
                self._sync_dir_from_cloud(disk_id, item["path"], child_local)
            else:
                target_file = local_dir / name
                self.api.download_file(disk_id, item["path"], str(target_file))

    def _disk_and_relative_from_path(self, path: str):
        resolved = Path(path).resolve()
        try:
            relative = resolved.relative_to(self.root_dir.resolve())
        except ValueError:
            return None, None

        parts = relative.parts
        if not parts:
            return None, None
        disk_title = parts[0]
        disk_id = TITLE_TO_DISK.get(disk_title)
        if not disk_id:
            return None, None
        rel = Path(*parts[1:]) if len(parts) > 1 else Path(".")
        return disk_id, rel

    def handle_created_or_modified(self, path: str, is_dir: bool):
        disk_id, rel = self._disk_and_relative_from_path(path)
        if not disk_id or rel is None:
            return
        remote = "/" if str(rel) == "." else "/" + str(rel).replace("\\", "/")

        if is_dir:
            self.api.create_folder(disk_id, remote)
        else:
            if os.path.exists(path):
                self.api.upload_file(disk_id, path, remote)

    def handle_deleted(self, path: str):
        disk_id, rel = self._disk_and_relative_from_path(path)
        if not disk_id or rel is None:
            return
        if str(rel) == ".":
            return
        remote = "/" + str(rel).replace("\\", "/")
        self.api.delete_path(disk_id, remote)

    def handle_moved(self, src_path: str, dest_path: str, is_dir: bool):
        self.handle_deleted(src_path)
        self.handle_created_or_modified(dest_path, is_dir=is_dir)


class RediskWatchHandler(FileSystemEventHandler):
    def __init__(self, service: RediskService):
        self.service = service

    def on_created(self, event):
        if self.service.events_paused:
            return
        self.service.handle_created_or_modified(
            event.src_path,
            is_dir=event.is_directory,
        )

    def on_modified(self, event):
        if self.service.events_paused:
            return
        if event.is_directory:
            return
        self.service.handle_created_or_modified(event.src_path, is_dir=False)

    def on_deleted(self, event):
        if self.service.events_paused:
            return
        self.service.handle_deleted(event.src_path)

    def on_moved(self, event):
        if self.service.events_paused:
            return
        self.service.handle_moved(
            src_path=event.src_path,
            dest_path=event.dest_path,
            is_dir=event.is_directory,
        )
