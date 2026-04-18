import os
import subprocess
import sys
import tempfile
import webbrowser

from PIL import Image, ImageDraw
from PyQt6.QtGui import QAction, QIcon
from PyQt6.QtWidgets import (
    QApplication,
    QInputDialog,
    QLineEdit,
    QMenu,
    QMessageBox,
    QSystemTrayIcon,
)

from core.redisk_service import DISK_TITLES, RediskService

DISK_AUTH_URLS = {
    "yandex": "https://oauth.yandex.ru/authorize",
    "nextcloud": "https://nextcloud.com/sign-up/",
}


class TrayController:
    def __init__(
        self,
        app: QApplication,
        tray_icon: QSystemTrayIcon,
        service: RediskService,
    ):
        self.app = app
        self.tray_icon = tray_icon
        self.service = service
        self.notifications_enabled = True
        self.menu = QMenu()
        self.notifications_action = QAction("Отключить уведомления")
        self.notifications_action.triggered.connect(self.toggle_notifications)

    def show_notification(self, title: str, message: str):
        if self.notifications_enabled:
            self.tray_icon.showMessage(title, message)

    def open_redisk(self):
        mount_dir = str(self.service.root_dir)
        os.makedirs(mount_dir, exist_ok=True)

        try:
            subprocess.Popen(["xdg-open", mount_dir])
        except Exception as exc:
            print(f"Не удалось открыть файловый менеджер: {exc}")
            self.show_notification(
                "DiscoHack",
                "Не удалось открыть файловый менеджер",
            )
            return

        print(f"Открыт Redisk: {mount_dir}")

    def connect_disk(self, disk_id: str):
        disk_title = DISK_TITLES[disk_id]
        auth_url = DISK_AUTH_URLS[disk_id]

        webbrowser.open(auth_url, new=2)
        print(f"Открыта авторизация: {disk_title}")
        if disk_id == "yandex":
            token, ok = QInputDialog.getText(
                None,
                "Подключение Яндекс.Диск",
                "Вставьте OAuth токен:",
            )
            if not ok or not token.strip():
                return
            is_connected = self.service.connect_yandex(token.strip())
        else:
            url, ok = QInputDialog.getText(
                None,
                "Подключение NextCloud",
                (
                    "URL WebDAV (например "
                    "https://.../remote.php/dav/files/<user>/):"
                ),
            )
            if not ok or not url.strip():
                return
            login, ok = QInputDialog.getText(
                None,
                "Подключение NextCloud",
                "Логин:",
            )
            if not ok or not login.strip():
                return
            password, ok = QInputDialog.getText(
                None,
                "Подключение NextCloud",
                "Пароль / App Password:",
                QLineEdit.EchoMode.Password,
            )
            if not ok or not password:
                return
            is_connected = self.service.connect_nextcloud(
                url=url.strip(),
                login=login.strip(),
                password=password,
            )

        if not is_connected:
            QMessageBox.critical(
                None,
                "DiscoHack",
                f"Не удалось подключить {disk_title}. Проверьте данные.",
            )
            return

        self.show_notification("DiscoHack", f"{disk_title} подключен")
        self.open_redisk()
        self.rebuild_menu()

    def disconnect_disk(self, disk_id: str):
        disk_title = DISK_TITLES[disk_id]
        self.service.disconnect_disk(disk_id)
        self.show_notification("DiscoHack", f"{disk_title} отключен")
        print(f"Отключен диск: {disk_title}")
        self.rebuild_menu()

    def toggle_notifications(self):
        self.notifications_enabled = not self.notifications_enabled
        if self.notifications_enabled:
            print("Уведомления включены")
            self.notifications_action.setText("Отключить уведомления")
            self.show_notification("DiscoHack", "Уведомления включены")
        else:
            print("Уведомления отключены")
            self.notifications_action.setText("Включить уведомления")

    def add_disconnect_menu(self):
        connected_disks = self.service.get_connected_disks()
        if not connected_disks:
            return

        if len(connected_disks) == 1:
            disk_id = connected_disks[0]
            action = QAction(f"Отключить {DISK_TITLES[disk_id]}")
            action.triggered.connect(lambda: self.disconnect_disk(disk_id))
            self.menu.addAction(action)
            return

        disconnect_menu = QMenu("Отключить диск")
        for disk_id in connected_disks:
            action = QAction(f"Отключить {DISK_TITLES[disk_id]}")
            action.triggered.connect(lambda _, d=disk_id: self.disconnect_disk(d))
            disconnect_menu.addAction(action)
        self.menu.addMenu(disconnect_menu)

    def add_add_disk_menu(self):
        add_menu = QMenu("Добавить диск")
        connected_disks = set(self.service.get_connected_disks())
        available = [
            disk_id
            for disk_id in ("yandex", "nextcloud")
            if disk_id not in connected_disks
        ]

        if not available:
            disabled_action = QAction("Все диски уже подключены")
            disabled_action.setEnabled(False)
            add_menu.addAction(disabled_action)
        else:
            for disk_id in available:
                action = QAction(DISK_TITLES[disk_id])
                action.triggered.connect(lambda _, d=disk_id: self.connect_disk(d))
                add_menu.addAction(action)

        self.menu.addMenu(add_menu)

    def rebuild_menu(self):
        self.menu.clear()
        self.add_disconnect_menu()
        self.add_add_disk_menu()

        open_action = QAction("Открыть Redisk")
        open_action.triggered.connect(self.open_redisk)
        self.menu.addAction(open_action)
        self.menu.addAction(self.notifications_action)

        self.menu.addSeparator()
        quit_action = QAction("Закрыть")
        quit_action.triggered.connect(self.quit_app)
        self.menu.addAction(quit_action)

    def quit_app(self):
        print("Программа закрыта")
        self.service.shutdown()
        self.tray_icon.hide()
        self.app.quit()


def create_icon_path():
    width = 64
    height = 64
    image = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(image)
    draw.ellipse((10, 20, 54, 60), fill=(100, 150, 255))

    temp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    image.save(temp_file.name)
    return temp_file.name


def run_tray(service: RediskService):
    app = QApplication(sys.argv)
    icon_path = create_icon_path()
    tray_icon = QSystemTrayIcon(QIcon(icon_path), parent=app)
    controller = TrayController(app=app, tray_icon=tray_icon, service=service)
    controller.rebuild_menu()

    tray_icon.setContextMenu(controller.menu)
    tray_icon.show()
    controller.show_notification("DiscoHack", "Программа запущена")

    sys.exit(app.exec())
