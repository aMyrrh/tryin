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

try:
    from gi.repository import AppIndicator3 as AppIndicator
    from gi.repository import Gtk
    from gi.repository import GLib
    APPINDICATOR_AVAILABLE = True
except ImportError:
    APPINDICATOR_AVAILABLE = False

from core.redisk_service import DISK_TITLES, RediskService

DISK_AUTH_URLS = {
    "yandex": "https://oauth.yandex.ru/authorize",
    "nextcloud": "https://nextcloud.com/sign-up/",
}


class TrayController:
    def __init__(self, service: RediskService):
        self.service = service
        self.notifications_enabled = True

    def show_notification(self, title: str, message: str):
        if self.notifications_enabled:
            self._show_notification_impl(title, message)

    def _show_notification_impl(self, title: str, message: str):
        raise NotImplementedError

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
            self._set_notifications_text("Отключить уведомления")
            self.show_notification("DiscoHack", "Уведомления включены")
        else:
            print("Уведомления отключены")
            self._set_notifications_text("Включить уведомления")

    def _set_notifications_text(self, text: str):
        raise NotImplementedError

    def rebuild_menu(self):
        raise NotImplementedError

    def quit_app(self):
        print("Программа закрыта")
        self.service.shutdown()
        self._quit_impl()

    def _quit_impl(self):
        raise NotImplementedError


class QTrayController(TrayController):
    def __init__(self, app: QApplication, tray_icon: QSystemTrayIcon, service: RediskService):
        super().__init__(service)
        self.app = app
        self.tray_icon = tray_icon
        self.menu = QMenu()
        self.notifications_action = QAction("Отключить уведомления")
        self.notifications_action.triggered.connect(self.toggle_notifications)

    def _show_notification_impl(self, title: str, message: str):
        self.tray_icon.showMessage(title, message)

    def _set_notifications_text(self, text: str):
        self.notifications_action.setText(text)

    def add_disconnect_menu(self):
        heading = QAction("Отключить диск")
        heading.setEnabled(False)
        self.menu.addAction(heading)

        connected_disks = self.service.get_connected_disks()
        if not connected_disks:
            action = QAction("Нет подключенных дисков")
            action.setEnabled(False)
            self.menu.addAction(action)
            return

        for disk_id in connected_disks:
            action = QAction(f"Отключить {DISK_TITLES[disk_id]}")
            action.triggered.connect(lambda _, d=disk_id: self.disconnect_disk(d))
            self.menu.addAction(action)

    def add_connect_menu(self):
        if self.menu.actions():
            self.menu.addSeparator()

        heading = QAction("Подключить диск")
        heading.setEnabled(False)
        self.menu.addAction(heading)

        connected_disks = set(self.service.get_connected_disks())
        available = [
            disk_id
            for disk_id in ("yandex", "nextcloud")
            if disk_id not in connected_disks
        ]

        if not available:
            action = QAction("Все диски уже подключены")
            action.setEnabled(False)
            self.menu.addAction(action)
            return

        for disk_id in available:
            action = QAction(f"Подключить {DISK_TITLES[disk_id]}")
            action.triggered.connect(lambda _, d=disk_id: self.connect_disk(d))
            self.menu.addAction(action)

    def rebuild_menu(self):
        self.menu.clear()
        self.add_disconnect_menu()
        self.add_connect_menu()

        open_action = QAction("Открыть Redisk")
        open_action.triggered.connect(self.open_redisk)
        self.menu.addAction(open_action)
        self.menu.addAction(self.notifications_action)

        self.menu.addSeparator()
        quit_action = QAction("Закрыть")
        quit_action.triggered.connect(self.quit_app)
        self.menu.addAction(quit_action)

        self.tray_icon.setContextMenu(self.menu)

    def _quit_impl(self):
        self.tray_icon.hide()
        self.app.quit()


class AppIndicatorTrayController(TrayController):
    def __init__(self, service: RediskService):
        super().__init__(service)
        self.icon_path = create_icon_path()
        self.indicator = AppIndicator.Indicator.new(
            "discohack",
            self.icon_path,
            AppIndicator.IndicatorCategory.APPLICATION_STATUS
        )
        self.indicator.set_status(AppIndicator.IndicatorStatus.ACTIVE)
        self.menu = Gtk.Menu()
        self.notifications_item = Gtk.MenuItem(label="Отключить уведомления")
        self.notifications_item.connect("activate", lambda w: self.toggle_notifications())

    def _show_notification_impl(self, title: str, message: str):
        # Для простоты, используем notify-send если доступен
        try:
            subprocess.run(["notify-send", title, message], check=True)
        except Exception:
            print(f"Уведомление: {title} - {message}")

    def _set_notifications_text(self, text: str):
        self.notifications_item.set_label(text)

    def add_disconnect_menu(self):
        heading = Gtk.MenuItem(label="Отключить диск")
        heading.set_sensitive(False)
        self.menu.append(heading)

        connected_disks = self.service.get_connected_disks()
        if not connected_disks:
            item = Gtk.MenuItem(label="Нет подключенных дисков")
            item.set_sensitive(False)
            self.menu.append(item)
            return

        for disk_id in connected_disks:
            item = Gtk.MenuItem(label=f"Отключить {DISK_TITLES[disk_id]}")
            item.connect("activate", lambda w, d=disk_id: self.disconnect_disk(d))
            self.menu.append(item)

    def add_connect_menu(self):
        separator = Gtk.SeparatorMenuItem()
        self.menu.append(separator)

        heading = Gtk.MenuItem(label="Подключить диск")
        heading.set_sensitive(False)
        self.menu.append(heading)

        connected_disks = set(self.service.get_connected_disks())
        available = [
            disk_id
            for disk_id in ("yandex", "nextcloud")
            if disk_id not in connected_disks
        ]

        if not available:
            item = Gtk.MenuItem(label="Все диски уже подключены")
            item.set_sensitive(False)
            self.menu.append(item)
            return

        for disk_id in available:
            item = Gtk.MenuItem(label=f"Подключить {DISK_TITLES[disk_id]}")
            item.connect("activate", lambda w, d=disk_id: self.connect_disk(d))
            self.menu.append(item)

    def rebuild_menu(self):
        # Создать новое меню
        self.menu = Gtk.Menu()

        self.add_disconnect_menu()
        self.add_connect_menu()

        separator = Gtk.SeparatorMenuItem()
        self.menu.append(separator)

        open_item = Gtk.MenuItem(label="Открыть Redisk")
        open_item.connect("activate", lambda w: self.open_redisk())
        self.menu.append(open_item)

        self.menu.append(self.notifications_item)

        separator2 = Gtk.SeparatorMenuItem()
        self.menu.append(separator2)

        quit_item = Gtk.MenuItem(label="Закрыть")
        quit_item.connect("activate", lambda w: self.quit_app())
        self.menu.append(quit_item)

        self.menu.show_all()
        self.indicator.set_menu(self.menu)

    def _quit_impl(self):
        Gtk.main_quit()


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
    if APPINDICATOR_AVAILABLE and sys.platform.startswith("linux"):
        # Используем AppIndicator для GNOME/Ubuntu
        controller = AppIndicatorTrayController(service)
        controller.rebuild_menu()
        controller.show_notification("DiscoHack", "Программа запущена")
        Gtk.main()
    else:
        # Fallback to QSystemTrayIcon
        app = QApplication(sys.argv)
        icon_path = create_icon_path()
        tray_icon = QSystemTrayIcon(QIcon(icon_path), parent=app)
        controller = QTrayController(app, tray_icon, service)
        controller.rebuild_menu()

        tray_icon.show()
        controller.show_notification("DiscoHack", "Программа запущена")

        sys.exit(app.exec())
