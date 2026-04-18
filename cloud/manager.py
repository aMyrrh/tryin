from urllib.parse import urlparse

from .api import CloudAPI
from ..utils.config import load_config, save_config

SERVICE_LABELS = {
    'yandex': 'Яндекс.Диск',
    'nextcloud': 'Next Cloud'
}
SERVICE_AUTH_URLS = {
    'yandex': 'https://passport.yandex.ru/auth',
    'nextcloud': 'https://demo.nextcloud.com/index.php/login'
}


class CloudManager:
    def __init__(self):
        self.config = load_config()
        self.notifications_enabled = self.config.get('notifications_enabled', True)
        self.services = self.config.get('services', {})
        self.api_cache = {}
        self._validate_services()

    def _validate_services(self):
        for service_key in list(self.services.keys()):
            config = self.services.get(service_key)
            if not self._create_api(service_key, config):
                self.services.pop(service_key, None)
        self.save()

    def save(self):
        self.config['notifications_enabled'] = self.notifications_enabled
        self.config['services'] = self.services
        save_config(self.config)

    def get_service_label(self, service_key):
        return SERVICE_LABELS.get(service_key, service_key)

    def get_service_key_by_label(self, label):
        for key, value in SERVICE_LABELS.items():
            if value == label:
                return key
        return None

    def get_auth_url(self, service_key):
        return SERVICE_AUTH_URLS.get(service_key)

    def get_connected_services(self):
        return [service_key for service_key in self.services.keys() if self.get_api(service_key) is not None]

    def get_available_services(self):
        return [service_key for service_key in SERVICE_LABELS if service_key not in self.get_connected_services()]

    def get_service_config(self, service_key):
        return self.services.get(service_key, {})

    def has_service(self, service_key):
        return service_key in self.get_connected_services()

    def connect_yandex(self, token):
        if not token:
            return False
        self.services['yandex'] = {'token': token}
        if self._create_api('yandex', self.services['yandex']):
            self.save()
            return True
        self.services.pop('yandex', None)
        return False

    def connect_nextcloud(self, url, login, password):
        if not url or not login or not password:
            return False
        normalized_url = self._normalize_nextcloud_url(url)
        self.services['nextcloud'] = {
            'url': normalized_url,
            'login': login,
            'password': password
        }
        if self._create_api('nextcloud', self.services['nextcloud']):
            self.save()
            return True
        self.services.pop('nextcloud', None)
        return False

    def disconnect_service(self, service_key):
        self.services.pop(service_key, None)
        self.api_cache.pop(service_key, None)
        self.save()

    def _create_api(self, service_key, config):
        if not config:
            return False
        api = None
        if service_key == 'yandex':
            api = CloudAPI(yandex_token=config.get('token'))
            if api.yandex:
                self.api_cache[service_key] = api
                return True
            return False

        if service_key == 'nextcloud':
            api = CloudAPI(nextcloud_config=config)
            if api.nextcloud:
                self.api_cache[service_key] = api
                return True
            return False

        return False

    def get_api(self, service_key):
        if service_key in self.api_cache:
            return self.api_cache[service_key]

        config = self.services.get(service_key)
        if not config:
            return None

        if self._create_api(service_key, config):
            return self.api_cache.get(service_key)
        return None

    def list_files(self, service_key, path='/'):
        api = self.get_api(service_key)
        if not api:
            return []
        if service_key == 'yandex':
            return api.yandex_list_files(path)
        if service_key == 'nextcloud':
            return api.nextcloud_list_files(path)
        return []

    def download_file(self, service_key, cloud_path, local_path):
        api = self.get_api(service_key)
        if not api:
            return False
        if service_key == 'yandex':
            return api.yandex_download(cloud_path, local_path)
        if service_key == 'nextcloud':
            return api.nextcloud_download(cloud_path, local_path)
        return False

    def upload_file(self, service_key, local_path, cloud_path):
        api = self.get_api(service_key)
        if not api:
            return False
        if service_key == 'yandex':
            return api.yandex_upload(local_path, cloud_path)
        if service_key == 'nextcloud':
            return api.nextcloud_upload(local_path, cloud_path)
        return False

    def delete_file(self, service_key, cloud_path):
        api = self.get_api(service_key)
        if not api:
            return False
        if service_key == 'yandex':
            return api.yandex_delete(cloud_path)
        if service_key == 'nextcloud':
            return api.nextcloud_delete(cloud_path)
        return False

    def make_directory(self, service_key, path):
        api = self.get_api(service_key)
        if not api:
            return False
        if service_key == 'yandex':
            return api.yandex_mkdir(path)
        if service_key == 'nextcloud':
            return api.nextcloud_mkdir(path)
        return False

    def move_file(self, service_key, source_path, destination_path):
        api = self.get_api(service_key)
        if not api:
            return False
        if service_key == 'yandex':
            return api.yandex_move(source_path, destination_path)
        if service_key == 'nextcloud':
            return api.nextcloud_move(source_path, destination_path)
        return False

    def _normalize_nextcloud_url(self, url):
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url
        return 'https://' + url
