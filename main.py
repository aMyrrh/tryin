from core.redisk_service import RediskService
from gui.tray import run_tray


def main():
    service = RediskService()
    run_tray(service)


if __name__ == "__main__":
    main()
