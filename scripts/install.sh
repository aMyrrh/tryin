#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

pip3 install -r requirements.txt
chmod +x main.py
sudo ln -sf "$PROJECT_DIR/main.py" /usr/local/bin/discohack

echo "Готово. Запуск: discohack"