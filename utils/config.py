import os
import json
from pathlib import Path

def get_config_dir():
    config_dir = Path.home() / ".config" / "discohack"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir

def load_config():
    config_file = get_config_dir() / "config.json"
    if config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    return {}

def save_config(config):
    config_file = get_config_dir() / "config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)