import yaml
import os
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel
from platformdirs import user_config_dir

class ConnectionConfig(BaseModel):
    name: str
    url: str
    readonly: bool = True

class AppConfig(BaseModel):
    connections: List[ConnectionConfig] = []

class ConfigManager:
    APP_NAME = "mcp-database-manager"

    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir:
            self.config_dir = config_dir
        else:
            self.config_dir = Path.home() / ".mcp-database-manager"
        self.config_file = self.config_dir / "config.yaml"
        self._config: Optional[AppConfig] = None
        self._ensure_config_exists()

    def _ensure_config_exists(self):
        if not self.config_file.exists():
            self.config_dir.mkdir(parents=True, exist_ok=True)
            # Create default config with comments
            default_config_content = """connections:
  # Example SQLite connection
  # - name: "example_sqlite"
  #   url: "sqlite:///./example.db"
  #   readonly: true

  # Example PostgreSQL connection
  # - name: "local_postgres"
  #   url: "postgresql://user:password@localhost/dbname"
  #   readonly: true
"""
            with open(self.config_file, "w") as f:
                f.write(default_config_content)

    def load_config(self) -> AppConfig:
        self._ensure_config_exists()
        try:
            with open(self.config_file, "r") as f:
                data = yaml.safe_load(f) or {}
                self._config = AppConfig(**data)
        except Exception as e:
            print(f"Error loading config from {self.config_file}: {e}")
            # Return empty config or default on error
            self._config = AppConfig()
        return self._config

    def get_connection(self, name: str) -> Optional[ConnectionConfig]:
        if not self._config:
            self.load_config()
        
        for conn in self._config.connections:
            if conn.name == name:
                return conn
        return None

    def list_connections(self) -> List[ConnectionConfig]:
        if not self._config:
            self.load_config()
        return self._config.connections
