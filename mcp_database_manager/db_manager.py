from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from .config import ConfigManager

class DatabaseManager:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self._engines: Dict[str, Engine] = {}

    def _get_engine(self, connection_name: str) -> Engine:
        if connection_name in self._engines:
            return self._engines[connection_name]

        conn_config = self.config_manager.get_connection(connection_name)
        if not conn_config:
            raise ValueError(f"Connection '{connection_name}' not found in configuration.")

        try:
            engine = create_engine(conn_config.url)
            self._engines[connection_name] = engine
            return engine
        except Exception as e:
            raise RuntimeError(f"Failed to create engine for '{connection_name}': {e}")

    def get_schema(self, connection_name: str) -> str:
        engine = self._get_engine(connection_name)
        inspector = inspect(engine)
        
        schema_md = f"# Schema for {connection_name}\n\n"
        
        for table_name in inspector.get_table_names():
            schema_md += f"## Table: {table_name}\n\n"
            columns = inspector.get_columns(table_name)
            if columns:
                schema_md += "| Column | Type | Nullable | Default |\n"
                schema_md += "|---|---|---|---|\n"
                for col in columns:
                    default_val = col.get('default', '')
                    if default_val is None:
                        default_val = 'NULL'
                    schema_md += f"| {col['name']} | {col['type']} | {col['nullable']} | {default_val} |\n"
            schema_md += "\n"
            
        return schema_md

    def execute_read(self, connection_name: str, query: str) -> List[Dict[str, Any]]:
        # Basic security check for read-only
        query_lower = query.strip().lower()
        forbidden_keywords = ['insert', 'update', 'delete', 'drop', 'alter', 'create', 'truncate', 'grant', 'revoke']
        if any(query_lower.startswith(kw) for kw in forbidden_keywords):
             raise ValueError("Write operations are not allowed in read_sql. Use write_sql instead.")

        engine = self._get_engine(connection_name)
        with engine.connect() as connection:
            # Use execution_options to try to enforce read-only if possible (DB dependent)
            # For now, we rely on the connection context and basic checks.
            result = connection.execute(text(query))
            return [dict(row._mapping) for row in result]

    def execute_write(self, connection_name: str, query: str) -> Dict[str, Any]:
        conn_config = self.config_manager.get_connection(connection_name)
        if not conn_config:
             raise ValueError(f"Connection '{connection_name}' not found.")
        
        if conn_config.readonly:
            raise PermissionError(f"Connection '{connection_name}' is configured as READ-ONLY.")

        engine = self._get_engine(connection_name)
        with engine.begin() as connection: # Use begin() for transaction
            result = connection.execute(text(query))
            return {
                "status": "success",
                "rows_affected": result.rowcount
            }
