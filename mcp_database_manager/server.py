import asyncio
import json
import sys
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

from .config import ConfigManager
from .db_manager import DatabaseManager


def _redact_connection_url(url: str) -> str:
    """Best-effort redaction of secrets in SQLAlchemy-style URLs.

    Prefers SQLAlchemy's URL renderer (hide_password=True). Falls back to returning
    the original string if parsing fails.
    """

    try:
        # SQLAlchemy dependency is already required by this project.
        from sqlalchemy.engine.url import make_url

        return make_url(url).render_as_string(hide_password=True)
    except Exception:
        return url

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def main():
    # Initialize managers
    config_manager = ConfigManager()
    db_manager = DatabaseManager(config_manager)

    app = Server("mcp-database-manager")

    @app.list_tools()
    async def list_tools() -> List[types.Tool]:
        return [
            types.Tool(
                name="list_connections",
                description=(
                    "List configured database connections and their permission (readonly)."
                    " Passwords are redacted from URLs in the output."
                    " Returns: JSON array of connection configs."
                ),
                inputSchema={
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "include_urls": {
                            "type": "boolean",
                            "description": "Whether to include (redacted) connection URLs in the output. Default: true.",
                        }
                    },
                    "required": ["include_urls"],
                    "additionalProperties": False,
                },
            ),
            types.Tool(
                name="get_schema",
                description=(
                    "获取数据库 schema（Markdown）。默认返回所有表的摘要；"
                    "传入 table_names 时返回这些表的列详情。"
                ),
                inputSchema={
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Connection name (matches config.yaml connections[].name)",
                        },
                        "table_names": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                            "description": "Optional list of table names. If omitted, returns a summary of all tables.",
                        },
                    },
                    "required": ["connection_name"],
                    "additionalProperties": False,
                },
            ),
            types.Tool(
                name="read_sql",
                description=(
                    "执行只读 SQL 查询（例如 SELECT/SHOW/PRAGMA）。"
                    " Returns: JSON array of rows."
                ),
                inputSchema={
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Connection name (matches config.yaml connections[].name)",
                        },
                        "query": {
                            "type": "string",
                            "minLength": 1,
                            "description": "SQL to execute (read-only)",
                        },
                    },
                    "required": ["connection_name", "query"],
                    "additionalProperties": False,
                },
            ),
            types.Tool(
                name="write_sql",
                description=(
                    "执行写入 SQL（INSERT/UPDATE/DELETE/DDL）。"
                    " 仅当连接 readonly=false 时允许。"
                    " Returns: JSON {status, rows_affected}."
                ),
                inputSchema={
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "properties": {
                        "connection_name": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Connection name (matches config.yaml connections[].name)",
                        },
                        "query": {
                            "type": "string",
                            "minLength": 1,
                            "description": "SQL to execute (write)",
                        },
                    },
                    "required": ["connection_name", "query"],
                    "additionalProperties": False,
                },
            ),
        ]

    @app.call_tool()
    async def call_tool(
        name: str, arguments: Any
    ) -> List[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        # Some MCP clients may send `arguments` as `None` or a non-dict type.
        # Normalize to a dict so `.get()` usage below is safe.
        if not isinstance(arguments, dict):
            arguments = {}

        if name == "list_connections":
            connections = config_manager.list_connections()
            include_urls = arguments.get("include_urls", True)

            safe_connections = []
            for c in connections:
                data = c.dict()

                if include_urls:
                    if isinstance(data.get("url"), str):
                        data["url"] = _redact_connection_url(data["url"])
                else:
                    data.pop("url", None)

                safe_connections.append(data)

            return [
                types.TextContent(
                    type="text",
                    text=json.dumps(safe_connections, indent=2),
                )
            ]

        elif name == "get_schema":
            connection_name = arguments.get("connection_name")
            table_names = arguments.get("table_names")
            if not connection_name:
                raise ValueError("connection_name is required")
            
            try:
                schema = db_manager.get_schema(connection_name, table_names)
                return [types.TextContent(type="text", text=schema)]
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

        elif name == "read_sql":
            connection_name = arguments.get("connection_name")
            query = arguments.get("query")
            if not connection_name or not query:
                raise ValueError("connection_name and query are required")

            try:
                results = db_manager.execute_read(connection_name, query)
                return [
                    types.TextContent(
                        type="text",
                        text=json.dumps(results, indent=2, default=str),
                    )
                ]
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

        elif name == "write_sql":
            connection_name = arguments.get("connection_name")
            query = arguments.get("query")
            if not connection_name or not query:
                raise ValueError("connection_name and query are required")

            try:
                result = db_manager.execute_write(connection_name, query)
                return [
                    types.TextContent(
                        type="text",
                        text=json.dumps(result, indent=2, default=str),
                    )
                ]
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

        else:
            raise ValueError(f"Unknown tool: {name}")

    async def run():
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options(),
            )

    asyncio.run(run())

if __name__ == "__main__":
    main()
