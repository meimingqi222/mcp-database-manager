import asyncio
import json
import sys
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

from .config import ConfigManager
from .db_manager import DatabaseManager

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
                description="List available database connections and their permission status.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="get_schema",
                description="Get the schema of a specific database. By default, returns a summary of all tables. Provide 'table_names' to get detailed column information for specific tables.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection_name": {"type": "string", "description": "Name of the database connection"},
                        "table_names": {
                            "type": "array", 
                            "items": {"type": "string"},
                            "description": "Optional list of table names to get detailed schema for. If omitted, returns a summary of all tables."
                        },
                    },
                    "required": ["connection_name"],
                },
            ),
            types.Tool(
                name="read_sql",
                description="Execute a read-only SQL query.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection_name": {"type": "string", "description": "Name of the database connection"},
                        "query": {"type": "string", "description": "SQL query to execute"},
                    },
                    "required": ["connection_name", "query"],
                },
            ),
            types.Tool(
                name="write_sql",
                description="Execute a write SQL query (INSERT, UPDATE, DELETE). Only works if connection is not read-only.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection_name": {"type": "string", "description": "Name of the database connection"},
                        "query": {"type": "string", "description": "SQL query to execute"},
                    },
                    "required": ["connection_name", "query"],
                },
            ),
        ]

    @app.call_tool()
    async def call_tool(
        name: str, arguments: Any
    ) -> List[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name == "list_connections":
            connections = config_manager.list_connections()
            return [
                types.TextContent(
                    type="text",
                    text=json.dumps([c.dict() for c in connections], indent=2),
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
