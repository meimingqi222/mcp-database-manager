# MCP Database Manager

An MCP server that enables LLM agents to perform CRUD operations across multiple databases.

## Features

- Multi-Database Support
- Permission Management (Read-only by default)
- SQLAlchemy Integration
- `list_connections` redacts passwords from connection URLs in its output

## Database Support

This MCP server supports multiple databases. You can enable specific database drivers using installation extras:

| Extra | Database | Drivers Installed |
|-------|----------|-------------------|
| `postgres` | PostgreSQL | `psycopg2-binary` |
| `mysql` | MySQL | `pymysql`, `cryptography` |
| `mssql` | SQL Server | `pymssql` |
| `all` | All of the above | All drivers |

## Configuration

The configuration file is located at:
- `~/.mcp-database-manager/config.yaml` (on all platforms)

Example `config.yaml`:

```yaml
connections:
  - name: "main_db"
    url: "sqlite:///./main.db"
    readonly: true
```

## Cursor Configuration

To use this MCP server in Cursor, add the following to your MCP settings (Settings > Features > MCP):

### Option 1: Local Development (Recommended for debugging)

Use this if you have the source code locally and want to test changes immediately.

```json
{
  "mcpServers": {
    "database-manager": {
      "command": "uv",
      "args": [
        "run",
        "--extra",
        "all",
        "mcp-database-manager"
      ],
      "cwd": "/absolute/path/to/mcp-database-manager"
    }
  }
}
```

> **Note:** Replace `/absolute/path/to/mcp-database-manager` with the actual path to your project directory.

### Option 2: Using PyPI (Published version)

Use this to run the stable version published on PyPI without cloning the repository.

```json
{
  "mcpServers": {
    "database-manager": {
      "command": "uvx",
      "args": [
        "--from",
        "mcp-database-manager[all]",
        "mcp-database-manager"
      ]
    }
  }
}
```

## Troubleshooting

If you see "Module not found" errors, ensure you are using `uv run` which handles the virtual environment automatically.
