# cloudera-manager-mcp

MCP (Model Context Protocol) server for the Cloudera Manager REST API.
Designed for the multi-agent log analysis and operations platform running on
Cloudera Public Cloud / CDP.

The server exposes CM capabilities as MCP tools so that agents built in
**Cloudera Agent Studio** can extract logs, retrieve alerts, read and modify
configurations, restart services, and manage the CM registry — all through
a single, consistent interface.

---

## Table of contents

1. [Architecture](#architecture)
2. [Project structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Running locally](#running-locally)
7. [Deployment in Cloudera Agent Studio](#deployment-in-cloudera-agent-studio)
8. [Exposed tools](#exposed-tools)
9. [CM registry (Iceberg table)](#cm-registry-iceberg-table)
10. [Security notes](#security-notes)

---

## Architecture

```
Cloudera Agent Studio
        │
        │  MCP (stdio)
        ▼
┌─────────────────────────┐
│   cloudera-manager-mcp  │
│                         │
│  server.py              │  tool dispatch
│  ├── cm_pool.py         │  HTTP client pool (one per CM)
│  ├── cm_client.py       │  CM REST API calls (httpx + tenacity)
│  ├── cm_registry.py     │  Iceberg registry via Impala / HiveServer2
│  ├── config.py          │  settings from environment variables
│  └── models.py          │  Pydantic I/O models
└────────────┬────────────┘
             │  HTTPS / REST API
             ▼
   Cloudera Manager instances
   (one per DataHub environment)
             │
             │  reads / writes
             ▼
   Iceberg table — cdp_registry.cloudera_managers
   (Impala or HiveServer2)
```

CM instances are **not** configured in environment variables.
They are stored in an Iceberg table and loaded at startup.
The only variables in `.env` are the Impala / HiveServer2 connection
parameters needed to read that table.

---

## Project structure

```
cloudera-mcp-server/
├── pyproject.toml                       package metadata, dependencies, entry point
├── env.example                          environment variable template
├── agent_studio_mcp.json                Agent Studio MCP server configuration
├── requirements.txt                     pip-compatible dependency list
├── docs/
│   └── registry_table.sql               DDL for the Iceberg registry table
└── src/
    └── cloudera_manager_mcp/
        ├── __init__.py
        ├── server.py                    MCP server entry point, tool definitions and dispatch
        ├── cm_client.py                 async HTTP client for CM REST API
        ├── cm_pool.py                   multi-CM connection pool, cluster → host resolution
        ├── cm_registry.py               Iceberg registry (Impala / HiveServer2 via impyla)
        ├── config.py                    Pydantic settings (ImpalaSettings, ServerSettings)
        └── models.py                    Pydantic input / output models for every tool
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 or later |
| uv | 0.4 or later — [install](https://docs.astral.sh/uv/getting-started/installation/) |
| Cloudera Manager | 7.x (CDP 7.1.7+) |
| Impala or HiveServer2 | any version that supports Iceberg v2 |
| CM user role | Cluster Administrator or higher |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/dvergari/cloudera-mcp-server.git
cd cloudera-mcp-server

# Create the virtual environment and install all dependencies
uv sync

# Copy and edit the environment configuration
cp env.example .env
$EDITOR .env
```

`uv sync` reads `pyproject.toml`, resolves the dependency graph, creates
`.venv/` in the project root, and installs everything. No manual
`pip install` step is needed.

---

## Configuration

All configuration is done through environment variables, loaded from `.env`
at startup. See `.env.example` for the full reference with inline documentation.

### Variable groups

| Prefix | Purpose |
|---|---|
| `MCP_*` | Server behaviour — log level, concurrency limits, retry policy |
| `IMPALA_*` | SQL backend for the CM registry Iceberg table |

### SQL backend selection

The server supports two backends for reading the Iceberg registry table,
both accessed through **impyla**:

| `IMPALA_BACKEND` | Default port | Kerberos service |
|---|---|---|
| `impala` | 21050 | `impala` |
| `hiveserver2` | 10000 | `hive` |

Setting `IMPALA_BACKEND=hiveserver2` is sufficient to switch backends.
The port and Kerberos service name are resolved automatically from the
backend unless overridden explicitly.

### Minimal `.env` for development (Impala, no TLS)

```env
MCP_LOG_LEVEL=DEBUG
IMPALA_BACKEND=impala
IMPALA_HOST=impala-dev.example.com
IMPALA_AUTH_MECHANISM=PLAIN
IMPALA_USE_SSL=false
IMPALA_DATABASE=cdp_registry
IMPALA_TABLE=cloudera_managers
```

### Minimal `.env` for production (HiveServer2, LDAP, TLS)

```env
MCP_LOG_LEVEL=INFO
IMPALA_BACKEND=hiveserver2
IMPALA_HOST=hs2-prod.example.com
IMPALA_AUTH_MECHANISM=LDAP
IMPALA_USERNAME=svc-mcp-agent
IMPALA_PASSWORD=<password>
IMPALA_USE_SSL=true
IMPALA_DATABASE=cdp_registry
IMPALA_TABLE=cloudera_managers
IMPALA_REFRESH_INTERVAL_SECS=300
```

---

## Running locally

```bash
# Run directly with uv (recommended)
uv run cloudera-manager-mcp

# Or activate the virtualenv first
source .venv/bin/activate
cloudera-manager-mcp
```

The server starts and listens on **stdio** (standard input / output)

---

## Deployment in Cloudera Agent Studio

The `agent_studio_mcp.json` file contains ready-to-use configuration blocks
for three deployment modes. Copy the relevant block into Agent Studio →
**Settings → MCP Servers**.

### Mode A — local checkout (development)

Uses `uv --directory` to run the server in-place.
No build step required; uv manages the virtualenv automatically.

```json
{
  "command": "uv",
  "args": [
    "--directory", "/path/to/cloudera-mcp-server-en",
    "run", "cloudera-manager-mcp"
  ],
  "env": { "IMPALA_HOST": "...", "..." : "..." }
}
```

### Mode B — Git repository (staging / CI)

Uses `uvx --from git+https://...` to install and run in a single step.
Replace the URL with your internal GitLab or GitHub repository.

```json
{
  "command": "uvx",
  "args": [
    "--from", "git+https://github.com/your-org/cloudera-mcp-server.git",
    "cloudera-manager-mcp"
  ],
  "env": { "IMPALA_HOST": "...", "..." : "..." }
}
```

### Mode C — local wheel (air-gapped / production)

Build the wheel once, copy it to the Agent Studio host, then point `uvx` at it.
No network access required at runtime.

```bash
# Build
uv build
# → dist/cloudera_manager_mcp-1.0.0-py3-none-any.whl

# Copy to the Agent Studio host, then configure:
```

```json
{
  "command": "uvx",
  "args": [
    "--from", "/opt/mcp-wheels/cloudera_manager_mcp-1.0.0-py3-none-any.whl",
    "cloudera-manager-mcp"
  ],
  "env": { "IMPALA_HOST": "...", "..." : "..." }
}
```

---

## Exposed tools

### CM operational tools

| Tool | Description |
|---|---|
| `get_service_logs` | Extract logs from a service (YARN, HDFS, Solr, Spark, Hive…) with filters for host, role type, log level, and keyword |
| `get_alerts` | Fetch CM health alerts filtered by cluster, severity, service, and time range |
| `get_service_metrics` | Retrieve time-series metrics using CM metric names (cpu_percent, jvm_heap_used_mb…) |
| `get_config` | Read service or role-config-group configuration; `view=full` includes defaults |
| `update_config` | Modify a single CM configuration parameter; always returns `old_value` for verification |
| `run_service_command` | Start / stop / restart / rollingRestart a service — async, returns `command_id` |
| `get_command_status` | Poll the status of a running CM command by `command_id` |
| `get_host_status` | Health status of cluster hosts: health summary, resources, active roles |
| `get_audit_events` | CM audit log entries: logins, config changes, service operations |
| `list_datahubs` | Enumerate DataHub clusters with status, services, node count, cloud provider |
| `list_clusters` | List all clusters managed by every configured CM instance |
| `list_services` | List services in a specific cluster |
| `refresh_cluster_map` | Rebuild the internal cluster → CM host lookup table |

### Registry management tools

| Tool | Description |
|---|---|
| `registry_list` | List all CM instances stored in the Iceberg table (passwords excluded) |
| `registry_stats` | Aggregate statistics: total CMs, active count, environments, cloud providers |
| `registry_add` | Register a new CM; immediately adds it to the connection pool |
| `registry_deactivate` | Soft-delete a CM (`active=FALSE`); row is preserved for audit history |
| `registry_update_field` | Update a single field (port, api_version, timeout…) for a given CM host |
| `registry_reload` | Force a registry cache reload from Iceberg and refresh the connection pool |

---

## CM registry (Iceberg table)

CM instances are stored in the Iceberg table `cdp_registry.cloudera_managers`.
The full DDL, sample data, and common update operations are in
`docs/registry_table.sql`.

To create the table and insert the first records:

```bash
impala-shell -f docs/registry_table.sql
```

The server reads the table at startup and caches the results in memory.
The cache is refreshed every `IMPALA_REFRESH_INTERVAL_SECS` seconds (default 300).
To force an immediate reload without restarting the server, call the
`registry_reload` tool.

---

## Security notes

- **CM credentials** are stored only in the Iceberg registry table, never in
  environment variables or config files.
- **Passwords** in the registry table are currently stored in plaintext.
  For production deployments, integrate `cryptography.Fernet` or a cloud
  secrets manager (AWS Secrets Manager, Azure Key Vault, GCP Secret Manager)
  at the `cm_registry.load()` call site.
- **Tool output** never includes CM passwords. The `registry_list` tool
  explicitly excludes the `password` column from its SELECT statement.
- **Sensitive CM config parameters** (`sensitive=true`) are passed through
  without being logged by the server.
- **Destructive tools** (`update_config`, `run_service_command`,
  `registry_deactivate`) should be gated by an agent-level confirmation step
  before the tool is actually called. This is enforced at the agent prompt
  level, not at the server level.
- Use a **dedicated CM service account** (`svc-mcp-agent`) with the minimum
  required permissions rather than the `admin` user.
