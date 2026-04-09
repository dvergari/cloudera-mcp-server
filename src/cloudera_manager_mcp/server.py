"""
server.py — Cloudera Manager MCP Server built with FastMCP.

FastMCP generates JSON Schema, validates inputs, and handles the MCP
protocol lifecycle automatically. Each tool is a plain async function
decorated with @mcp.tool().

Exposed tools:

  CM operational tools:
    get_service_logs      — extract logs from a service with filters
    get_alerts            — fetch CM health alerts
    get_service_metrics   — retrieve time-series metrics
    get_config            — read service / role-config-group configuration
    update_config         — modify a single configuration parameter
    run_service_command   — start / stop / restart a service (async)
    get_command_status    — poll an async CM command
    get_host_status       — health and role status for cluster hosts
    get_audit_events      — CM audit log entries
    list_datahubs         — enumerate DataHub clusters
    list_clusters         — list all managed clusters
    list_services         — list services in a cluster
    refresh_cluster_map   — rebuild the internal cluster -> CM host map

  Registry management tools (Iceberg table):
    registry_list         — list all registered CM instances
    registry_stats        — aggregate registry statistics
    registry_add          — add a new CM to the registry
    registry_deactivate   — soft-delete a CM
    registry_update_field — update a single field for a CM
    registry_reload       — force a registry cache reload
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Literal, Optional

import structlog
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from config import ImpalaSettings, ServerSettings
from cm_registry import CMRegistry
from cm_pool import CMPool

import sys


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(colors=False),
    ],
    logger_factory = structlog.PrintLoggerFactory(file=sys.stderr),
)
log = structlog.get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN — initialise registry and pool, tear them down on exit
# ─────────────────────────────────────────────────────────────────────────────

server_cfg = ServerSettings()

# Module-level references populated inside lifespan and used by tool functions.
_registry: Optional[CMRegistry] = None
_pool:     Optional[CMPool]     = None


@asynccontextmanager
async def lifespan(app: FastMCP):
    """Start registry + CM pool before serving; stop them on shutdown."""
    global _registry, _pool

    load_dotenv()

    log.info("server.starting", name=server_cfg.server_name,
             version=server_cfg.server_version)

    impala_cfg = ImpalaSettings()
    _registry  = CMRegistry(impala_cfg)
    _registry.start()

    instances = _registry.get_all()
    if not instances:
        log.warning(
            "server.no_cm_instances",
            table=impala_cfg.full_table_name,
            hint="Register CMs via registry_add or insert rows in the Iceberg table.",
        )

    _pool = CMPool(instances, server_cfg)
    await _pool.start()

    log.info("server.ready", cm_count=len(instances))

    try:
        yield
    finally:
        await _pool.stop()
        _registry.stop()
        log.info("server.stopped")


# ─────────────────────────────────────────────────────────────────────────────
# FASTMCP SERVER INSTANCE
# ─────────────────────────────────────────────────────────────────────────────

mcp = FastMCP(server_cfg.server_name, lifespan=lifespan)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _dump(obj) -> str:
    """Serialise a result to indented JSON string."""
    return json.dumps(obj, indent=2, default=str)


# ─────────────────────────────────────────────────────────────────────────────
# CM OPERATIONAL TOOLS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_service_logs(
    cluster_name:   str,
    service_name:   str,
    start_time:     str,
    end_time:       Optional[str] = None,
    host_filter:    Optional[str] = None,
    role_filter:    Optional[str] = None,
    level_filter:   Optional[Literal["FATAL","ERROR","WARN","INFO","DEBUG","TRACE"]] = None,
    keyword_filter: Optional[str] = None,
    max_lines:      int = 1000,
) -> str:
    """
    Extract logs from a Cloudera service (YARN, HDFS, Solr, Spark, Hive...)
    on a specified DataHub cluster. Returns entries in chronological order.

    Args:
        cluster_name:   DataHub cluster name.
        service_name:   CM service name (e.g. YARN, HDFS, SOLR).
        start_time:     ISO 8601 start time (e.g. 2024-11-01T00:00:00Z).
        end_time:       ISO 8601 end time. Defaults to now.
        host_filter:    Substring match on hostname.
        role_filter:    Role type filter (e.g. NODEMANAGER, NAMENODE).
        level_filter:   Minimum log level.
        keyword_filter: Keyword to search in log messages.
        max_lines:      Maximum number of log lines to return (1-5000).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    result = await client.get_service_logs(
        cluster_name=   cluster_name,
        service_name=   service_name,
        start_time=     start_time,
        end_time=       end_time,
        host_filter=    host_filter,
        role_filter=    role_filter,
        level_filter=   level_filter,
        keyword_filter= keyword_filter,
        max_lines=      min(max_lines, server_cfg.max_log_lines),
        max_hours=      server_cfg.max_log_range_hours,
    )
    return _dump(result)


@mcp.tool()
async def get_alerts(
    cluster_name:   Optional[str] = None,
    severity:       Literal["CRITICAL","WARNING","INFORMATIONAL","ALL"] = "ALL",
    start_time:     Optional[str] = None,
    end_time:       Optional[str] = None,
    service_filter: Optional[str] = None,
    max_results:    int = 100,
) -> str:
    """
    Fetch CM health alerts, optionally filtered by cluster, severity,
    service, and time range. Aggregates results from all CM instances
    when no cluster is specified.

    Args:
        cluster_name:   Cluster name. Omit to query all clusters.
        severity:       Minimum severity: CRITICAL, WARNING, INFORMATIONAL, or ALL.
        start_time:     ISO 8601 start time.
        end_time:       ISO 8601 end time. Defaults to now.
        service_filter: Filter by service name.
        max_results:    Maximum number of alerts to return (1-1000).
    """
    clients = (
        [_pool.get_client_for_cluster(cluster_name)]
        if cluster_name else _pool.all_clients()
    )
    all_alerts: list[dict] = []
    for client in clients:
        res = await client.get_alerts(
            cluster_name=   cluster_name,
            severity=       severity,
            start_time=     start_time,
            end_time=       end_time,
            service_filter= service_filter,
            max_results=    max_results,
        )
        all_alerts.extend(res["alerts"])

    all_alerts.sort(key=lambda a: a.get("timestamp", ""), reverse=True)
    return _dump({"total": len(all_alerts), "alerts": all_alerts})


@mcp.tool()
async def get_service_metrics(
    cluster_name: str,
    service_name: str,
    metric_names: list[str],
    start_time:   str,
    end_time:     Optional[str] = None,
    roll_up:      Literal["RAW","TEN_MINUTELY","HOURLY","DAILY"] = "TEN_MINUTELY",
) -> str:
    """
    Retrieve time-series metrics for a Cloudera service using CM native
    metric names (e.g. cpu_percent, jvm_heap_used_mb, yarn_containers_running).

    Args:
        cluster_name: DataHub cluster name.
        service_name: CM service name.
        metric_names: List of CM metric names.
        start_time:   ISO 8601 start time.
        end_time:     ISO 8601 end time. Defaults to now.
        roll_up:      Time-series granularity.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    result = await client.get_service_metrics(
        cluster_name= cluster_name,
        service_name= service_name,
        metric_names= metric_names,
        start_time=   start_time,
        end_time=     end_time,
        roll_up=      roll_up,
    )
    return _dump(result)


@mcp.tool()
async def get_config(
    cluster_name:      str,
    service_name:      str,
    role_config_group: Optional[str] = None,
    view:              Literal["summary","full"] = "full",
) -> str:
    """
    Read the configuration of a CM service or role config group.
    view='full' returns all parameters including defaults;
    view='summary' returns only explicitly overridden parameters.

    Args:
        cluster_name:      DataHub cluster name.
        service_name:      CM service name.
        role_config_group: Role config group (e.g. yarn-NODEMANAGER-BASE).
                           Omit for service-level config.
        view:              'full' or 'summary'.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    result = await client.get_config(
        cluster_name=      cluster_name,
        service_name=      service_name,
        role_config_group= role_config_group,
        view=              view,
    )
    return _dump(result)


@mcp.tool()
async def update_config(
    cluster_name:      str,
    service_name:      str,
    config_key:        str,
    config_value:      str,
    role_config_group: Optional[str] = None,
    message:           Optional[str] = None,
) -> str:
    """
    Modify a single CM configuration parameter. Always returns old_value
    alongside new_value for verification. Does NOT automatically restart
    the service — call run_service_command afterwards if needed.

    WARNING: this operation can affect cluster stability.

    Args:
        cluster_name:      DataHub cluster name.
        service_name:      CM service name.
        config_key:        Exact CM parameter name.
        config_value:      New value (always as string).
        role_config_group: Target role config group. Omit for service-level.
        message:           Audit note for the change.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    result = await client.update_config(
        cluster_name=      cluster_name,
        service_name=      service_name,
        config_key=        config_key,
        config_value=      config_value,
        role_config_group= role_config_group,
        message=           message,
    )
    return _dump(result)


@mcp.tool()
async def run_service_command(
    cluster_name:     str,
    service_name:     str,
    command:          Literal["start","stop","restart","rollingRestart","decommission"],
    slave_batch_size: Optional[int] = None,
    slave_fail_count: Optional[int] = None,
) -> str:
    """
    Execute a command on a CM service: start, stop, restart, rollingRestart.
    The command is asynchronous — returns a command_id to monitor with
    get_command_status. Use rollingRestart for zero-downtime restarts on
    services with replication.

    Args:
        cluster_name:     DataHub cluster name.
        service_name:     CM service name.
        command:          Command to execute.
        slave_batch_size: rollingRestart only — hosts per batch.
        slave_fail_count: rollingRestart only — maximum tolerated failures.
    """
    client     = _pool.get_client_for_cluster(cluster_name)
    extra_args: dict = {}
    if command == "rollingRestart":
        if slave_batch_size:
            extra_args["slaveBatchSize"] = slave_batch_size
        if slave_fail_count:
            extra_args["slaveFailCount"] = slave_fail_count

    result = await client.run_service_command(
        cluster_name= cluster_name,
        service_name= service_name,
        command=      command,
        extra_args=   extra_args or None,
    )
    return _dump(result)


@mcp.tool()
async def get_command_status(command_id: int, cluster_name: str) -> str:
    """
    Poll the status of a running or completed CM command.
    active=false indicates the command has finished — check 'success' for outcome.

    Args:
        command_id:   Command ID returned by run_service_command.
        cluster_name: Cluster the command was issued against.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    result = await client.get_command_status(command_id)
    return _dump(result)


@mcp.tool()
async def get_host_status(
    cluster_name:  Optional[str] = None,
    host_filter:   Optional[str] = None,
    include_roles: bool = True,
) -> str:
    """
    Return the health status of cluster hosts: healthSummary, commission
    state, maintenance mode, resources (CPU/RAM), and active roles per node.

    Args:
        cluster_name:  Filter by cluster. Omit for all hosts.
        host_filter:   Substring match on hostname.
        include_roles: Include active role list per host.
    """
    pairs = (
        [(_pool.get_client_for_cluster(cluster_name), cluster_name)]
        if cluster_name else [(c, None) for c in _pool.all_clients()]
    )
    all_hosts: list[dict] = []
    for client, cn in pairs:
        res = await client.get_host_status(
            cluster_name=  cn,
            host_filter=   host_filter,
            include_roles= include_roles,
        )
        all_hosts.extend(res["hosts"])

    return _dump({"total_hosts": len(all_hosts), "hosts": all_hosts})


@mcp.tool()
async def get_audit_events(
    start_time:       str,
    cluster_name:     Optional[str]  = None,
    end_time:         Optional[str]  = None,
    service_filter:   Optional[str]  = None,
    user_filter:      Optional[str]  = None,
    operation_filter: Optional[str]  = None,
    allowed_filter:   Optional[bool] = None,
    max_results:      int = 200,
) -> str:
    """
    Fetch CM audit log entries: logins, configuration changes, service
    commands, HDFS/Ranger/Knox operations.

    Args:
        start_time:       ISO 8601 start time.
        cluster_name:     Filter by cluster. Omit for all clusters.
        end_time:         ISO 8601 end time. Defaults to now.
        service_filter:   Filter by service (e.g. HDFS, YARN).
        user_filter:      CM username to filter by.
        operation_filter: Operation type (e.g. LOGIN, CONFIG_CHANGE, RESTART).
        allowed_filter:   True=allowed only, False=denied only, None=both.
        max_results:      Maximum number of entries to return (1-2000).
    """
    clients = (
        [_pool.get_client_for_cluster(cluster_name)]
        if cluster_name else _pool.all_clients()
    )
    all_entries: list[dict] = []
    for client in clients:
        res = await client.get_audit_events(
            cluster_name=     cluster_name,
            start_time=       start_time,
            end_time=         end_time,
            service_filter=   service_filter,
            user_filter=      user_filter,
            operation_filter= operation_filter,
            allowed_filter=   allowed_filter,
            max_results=      max_results,
        )
        all_entries.extend(res["entries"])

    all_entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    return _dump({"total": len(all_entries), "entries": all_entries})


@mcp.tool()
async def list_datahubs(
    environment_name: Optional[str] = None,
    status_filter:    Literal["AVAILABLE","STOPPED","FAILED","ALL"] = "ALL",
) -> str:
    """
    List all DataHub clusters in the configured CDP environments, with
    status, installed services, node count, cloud provider, and region.
    Use this first to discover available clusters before other tool calls.

    Args:
        environment_name: Filter by CDP environment name. Omit for all.
        status_filter:    Filter by DataHub status.
    """
    all_dh: list[dict] = []
    for client in _pool.all_clients():
        res = await client.list_datahubs(
            environment_name= environment_name,
            status_filter=    status_filter,
        )
        all_dh.extend(res["datahubs"])
    return _dump({"total": len(all_dh), "datahubs": all_dh})


@mcp.tool()
async def list_clusters() -> str:
    """
    List all clusters managed by every configured CM instance,
    with status, services, and the CM host that manages each cluster.
    """
    all_clusters: list[dict] = []
    for client in _pool.all_clients():
        clusters = await client.list_clusters()
        for c in clusters:
            c["_cm_host"] = client.cfg.effective_host
        all_clusters.extend(clusters)
    return _dump({"total": len(all_clusters), "clusters": all_clusters})


@mcp.tool()
async def list_services(cluster_name: str) -> str:
    """
    List the services in a specific cluster with name, type, and state.

    Args:
        cluster_name: DataHub cluster name.
    """
    client   = _pool.get_client_for_cluster(cluster_name)
    services = await client.list_services(cluster_name)
    return _dump({"cluster": cluster_name, "total": len(services), "services": services})


@mcp.tool()
async def refresh_cluster_map() -> str:
    """
    Rebuild the internal cluster -> CM host lookup table.
    Call this after provisioning new DataHubs or modifying the CDP environment.
    """
    await _pool.refresh_cluster_map()
    return _dump({"message": "Cluster map refreshed successfully."})


# ─────────────────────────────────────────────────────────────────────────────
# REGISTRY MANAGEMENT TOOLS
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
async def registry_list(include_inactive: bool = False) -> str:
    """
    List all Cloudera Manager instances stored in the Iceberg registry table.
    Shows host, environment, API version, cloud provider, and active status.
    Passwords are never included in the output.

    Args:
        include_inactive: Include deactivated CM entries.
    """
    rows = await _registry.async_list_raw(include_inactive=include_inactive)
    return _dump({"total": len(rows), "cloudera_managers": rows})


@mcp.tool()
async def registry_stats() -> str:
    """
    Return aggregate statistics about the CM registry: total count,
    active count, distinct environments, cloud providers covered,
    and last cache refresh timestamp.
    """
    stats = await _registry.async_get_stats()
    return _dump(stats)


@mcp.tool()
async def registry_add(
    host:             str,
    environment_name: str,
    username:         str,
    password:         str,
    port:             int = 7183,
    use_tls:          bool = True,
    verify_ssl:       bool = True,
    api_version:      str = "v51",
    timeout_seconds:  int = 30,
    cloud_provider:   Optional[Literal["AWS","AZURE","GCP"]] = None,
    region:           Optional[str] = None,
    description:      Optional[str] = None,
    tags:             Optional[str] = None,
    use_knox:         bool = False,
    lb_host:          Optional[str] = None,
    lb_port:          int = 443,
    cluster_name:     Optional[str] = None,
) -> str:
    """
    Register a new Cloudera Manager in the Iceberg table. The CM is
    immediately added to the connection pool.
    Set use_knox=true to route CM API calls through the CDP load balancer.
    Knox URL: https://<lb_host>:<lb_port>/<cluster_name>/cdp-proxy-api/cm-api/<version>/

    Args:
        host:             CM hostname (used for direct connections).
        environment_name: CDP environment name.
        username:         CM username.
        password:         CM password.
        port:             CM API port (default 7183).
        use_tls:          Use HTTPS for direct connections.
        verify_ssl:       Verify TLS certificate.
        api_version:      CM API version (default v51).
        timeout_seconds:  HTTP timeout in seconds.
        cloud_provider:   AWS, AZURE, or GCP.
        region:           Cloud region.
        description:      Free-text notes.
        tags:             Free-form JSON tags string.
        use_knox:         Route CM calls through CDP load balancer.
        lb_host:          CDP load balancer hostname (required when use_knox=true).
        lb_port:          CDP load balancer HTTPS port (default 443).
        cluster_name:     CDP DataHub cluster name (required when use_knox=true).
    """
    cm_id = await _registry.async_register(
        host=             host,
        environment_name= environment_name,
        username=         username,
        password=         password,
        port=             port,
        use_tls=          use_tls,
        verify_ssl=       verify_ssl,
        api_version=      api_version,
        timeout_seconds=  timeout_seconds,
        cloud_provider=   cloud_provider,
        region=           region,
        description=      description,
        tags=             tags,
        use_knox=         use_knox,
        lb_host=          lb_host,
        lb_port=          lb_port,
        cluster_name=     cluster_name,
    )
    await _pool.reload(_registry.get_all())
    return _dump({"cm_id": cm_id, "host": host,
                  "message": "CM registered and added to the connection pool."})


@mcp.tool()
async def registry_deactivate(host: str) -> str:
    """
    Soft-delete a CM by setting active=FALSE. The row is preserved for
    audit history. The CM is removed from the active connection pool.

    Args:
        host: Hostname of the CM to deactivate.
    """
    await _registry.async_deactivate(host)
    await _pool.reload(_registry.get_all())
    return _dump({"host": host,
                  "message": "CM deactivated and removed from the connection pool."})


@mcp.tool()
async def registry_update_field(host: str, field: str, value: str) -> str:
    """
    Update a single field of a CM entry in the Iceberg registry.
    Updatable fields: port, username, password, use_tls, verify_ssl,
    api_version, timeout_seconds, description, cloud_provider, region,
    tags, use_knox, lb_host, lb_port, cluster_name.

    Args:
        host:  CM hostname.
        field: Field name to update.
        value: New value as string (type coercion applied automatically).
    """
    await _registry.async_update_field(host, field, value)
    await _pool.reload(_registry.get_all())
    return _dump({"host": host, "field": field, "value": value,
                  "message": "Field updated. Connection pool reloaded."})


@mcp.tool()
async def registry_reload() -> str:
    """
    Force a reload of the CM registry cache from the Iceberg table and
    refresh the connection pool. Use after manual table edits or after
    registry_add / registry_deactivate without waiting for the automatic
    refresh cycle.
    """
    instances = await _registry.async_load()
    await _pool.reload(instances)
    return _dump({"message": f"Registry reloaded: {len(instances)} active CM instances.",
                  "cm_count": len(instances)})


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Synchronous entry point registered as a console script in pyproject.toml.

    Called by:
      - uv run cloudera-manager-mcp
      - uvx cloudera-manager-mcp
      - the 'command'/'args' block in Agent Studio's MCP server configuration
    """

    mcp.run()


if __name__ == "__main__":
    run()