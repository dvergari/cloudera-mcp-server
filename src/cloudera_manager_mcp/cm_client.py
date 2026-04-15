"""
cm_client.py — Async HTTP client for the Cloudera Manager REST API.

Responsibilities:
  - Basic authentication
  - Exponential-backoff retry via tenacity
  - Per-CM connection pooling (httpx)
  - Typed error hierarchy for fine-grained error handling by callers
  - All CM API methods used by the MCP tools
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config import ClouderaManagerSettings, ServerSettings

log = structlog.get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class CMClientError(Exception):
    """Base error for all CM client failures."""

class CMAuthError(CMClientError):
    """Authentication or authorisation failure (HTTP 401/403)."""

class CMNotFoundError(CMClientError):
    """Requested resource does not exist (HTTP 404)."""

class CMServiceUnavailable(CMClientError):
    """CM unreachable or in maintenance mode (HTTP 503/504)."""

class CMCommandFailed(CMClientError):
    """An async CM command (restart, config apply, etc.) completed with failure."""


# ─────────────────────────────────────────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class ClouderaManagerClient:
    """
    Async HTTP client for a single Cloudera Manager instance.
    One instance per CM in the connection pool.

    Usage:
        async with ClouderaManagerClient(settings, server_cfg) as client:
            clusters = await client.list_clusters()
    """

    def __init__(
        self,
        settings:   ClouderaManagerSettings,
        server_cfg: ServerSettings,
    ) -> None:
        self.cfg        = settings
        self._server_cfg = server_cfg
        self._http:      Optional[httpx.AsyncClient] = None

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def __aenter__(self) -> "ClouderaManagerClient":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        """
        Open the underlying HTTP connection pool.

        Direct connection:
          base_url = https://<host>:<port>/api/<version>
          auth     = HTTP Basic (username:password)
          TLS      = controlled by use_tls / verify_ssl

        Via CDP load balancer (Knox):
          base_url = https://<lb_host>:<lb_port>/<cluster_name>/cdp-proxy-api/cm-api/<version>
          auth     = PLAIN over HTTPS (same username/password)
          TLS      = always enforced
        """
        self._http = httpx.AsyncClient(
            base_url=self.cfg.base_url,
            auth=(self.cfg.username, self.cfg.password),
            verify=self.cfg.effective_verify_ssl,
            timeout=httpx.Timeout(self.cfg.timeout_seconds),
            limits=httpx.Limits(
                max_connections=self._server_cfg.max_concurrent_requests,
                max_keepalive_connections=5,
            ),
            headers={
                "Accept":       "application/json",
                "Content-Type": "application/json",
            },
        )
        if self.cfg.use_knox:
            log.info(
                "cm_client.connected",
                mode="load-balancer",
                lb_host=self.cfg.lb_host,
                lb_port=self.cfg.lb_port,
                cluster_name=self.cfg.cluster_name,
                api=self.cfg.api_version,
                env=self.cfg.environment_name,
            )
        else:
            log.info(
                "cm_client.connected",
                mode="direct",
                host=self.cfg.host,
                port=self.cfg.port,
                api=self.cfg.api_version,
                env=self.cfg.environment_name,
            )

    async def close(self) -> None:
        """Close the HTTP connection pool."""
        if self._http:
            await self._http.aclose()
            log.info(
                "cm_client.closed",
                host=self.cfg.effective_host,
                env=self.cfg.environment_name,
            )

    # ── Internal HTTP helpers ────────────────────────────────────────────────

    def _retry_decorator(self):
        """Build a tenacity retry decorator with exponential backoff."""
        return retry(
            stop=stop_after_attempt(self._server_cfg.max_retries),
            wait=wait_exponential(
                multiplier=self._server_cfg.retry_wait_seconds,
                min=1,
                max=10,
            ),
            retry=retry_if_exception_type((httpx.TransportError, CMServiceUnavailable)),
            reraise=True,
        )

    async def _request(
        self,
        method:  str,
        path:    str,
        params:  Optional[dict] = None,
        json:    Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> Any:
        """
        Execute an HTTP request against the CM API with retry logic.
        Maps HTTP error codes to typed exceptions.

        headers: optional per-request headers that override the client defaults.
                 Used for endpoints that return text/plain instead of JSON
                 (e.g. /logs/full requires Accept: text/plain).
        """
        assert self._http, "Client not initialised. Call connect() or use async with."

        @self._retry_decorator()
        async def _execute() -> Any:
            try:
                response = await self._http.request(
                    method, path, params=params, json=json, headers=headers
                )
            except httpx.TransportError as exc:
                log.warning("cm_client.transport_error", path=path, error=str(exc))
                raise

            if response.status_code == 401:
                raise CMAuthError(
                    f"Authentication failed for {self.cfg.host}. "
                    "Check username and password."
                )
            if response.status_code == 403:
                raise CMAuthError(f"Permission denied: {path}")
            if response.status_code == 404:
                raise CMNotFoundError(f"Resource not found: {path}")
            if response.status_code in (503, 504):
                log.warning(
                    "cm_client.server_unavailable",
                    status=response.status_code,
                    path=path,
                )
                raise CMServiceUnavailable(
                    f"CM unavailable (HTTP {response.status_code}): {self.cfg.host}"
                )
            if response.status_code >= 400:
                raise CMClientError(
                    f"CM returned HTTP {response.status_code}: {response.text[:300]}"
                )

            return response.json() if response.content else {}

        return await _execute()

    async def _get(self, path: str, params: Optional[dict] = None) -> Any:
        return await self._request("GET", path, params=params)

    async def _put(self, path: str, json: dict) -> Any:
        return await self._request("PUT", path, json=json)

    async def _post(self, path: str, json: Optional[dict] = None) -> Any:
        return await self._request("POST", path, json=json or {})

    async def _get_text(self, path: str, params: Optional[dict] = None) -> str:
        """
        GET request that expects a plain-text response (e.g. /logs/full).
        Sends Accept: text/plain and returns the raw response body as a string.
        """
        assert self._http, "Client not initialised. Call connect() or use async with."

        @self._retry_decorator()
        async def _execute() -> str:
            try:
                response = await self._http.request(
                    "GET", path, params=params,
                    headers={"Accept": "text/plain"},
                )
            except httpx.TransportError as exc:
                log.warning("cm_client.transport_error", path=path, error=str(exc))
                raise

            if response.status_code == 401:
                raise CMAuthError(f"Authentication failed for {self.cfg.host}.")
            if response.status_code == 403:
                raise CMAuthError(f"Permission denied: {path}")
            if response.status_code == 404:
                raise CMNotFoundError(f"Resource not found: {path}")
            if response.status_code in (503, 504):
                raise CMServiceUnavailable(
                    f"CM unavailable (HTTP {response.status_code}): {self.cfg.host}"
                )
            if response.status_code >= 400:
                raise CMClientError(
                    f"CM returned HTTP {response.status_code}: {response.text[:300]}"
                )

            return response.text

        return await _execute()

    # ── Utility ──────────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        """Current UTC time as ISO 8601 string."""
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _validate_time_range(
        start_time: str,
        end_time:   Optional[str],
        max_hours:  int,
    ) -> tuple[str, str]:
        """
        Parse and validate a time range.
        Raises ValueError if the range exceeds max_hours or end < start.
        Returns (start_iso, end_iso) as normalised strings.
        """
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = (
            datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            if end_time
            else datetime.now(timezone.utc)
        )
        delta_hours = (end_dt - start_dt).total_seconds() / 3600

        if delta_hours > max_hours:
            raise ValueError(
                f"Time range too wide: {delta_hours:.1f}h exceeds the "
                f"{max_hours}h maximum. Narrow the range or raise MCP_MAX_LOG_RANGE_HOURS."
            )
        if delta_hours <= 0:
            raise ValueError("end_time must be later than start_time.")

        return start_dt.isoformat(), end_dt.isoformat()

    # ── Cluster discovery ────────────────────────────────────────────────────

    async def list_clusters(self) -> list[dict]:
        """
        Return the cluster(s) managed by this CM instance.

        Knox (CDP Public Cloud):
          GET /clusters is not available. Returns a synthetic entry built
          from the registry configuration — no API call is made.

        Direct:
          Calls GET /clusters and returns the full CM response.
        """
        if self.cfg.use_knox:
            return [{"name": self.cfg.cluster_name, "displayName": self.cfg.cluster_name}]

        data = await self._get("/clusters")
        return data.get("items", [])

    async def list_services(self, cluster_name: str) -> list[dict]:
        """Return all services in a cluster."""
        data = await self._get(f"/clusters/{cluster_name}/services")
        return data.get("items", [])

    async def _resolve_service_name(
        self, cluster_name: str, service_name: str
    ) -> str:
        """
        Return the exact service name as registered in CM, performing a
        case-insensitive match against the list of services in the cluster.

        Raises ValueError if no matching service is found.
        """
        services = await self.list_services(cluster_name)
        service_names = [s.get("name", "") for s in services]

        for name in service_names:
            if name.lower() == service_name.lower():
                if name != service_name:
                    log.debug(
                        "cm_client.service_name_corrected",
                        requested=service_name,
                        resolved=name,
                        cluster=cluster_name,
                    )
                return name

        raise ValueError(
            f"Service '{service_name}' not found on cluster '{cluster_name}'. "
            f"Available services: {sorted(service_names)}"
        )

    async def get_service(self, cluster_name: str, service_name: str) -> dict:
        """Return details for a single service."""
        service_name = await self._resolve_service_name(cluster_name, service_name)
        return await self._get(f"/clusters/{cluster_name}/services/{service_name}")

    # ── Log extraction ───────────────────────────────────────────────────────

    async def get_service_logs(
        self,
        cluster_name:   str,
        service_name:   str,
        start_time:     str,
        end_time:       Optional[str],
        host_filter:    Optional[str],
        role_filter:    Optional[str],
        level_filter:   Optional[str],
        keyword_filter: Optional[str],
        max_lines:      int,
        max_hours:      int,
    ) -> dict:
        """
        Extract logs from all roles of a service in parallel.

        Steps:
          1. Resolve the exact service name (case-insensitive match).
          2. Validate and normalise the time range.
          3. Enumerate all roles; exclude GATEWAY, apply host/role filters.
          4. Fetch /logs/full per role concurrently (max 5 at a time).
             NOTE: /logs/full does not support startTime, endTime or lines
             as query parameters — the full log file is always returned and
             all filtering is applied client-side.
          5. Filter by time range, log level hierarchy, and keyword.
          6. Aggregate by (level, message, server), sort by num_occurrence desc.

        Log level hierarchy (ascending severity):
          TRACE(0) < DEBUG(1) < INFO(2) < WARN(3) < ERROR(4) < FATAL(5)
        Specifying a level returns that level AND all higher-severity ones.
        Example: level_filter=WARN  →  includes WARN, ERROR, FATAL
                 level_filter=INFO  →  includes INFO, WARN, ERROR, FATAL
        """
        _LEVEL_RANK = {
            "TRACE": 0, "DEBUG": 1, "INFO": 2,
            "WARN":  3, "ERROR": 4, "FATAL": 5,
        }

        service_name = await self._resolve_service_name(cluster_name, service_name)
        start_iso, end_iso = self._validate_time_range(start_time, end_time, max_hours)

        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso)
        min_rank = _LEVEL_RANK.get((level_filter or "").upper(), 0) if level_filter else -1

        # Enumerate roles
        roles_data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/roles"
        )
        all_roles = roles_data.get("items", [])

        # GATEWAY roles are excluded unconditionally — CM does not store
        # meaningful service logs for them.
        target_roles = [
            r for r in all_roles
            if r.get("type", "").upper() != "GATEWAY"
            and (
                not host_filter
                or host_filter.lower() in r.get("hostRef", {}).get("hostname", "").lower()
            )
            and (
                not role_filter
                or role_filter.upper() in r.get("type", "").upper()
            )
        ]

        if not target_roles:
            return {
                "cluster":         cluster_name,
                "service":         service_name,
                "start_time":      start_dt,
                "end_time":        end_dt,
                "total_unique":    0,
                "total_raw_lines": 0,
                "entries":         [],
                "warning":         "No roles matched the provided filters.",
            }

        log.info(
            "cm_client.logs.fetching",
            cluster=cluster_name,
            service=service_name,
            role_count=len(target_roles),
            start=start_iso,
            end=end_iso,
        )

        semaphore    = asyncio.Semaphore(5)
        all_entries: list[dict] = []

        async def _fetch_role_logs(role: dict) -> list[dict]:
            async with semaphore:
                role_name = role["name"]
                role_type = role.get("type", "UNKNOWN")
                hostname  = role.get("hostRef", {}).get("hostname", "unknown")
                try:
                    # /logs/full returns the entire log as plain text.
                    # No query parameters are supported by this endpoint.
                    raw_text = await self._get_text(
                        f"/clusters/{cluster_name}/services/{service_name}"
                        f"/roles/{role_name}/logs/full",
                    )

                    log.debug(
                        "cm_client.logs.raw_text_length",
                        role=role_name,
                        chars=len(raw_text),
                    )

                    entries = []
                    for raw_line in raw_text.splitlines():
                        if len(entries) >= max_lines:
                            break
                        raw_line = raw_line.strip()
                        if not raw_line:
                            continue

                        # Tab-separated: timestamp \t level \t thread \t class \t message
                        parts   = raw_line.split(maxsplit=4)
                        ts_str  = f"{parts[0]} {parts[1]}" if len(parts) > 1 else ""
                        level   = parts[2].strip() if len(parts) > 2 else "INFO"
                        message = parts[4] if len(parts) > 4 else raw_line


                        # Filter by time range (client-side)
                        try:
                            ts_dt = datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            )




                            if ts_dt.tzinfo is None:
                                ts_dt = ts_dt.replace(tzinfo=timezone.utc)


                            if ts_dt < start_dt or ts_dt > end_dt:
                                continue
                        except ValueError:
                            pass  # unparseable timestamp — include the line

                        # Filter by log level hierarchy (client-side)
                        # min_rank == -1 means no filter (ALL levels)
                        if min_rank >= 0:
                            if _LEVEL_RANK.get(level.upper(), 0) < min_rank:
                                continue

                        # Filter by keyword (client-side)
                        if keyword_filter and keyword_filter.lower() not in message.lower():
                            continue

                        entries.append({
                            "timestamp": ts_str,
                            "level":     level,
                            "host":      hostname,
                            "role":      role_type,
                            "message":   message,
                            "log_class": parts[3] if len(parts) > 3 else None,
                        })
                    return entries

                except CMNotFoundError:
                    log.warning("cm_client.logs.role_not_found", role=role_name)
                    return []
                except Exception as exc:
                    log.error("cm_client.logs.role_error", role=role_name, error=str(exc))
                    return []

        results = await asyncio.gather(*[_fetch_role_logs(r) for r in target_roles])
        for batch in results:
            all_entries.extend(batch)

        # Aggregate by (level, message, server)
        aggregated: dict[tuple, dict] = {}
        for entry in all_entries:
            key = (
                entry.get("level",   ""),
                entry.get("message", ""),
                entry.get("host",    ""),
            )
            if key not in aggregated:
                aggregated[key] = {
                    "level":          entry.get("level",   ""),
                    "message":        entry.get("message", ""),
                    "server":         entry.get("host",    ""),
                    "num_occurrence": 0,
                    "last_seen":      "",
                }
            aggregated[key]["num_occurrence"] += 1
            ts = entry.get("timestamp", "")
            if ts > aggregated[key]["last_seen"]:
                aggregated[key]["last_seen"] = ts

        aggregated_list = sorted(
            aggregated.values(),
            key=lambda e: e["num_occurrence"],
            reverse=True,
        )

        return {
            "cluster":         cluster_name,
            "service":         service_name,
            "start_time":      start_iso,
            "end_time":        end_iso,
            "total_unique":    len(aggregated_list),
            "total_raw_lines": len(all_entries),
            "entries":         aggregated_list,
        }

    # ── Alerts / Events ──────────────────────────────────────────────────────

    async def get_alerts(
        self,
        cluster_name:   Optional[str],
        severity:       str,
        start_time:     Optional[str],
        end_time:       Optional[str],
        service_filter: Optional[str],
        max_results:    int,
    ) -> dict:
        """
        Fetch events and health alerts via the CM /events endpoint.

        CM Events API query language notes:
          - Time range: passed as separate 'from' and 'to' query parameters,
            NOT embedded in the query string.
          - Severity, cluster, service: filtered client-side from event
            attributes — the CM query language does not support these
            comparisons reliably across versions.

        CM severity levels (ordered lowest to highest):
          INFORMATION < IMPORTANT < CRITICAL
        """
        _SEVERITY_RANK = {"INFORMATION": 0, "IMPORTANT": 1, "CRITICAL": 2}

        query_params: dict = {
            "maxResults":   max_results,
            "resultOffset": 0,
        }

        if start_time:
            query_params["from"] = start_time
        if end_time:
            query_params["to"]   = end_time

        data  = await self._get("/events", params=query_params)
        items = data.get("items", [])

        min_rank = _SEVERITY_RANK.get(severity, 0) if severity != "ALL" else -1

        alerts = []
        for item in items:
            attr_map = {
                a["name"]: a.get("values", [None])[0]
                for a in item.get("attributes", [])
            }
            item_cluster  = attr_map.get("CLUSTER",  attr_map.get("cluster",  ""))
            item_service  = attr_map.get("SERVICE",  attr_map.get("service",  ""))
            item_severity = item.get("severity", "")

            if cluster_name and cluster_name.lower() not in item_cluster.lower():
                continue
            if service_filter and service_filter.lower() not in item_service.lower():
                continue
            if min_rank >= 0:
                if _SEVERITY_RANK.get(item_severity, 0) < min_rank:
                    continue

            alerts.append({
                "timestamp": item.get("timeOccurred", item.get("timeReceived", "")),
                "severity":  item_severity,
                "service":   item_service,
                "host":      attr_map.get("HOSTID", attr_map.get("host", "")),
                "cluster":   item_cluster or cluster_name or "",
                "content":   item.get("content", ""),
                "category":  item.get("category"),
            })

        return {"total": len(alerts), "alerts": alerts}

    # ── Metrics ──────────────────────────────────────────────────────────────

    async def get_service_metrics(
        self,
        cluster_name:  str,
        service_name:  str,
        metric_names:  list[str],
        start_time:    str,
        end_time:      Optional[str],
        roll_up:       str,
    ) -> dict:
        """
        Retrieve time-series metrics via the CM tsquery language.
        One SELECT clause per requested metric.
        """
        service_name  = await self._resolve_service_name(cluster_name, service_name)
        end_iso       = end_time or self._now_iso()
        service_lower = service_name.lower()

        selectors = ", ".join(
            f"select {metric} "
            f"where serviceName='{service_lower}' "
            f"and clusterName='{cluster_name}'"
            for metric in metric_names
        )

        query_params = {
            "query":                selectors,
            "from":                 start_time,
            "to":                   end_iso,
            "desiredRollup":        roll_up,
            "mustUseDesiredRollup": False,
        }

        data  = await self._get("/timeseries", params=query_params)
        items = data.get("items", [])

        series_list = []
        for item in items:
            for series in item.get("timeSeries", []):
                meta   = series.get("metadata", {})
                points = [
                    {
                        "timestamp": point.get("timestamp", ""),
                        "value":     point.get("value"),
                    }
                    for point in series.get("data", [])
                ]
                series_list.append({
                    "metric_name": meta.get("metricName", ""),
                    "entity_name": meta.get("entityName", service_name),
                    "unit":        (meta.get("unitNumerators") or [None])[0],
                    "points":      points,
                })

        return {
            "cluster": cluster_name,
            "service": service_name,
            "series":  series_list,
        }

    # ── Configuration ────────────────────────────────────────────────────────

    async def get_config(
        self,
        cluster_name:      str,
        service_name:      str,
        role_config_group: Optional[str],
        view:              str,
    ) -> dict:
        """
        Read service or role-config-group configuration.
        view='full'    → all parameters, including defaults
        view='summary' → only explicitly overridden parameters
        """
        service_name = await self._resolve_service_name(cluster_name, service_name)
        query_params = {"view": view}

        if role_config_group:
            path = (
                f"/clusters/{cluster_name}/services/{service_name}"
                f"/roleConfigGroups/{role_config_group}/config"
            )
        else:
            path = f"/clusters/{cluster_name}/services/{service_name}/config"

        data = await self._get(path, params=query_params)

        params_list = [
            {
                "name":               item.get("name", ""),
                "value":              item.get("value"),
                "default_value":      item.get("default"),
                "required":           item.get("required", False),
                "sensitive":          item.get("sensitive", False),
                "description":        item.get("description"),
                "validation_state":   item.get("validationState"),
                "validation_message": item.get("validationWarningsSuppressed"),
            }
            for item in data.get("items", [])
        ]

        return {
            "cluster":           cluster_name,
            "service":           service_name,
            "role_config_group": role_config_group,
            "params":            params_list,
        }

    async def update_config(
        self,
        cluster_name:      str,
        service_name:      str,
        config_key:        str,
        config_value:      str,
        role_config_group: Optional[str],
        message:           Optional[str],
    ) -> dict:
        """
        Update a single configuration parameter.
        Reads the current value first so old_value is always present in the result.
        Does NOT restart the service — call run_service_command afterwards if needed.
        """
        service_name = await self._resolve_service_name(cluster_name, service_name)

        current = await self.get_config(
            cluster_name, service_name, role_config_group, "summary"
        )
        old_value = next(
            (p["value"] for p in current["params"] if p["name"] == config_key),
            None,
        )

        payload: dict = {"items": [{"name": config_key, "value": config_value}]}
        if message:
            payload["message"] = message

        if role_config_group:
            path = (
                f"/clusters/{cluster_name}/services/{service_name}"
                f"/roleConfigGroups/{role_config_group}/config"
            )
        else:
            path = f"/clusters/{cluster_name}/services/{service_name}/config"

        await self._put(path, json=payload)

        log.info(
            "cm_client.config.updated",
            cluster=cluster_name,
            service=service_name,
            key=config_key,
            old=old_value,
            new=config_value,
        )

        return {
            "cluster":    cluster_name,
            "service":    service_name,
            "config_key": config_key,
            "old_value":  old_value,
            "new_value":  config_value,
            "success":    True,
            "message":    f"Configuration updated successfully. {message or ''}".strip(),
        }

    # ── Service commands ─────────────────────────────────────────────────────

    async def run_service_command(
        self,
        cluster_name: str,
        service_name: str,
        command:      str,
        extra_args:   Optional[dict] = None,
    ) -> dict:
        """
        Issue an async command against a service (start, stop, restart, rollingRestart).
        Returns immediately with a command_id.
        Poll status with get_command_status().
        """
        service_name = await self._resolve_service_name(cluster_name, service_name)
        path = (
            f"/clusters/{cluster_name}/services/{service_name}"
            f"/commands/{command}"
        )
        data = await self._post(path, json=extra_args)

        log.info(
            "cm_client.command.submitted",
            cluster=cluster_name,
            service=service_name,
            command=command,
            command_id=data.get("id"),
        )

        return {
            "command_id":     data.get("id"),
            "name":           data.get("name", command),
            "cluster":        cluster_name,
            "service":        service_name,
            "success":        data.get("success"),
            "active":         data.get("active", True),
            "start_time":     data.get("startTime"),
            "end_time":       data.get("endTime"),
            "result_message": data.get("resultMessage"),
        }

    async def get_command_status(self, command_id: int) -> dict:
        """Poll the status of a running or completed CM command."""
        data = await self._get(f"/commands/{command_id}")
        return {
            "command_id":     data.get("id"),
            "name":           data.get("name"),
            "cluster":        data.get("clusterRef", {}).get("clusterName"),
            "service":        data.get("serviceRef", {}).get("serviceName"),
            "success":        data.get("success"),
            "active":         data.get("active", False),
            "start_time":     data.get("startTime"),
            "end_time":       data.get("endTime"),
            "result_message": data.get("resultMessage"),
        }

    # ── Host status ──────────────────────────────────────────────────────────

    async def get_host_status(
        self,
        cluster_name:  Optional[str],
        host_filter:   Optional[str],
        include_roles: bool,
    ) -> dict:
        """
        Return health status for all hosts, optionally filtered by cluster
        and hostname. Includes active roles per host when include_roles=True.
        """
        query_params: dict = {"view": "full" if include_roles else "summary"}
        data      = await self._get("/hosts", params=query_params)
        all_hosts = data.get("items", [])

        hosts = []
        for host in all_hosts:
            hostname  = host.get("hostname", "")
            h_cluster = host.get("clusterRef", {}).get("clusterName")

            if cluster_name and h_cluster != cluster_name:
                continue
            if host_filter and host_filter.lower() not in hostname.lower():
                continue

            roles = []
            if include_roles:
                for role_ref in host.get("roleRefs", []):
                    roles.append({
                        "role_name":    role_ref.get("roleName", ""),
                        "role_type":    role_ref.get("roleType", ""),
                        "service_name": role_ref.get("serviceName", ""),
                        "health_status": "",
                    })

            hosts.append({
                "hostname":         hostname,
                "ip_address":       host.get("ipAddress", ""),
                "cluster":          h_cluster,
                "health_status":    host.get("healthSummary", "UNKNOWN"),
                "commission_state": host.get("commissionState", "UNKNOWN"),
                "maintenance_mode": host.get("maintenanceMode", False),
                "num_cores":        host.get("numCores", 0),
                "total_ram_gb":     round(host.get("totalPhysMemBytes", 0) / (1024 ** 3), 2),
                "roles":            roles,
            })

        return {"total_hosts": len(hosts), "hosts": hosts}

    # ── Audit log ────────────────────────────────────────────────────────────

    async def get_audit_events(
        self,
        cluster_name:     Optional[str],
        start_time:       str,
        end_time:         Optional[str],
        service_filter:   Optional[str],
        user_filter:      Optional[str],
        operation_filter: Optional[str],
        allowed_filter:   Optional[bool],
        max_results:      int,
    ) -> dict:
        """Fetch audit log entries from CM's /audits endpoint."""
        query_params: dict = {
            "maxResults": max_results,
            "startTime":  start_time,
            "endTime":    end_time or self._now_iso(),
        }
        if cluster_name:
            query_params["clusterName"] = cluster_name
        if service_filter:
            query_params["service"]     = service_filter
        if user_filter:
            query_params["username"]    = user_filter
        if operation_filter:
            query_params["command"]     = operation_filter
        if allowed_filter is not None:
            query_params["allowed"]     = str(allowed_filter).lower()

        data  = await self._get("/audits", params=query_params)
        items = data.get("items", [])

        entries = [
            {
                "timestamp":  item.get("timestamp", ""),
                "service":    item.get("service", ""),
                "user":       item.get("username", ""),
                "command":    item.get("command", ""),
                "resource":   item.get("resource"),
                "operation":  item.get("operationText", item.get("command", "")),
                "allowed":    item.get("allowed", True),
                "ip_address": item.get("ipAddress"),
                "cluster":    cluster_name or item.get("cluster", ""),
            }
            for item in items
        ]

        return {"total": len(entries), "entries": entries}

    # ── DataHub enumeration ──────────────────────────────────────────────────

    async def list_datahubs(
        self,
        environment_name: Optional[str],
        status_filter:    Optional[str],
    ) -> dict:
        """
        Enumerate DataHubs managed by this CM instance.

        Knox (CDP Public Cloud):
          Returns a synthetic entry built from the registry configuration
          plus the service list from GET /clusters/<cluster_name>/services.

        Direct:
          Uses GET /clusters with view=full, enriched with service list.
        """
        if self.cfg.use_knox:
            cluster_name = self.cfg.cluster_name
            try:
                svc_data = await self._get(f"/clusters/{cluster_name}/services")
                services = [s.get("name", "") for s in svc_data.get("items", [])]
            except Exception:
                services = []

            datahub = {
                "name":           cluster_name,
                "environment":    environment_name or self.cfg.environment_name,
                "status":         "AVAILABLE",
                "cluster_type":   None,
                "cm_host":        self.cfg.effective_host,
                "services":       services,
                "nodes_count":    0,
                "cloud_provider": None,
                "region":         None,
            }
            return {"total": 1, "datahubs": [datahub]}

        data     = await self._get("/clusters", params={"view": "full"})
        clusters = data.get("items", [])

        datahubs = []
        for cluster in clusters:
            cluster_status = cluster.get("entityStatus", "UNKNOWN")
            if status_filter and status_filter != "ALL" and cluster_status != status_filter:
                continue

            services = [s.get("name", "") for s in cluster.get("services", [])]
            if not services:
                svc_data = await self._get(f"/clusters/{cluster['name']}/services")
                services = [s.get("name", "") for s in svc_data.get("items", [])]

            datahubs.append({
                "name":           cluster.get("name", ""),
                "environment":    environment_name or self.cfg.environment_name,
                "status":         cluster_status,
                "cluster_type":   cluster.get("clusterType"),
                "cm_host":        self.cfg.host,
                "services":       services,
                "nodes_count":    cluster.get("hostsCount", 0),
                "cloud_provider": cluster.get("cloudProvider"),
                "region":         cluster.get("region"),
            })

        return {"total": len(datahubs), "datahubs": datahubs}