"""
cm_registry.py — CM registry backed by an Iceberg table accessed via impyla.

Supports two SQL backends, selected via IMPALA_BACKEND in the environment:
  impala      — Impala daemon (impalad), default port 21050
  hiveserver2 — HiveServer2 (Hive on Tez, CDW, Spark Thrift Server), default port 10000

Both backends use the same impyla (impala.dbapi) interface. The connection
parameters and Thrift dialect differences are handled transparently by
ImpalaConnectionPool._open_connection().

Responsibilities:
  - Open and manage a thread-safe connection pool for the chosen backend.
  - Read the Iceberg table `cdp_registry.cloudera_managers`.
  - Build ClouderaManagerSettings objects from each row.
  - Cache the results in memory and refresh them periodically.
  - Expose CRUD operations to register, deactivate, and update CM entries.

The Iceberg table is the single source of truth for all CM instances.
The DDL is in docs/registry_table.sql.

Dependencies:
  - impyla      — Python driver for Impala and HiveServer2 (HiveServer2 protocol)
  - thrift-sasl — SASL transport for Kerberos / LDAP authentication
  - thrift       — underlying transport layer
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Iterator, Optional

import structlog
from impala.dbapi import connect as impala_connect
from impala.interface import Connection, Cursor

from config import ClouderaManagerSettings, ImpalaSettings

log = structlog.get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────

class RegistryError(Exception):
    """Base error for all registry failures."""

class RegistryConnectionError(RegistryError):
    """Cannot connect to Impala."""

class RegistryQueryError(RegistryError):
    """Error while executing a query against the registry table."""

class RegistryEmptyError(RegistryError):
    """No active CM instances found in the registry."""


# ─────────────────────────────────────────────────────────────────────────────
# IMPALA CONNECTION POOL
# impyla operations are synchronous; they are run via asyncio.run_in_executor
# to avoid blocking the MCP server's event loop.
# ─────────────────────────────────────────────────────────────────────────────

class ImpalaConnectionPool:
    """
    Thread-safe Impala connection pool backed by a Queue.

    Each slot holds an open impyla connection.
    Before returning a connection to a caller the pool validates it with a
    lightweight ping; dead connections are replaced transparently.
    """

    def __init__(self, cfg: ImpalaSettings) -> None:
        self._cfg  = cfg
        self._pool = Queue(maxsize=cfg.pool_size)
        self._lock = threading.Lock()

    def _open_connection(self) -> Connection:
        """
        Create a new impyla connection, routing through Knox if configured.

        Connection matrix:
        ┌─────────────┬───────────┬──────────────────────────────────────────┐
        │ backend     │ use_knox  │ transport / auth                         │
        ├─────────────┼───────────┼──────────────────────────────────────────┤
        │ impala      │ False     │ binary Thrift · PLAIN / LDAP / GSSAPI    │
        │ hiveserver2 │ False     │ binary Thrift · PLAIN / LDAP / GSSAPI    │
        │ impala      │ True      │ HTTP · Knox BASIC auth over HTTPS         │
        │ hiveserver2 │ True      │ HTTP · Knox BASIC auth over HTTPS         │
        └─────────────┴───────────┴──────────────────────────────────────────┘

        All routing decisions are delegated to ImpalaSettings properties
        (effective_host, effective_port, effective_auth_mechanism, etc.)
        so this method stays free of if/else branching on backend or Knox mode.
        """
        kwargs: dict = {
            "host":                self._cfg.effective_host,
            "port":                self._cfg.effective_port,
            "database":            self._cfg.database,
            "auth_mechanism":      self._cfg.effective_auth_mechanism,
            "use_ssl":             self._cfg.effective_use_ssl,
            "timeout":             self._cfg.timeout_seconds,
            "use_http_transport":  self._cfg.effective_use_http_transport,
        }

        # HTTP path — only meaningful when use_http_transport=True.
        # For direct binary-Thrift connections the value is an empty string
        # and impyla simply ignores it.
        if self._cfg.effective_http_path:
            kwargs["http_path"] = self._cfg.effective_http_path

        # Credentials for PLAIN and LDAP (covers both direct and Knox connections)
        if self._cfg.effective_auth_mechanism in ("PLAIN", "LDAP"):
            if self._cfg.username:
                kwargs["user"]     = self._cfg.username
            if self._cfg.password:
                kwargs["password"] = self._cfg.password

        # Kerberos — only for direct connections with auth_mechanism=GSSAPI
        elif self._cfg.effective_auth_mechanism == "GSSAPI":
            kwargs["kerberos_service_name"] = self._cfg.effective_krb_service

        # CA certificate for TLS verification (direct and Knox)
        if self._cfg.effective_use_ssl and self._cfg.ca_cert:
            kwargs["ca_cert"] = self._cfg.ca_cert

        try:
            conn = impala_connect(**kwargs)
            log.debug(
                "impala_pool.connection_opened",
                backend=self._cfg.backend,
                host=self._cfg.effective_host,
                port=self._cfg.effective_port,
                knox=self._cfg.use_knox,
                cluster_name=self._cfg.cluster_name if self._cfg.use_knox else None,
                http_transport=self._cfg.effective_use_http_transport,
            )
            return conn
        except Exception as exc:
            mode = (
                f"load-balancer ({self._cfg.lb_host}/{self._cfg.cluster_name})"
                if self._cfg.use_knox
                else "direct"
            )
            raise RegistryConnectionError(
                f"Cannot connect to {self._cfg.backend} via {mode} at "
                f"{self._cfg.effective_host}:{self._cfg.effective_port}: {exc}"
            ) from exc

    def _is_alive(self, conn: Connection) -> bool:
        """Ping the connection to verify it is still open."""
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            return False

    def initialize(self) -> None:
        """Pre-fill the pool with pool_size connections."""
        for _ in range(self._cfg.pool_size):
            conn = self._open_connection()
            self._pool.put_nowait(conn)
        log.info(
            "impala_pool.initialized",
            backend=self._cfg.backend,
            host=self._cfg.host,
            port=self._cfg.effective_port,
            size=self._cfg.pool_size,
        )

    @contextmanager
    def acquire(self) -> Iterator[Connection]:
        """
        Context manager that yields a validated connection from the pool.
        The connection is returned to the pool on exit.
        If the pool is exhausted, a temporary extra connection is opened.
        Dead connections are replaced automatically.
        """
        conn: Optional[Connection] = None
        try:
            conn = self._pool.get(timeout=self._cfg.timeout_seconds)
        except Empty:
            # Pool exhausted — open a temporary connection
            log.warning("impala_pool.exhausted_opening_extra")
            conn = self._open_connection()

        # Replace the connection if it has gone stale
        if not self._is_alive(conn):
            log.warning("impala_pool.stale_connection_replacing")
            try:
                conn.close()
            except Exception:
                pass
            conn = self._open_connection()

        try:
            yield conn
        finally:
            # Return to pool if there is room; otherwise close
            try:
                self._pool.put_nowait(conn)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass

    def close_all(self) -> None:
        """Close every connection in the pool."""
        closed = 0
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
                closed += 1
            except Exception:
                pass
        log.info("impala_pool.closed", connections_closed=closed)


# ─────────────────────────────────────────────────────────────────────────────
# CM REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

class CMRegistry:
    """
    Cloudera Manager registry persisted in an Iceberg table via Impala.

    Public API:
      start()           — initialise pool, ensure table exists, load cache, start refresh thread.
      stop()            — stop refresh thread and close pool.
      load()            — reload all active CM rows from the DB; refresh in-memory cache.
      get_all()         — return the cached list of active CMs.
      get_by_environment(name) — filter cache by environment name.
      get_by_host(host)        — look up a single CM by host.
      register(...)     — INSERT a new CM row.
      deactivate(host)  — soft-delete: set active=FALSE.
      update_field(...) — UPDATE a single column for a given host.
      list_raw(...)     — return all rows as plain dicts (passwords excluded).
      get_stats()       — aggregate statistics query.
      async_*(...)      — async wrappers for all write/read methods.
    """

    def __init__(self, cfg: ImpalaSettings) -> None:
        self._cfg              = cfg
        self._pool             = ImpalaConnectionPool(cfg)
        self._cache:           list[ClouderaManagerSettings] = []
        self._cache_lock       = threading.RLock()
        self._last_load:       Optional[float]               = None
        self._refresh_thread:  Optional[threading.Thread]    = None
        self._stop_event       = threading.Event()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Initialise the connection pool, ensure the registry table exists,
        perform an initial load, then start the background refresh thread.
        Must be called before any read/write operation.
        """
        self._pool.initialize()
        self._ensure_table_exists()
        self.load()

        if self._cfg.refresh_interval_secs > 0:
            self._stop_event.clear()
            self._refresh_thread = threading.Thread(
                target=self._refresh_loop,
                name="cm-registry-refresh",
                daemon=True,
            )
            self._refresh_thread.start()
            log.info(
                "cm_registry.refresh_thread_started",
                interval_secs=self._cfg.refresh_interval_secs,
            )

    def stop(self) -> None:
        """Stop the refresh thread and release all Impala connections."""
        self._stop_event.set()
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5)
        self._pool.close_all()
        log.info("cm_registry.stopped")

    # ── Table bootstrap ──────────────────────────────────────────────────────

    def _ensure_table_exists(self) -> None:
        """
        Create the registry database and Iceberg table if they do not already
        exist. In production, prefer managing DDL through dedicated migrations;
        this method is intended for development and first-time setup.
        """
        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(f"USE {self._cfg.database}")
            except Exception:
                log.info("cm_registry.creating_database", db=self._cfg.database)
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._cfg.database}")
                cur.execute(f"USE {self._cfg.database}")

            cur.execute(f"SHOW TABLES LIKE '{self._cfg.table}'")
            if not cur.fetchall():
                log.info("cm_registry.creating_table", table=self._cfg.full_table_name)
                self._create_table(cur)

            cur.close()

    def _create_table(self, cur: Cursor) -> None:
        """
        Issue the CREATE TABLE DDL for the Iceberg registry table.
        The storage clause differs between backends:

          Impala:      STORED AS ICEBERG
          HiveServer2: STORED BY ICEBERG

        See also docs/registry_table.sql for the standalone DDL file.
        """
        storage_clause = (
            "STORED BY ICEBERG"
            if self._cfg.is_hiveserver2
            else "STORED AS ICEBERG"
        )

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self._cfg.full_table_name} (
            cm_id            STRING    COMMENT 'Unique CM record identifier (UUID)',
            environment_name STRING    COMMENT 'CDP environment name (e.g. prod-eu-west)',
            host             STRING    COMMENT 'CM hostname or IP — used for direct connections',
            port             INT       COMMENT 'CM API port (default 7183)',
            username         STRING    COMMENT 'CM API username',
            password         STRING    COMMENT 'CM password — plaintext',
            use_tls          BOOLEAN   COMMENT 'Use HTTPS for direct connections',
            verify_ssl       BOOLEAN   COMMENT 'Verify TLS certificate (direct only; ignored via Knox)',
            api_version      STRING    COMMENT 'CM API version (e.g. v51 for CDP 7.1.9)',
            timeout_seconds  INT       COMMENT 'HTTP timeout for CM calls in seconds',
            active           BOOLEAN   COMMENT 'FALSE = soft-deleted, excluded from the pool',
            description      STRING    COMMENT 'Free-text notes about this CM entry',
            registered_at    TIMESTAMP COMMENT 'First registration timestamp (UTC)',
            updated_at       TIMESTAMP COMMENT 'Last modification timestamp (UTC)',
            cloud_provider   STRING    COMMENT 'Cloud provider: AWS | AZURE | GCP',
            region           STRING    COMMENT 'Cloud region (e.g. eu-west-1, eastus)',
            tags             STRING    COMMENT 'Free-form JSON tags',
            use_knox         BOOLEAN   COMMENT 'Route CM calls through CDP load balancer / Knox',
            lb_host          STRING    COMMENT 'CDP load balancer hostname — required when use_knox=TRUE',
            lb_port          INT       COMMENT 'CDP load balancer HTTPS port (default 443)',
            cluster_name     STRING    COMMENT 'CDP DataHub cluster name — used in the Knox URL path'
        )
        {storage_clause}
        TBLPROPERTIES (
            'format-version'      = '2',
            'write.merge.mode'    = 'merge-on-read',
            'write.delete.mode'   = 'merge-on-read',
            'write.update.mode'   = 'merge-on-read'
        )
        """
        cur.execute(ddl)
        log.info(
            "cm_registry.table_created",
            table=self._cfg.full_table_name,
            backend=self._cfg.backend,
            storage_clause=storage_clause,
        )

    # ── Read operations ──────────────────────────────────────────────────────

    def load(self) -> list[ClouderaManagerSettings]:
        """
        Query all active CM rows from Iceberg and rebuild the in-memory cache.
        Safe to call at any time; protected by a reentrant lock.
        Returns the refreshed list.
        """
        query = f"""
            SELECT
                host,
                port,
                username,
                password,
                use_tls,
                verify_ssl,
                api_version,
                timeout_seconds,
                environment_name,
                active,
                use_knox,
                lb_host,
                lb_port,
                cluster_name
            FROM {self._cfg.full_table_name}
            WHERE active = TRUE
            ORDER BY environment_name, host
        """
        log.info("cm_registry.loading", table=self._cfg.full_table_name)

        instances: list[ClouderaManagerSettings] = []

        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                rows         = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]
            except Exception as exc:
                raise RegistryQueryError(f"Failed to load registry: {exc}") from exc
            finally:
                cur.close()

        for row in rows:
            row_dict = dict(zip(column_names, row))
            try:
                instances.append(
                    ClouderaManagerSettings(
                        host=             row_dict["host"],
                        port=             int(row_dict["port"] or 7183),
                        username=         row_dict["username"] or "admin",
                        password=         row_dict["password"] or "",
                        use_tls=          bool(row_dict["use_tls"]),
                        verify_ssl=       bool(row_dict["verify_ssl"]),
                        api_version=      row_dict["api_version"] or "v51",
                        timeout_seconds=  int(row_dict["timeout_seconds"] or 30),
                        environment_name= row_dict["environment_name"] or "default",
                        active=           True,
                        use_knox=         bool(row_dict.get("use_knox", False)),
                        lb_host=          row_dict.get("lb_host"),
                        lb_port=          int(row_dict["lb_port"] or 443)
                                          if row_dict.get("lb_port") else 443,
                        cluster_name=     row_dict.get("cluster_name"),
                    )
                )
            except Exception as exc:
                log.error(
                    "cm_registry.row_parse_error",
                    host=row_dict.get("host"),
                    error=str(exc),
                )

        if not instances:
            log.warning(
                "cm_registry.no_active_cm_found",
                table=self._cfg.full_table_name,
            )

        with self._cache_lock:
            self._cache     = instances
            self._last_load = time.monotonic()

        log.info("cm_registry.loaded", count=len(instances))
        return instances

    def get_all(self) -> list[ClouderaManagerSettings]:
        """Return a snapshot of the in-memory cache."""
        with self._cache_lock:
            if not self._cache and self._last_load is None:
                raise RegistryEmptyError(
                    "Registry cache is empty. Call start() before get_all()."
                )
            return list(self._cache)

    def get_by_environment(self, environment_name: str) -> list[ClouderaManagerSettings]:
        """Return all cached CMs belonging to the given environment."""
        return [
            cm for cm in self.get_all()
            if cm.environment_name.lower() == environment_name.lower()
        ]

    def get_by_host(self, host: str) -> Optional[ClouderaManagerSettings]:
        """Return the CM with the given host, or None if not found."""
        return next(
            (cm for cm in self.get_all() if cm.host.lower() == host.lower()),
            None,
        )

    # ── Write operations ─────────────────────────────────────────────────────

    def register(
        self,
        host:             str,
        environment_name: str,
        username:         str,
        password:         str,
        port:             int           = 7183,
        use_tls:          bool          = True,
        verify_ssl:       bool          = True,
        api_version:      str           = "v51",
        timeout_seconds:  int           = 30,
        cloud_provider:   Optional[str] = None,
        region:           Optional[str] = None,
        description:      Optional[str] = None,
        tags:             Optional[str] = None,
        use_knox:         bool          = False,
        lb_host:          Optional[str] = None,
        lb_port:          int           = 443,
        cluster_name:     Optional[str] = None,
    ) -> str:
        """
        Insert a new CM row into the Iceberg table.
        Returns the generated cm_id (UUID).
        """
        if use_knox:
            if not lb_host:
                raise ValueError("lb_host is required when use_knox=True.")
            if not cluster_name:
                raise ValueError("cluster_name is required when use_knox=True.")

        cm_id = str(uuid.uuid4())
        now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        def _sql_str(value: Optional[str]) -> str:
            return f"'{value}'" if value else "NULL"

        def _sql_bool(value: bool) -> str:
            return "TRUE" if value else "FALSE"

        query = f"""
            INSERT INTO {self._cfg.full_table_name}
            (cm_id, environment_name, host, port, username, password,
             use_tls, verify_ssl, api_version, timeout_seconds, active,
             description, registered_at, updated_at,
             cloud_provider, region, tags,
             use_knox, lb_host, lb_port, cluster_name)
            VALUES (
                '{cm_id}',
                '{environment_name}',
                '{host}',
                {port},
                '{username}',
                '{password}',
                {_sql_bool(use_tls)},
                {_sql_bool(verify_ssl)},
                '{api_version}',
                {timeout_seconds},
                TRUE,
                {_sql_str(description)},
                '{now}', '{now}',
                {_sql_str(cloud_provider)},
                {_sql_str(region)},
                {_sql_str(tags)},
                {_sql_bool(use_knox)},
                {_sql_str(lb_host)},
                {lb_port},
                {_sql_str(cluster_name)}
            )
        """

        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                conn.commit()
            except Exception as exc:
                raise RegistryQueryError(
                    f"Failed to register CM {host}: {exc}"
                ) from exc
            finally:
                cur.close()

        log.info(
            "cm_registry.registered",
            cm_id=cm_id,
            host=host,
            environment=environment_name,
            use_knox=use_knox,
        )
        self.load()
        return cm_id

    def deactivate(self, host: str) -> None:
        """
        Soft-delete a CM by setting active=FALSE.
        The row is preserved for historical traceability.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
            UPDATE {self._cfg.full_table_name}
            SET active = FALSE, updated_at = '{now}'
            WHERE host = '{host}'
        """
        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                conn.commit()
            except Exception as exc:
                raise RegistryQueryError(
                    f"Failed to deactivate CM {host}: {exc}"
                ) from exc
            finally:
                cur.close()

        log.info("cm_registry.deactivated", host=host)
        self.load()

    def update_field(self, host: str, field: str, value: str) -> None:
        """
        Update a single column for the CM with the given host.

        Updatable fields:
          port, username, password, use_tls, verify_ssl,
          api_version, timeout_seconds, description,
          cloud_provider, region, tags.
        """
        allowed_fields = {
            "port", "username", "password", "use_tls", "verify_ssl",
            "api_version", "timeout_seconds", "description",
            "cloud_provider", "region", "tags",
            "use_knox", "lb_host", "lb_port", "cluster_name",
        }
        if field not in allowed_fields:
            raise ValueError(
                f"Field '{field}' is not updatable. "
                f"Allowed fields: {sorted(allowed_fields)}"
            )

        bool_fields = {"use_tls", "verify_ssl", "use_knox"}
        int_fields  = {"port", "timeout_seconds", "lb_port"}

        if field in bool_fields:
            sql_value = "TRUE" if value.lower() in ("true", "1", "yes") else "FALSE"
        elif field in int_fields:
            sql_value = str(int(value))
        else:
            sql_value = f"'{value}'"

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
            UPDATE {self._cfg.full_table_name}
            SET {field} = {sql_value}, updated_at = '{now}'
            WHERE host = '{host}'
        """

        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                conn.commit()
            except Exception as exc:
                raise RegistryQueryError(
                    f"Failed to update field '{field}' for CM {host}: {exc}"
                ) from exc
            finally:
                cur.close()

        log.info("cm_registry.field_updated", host=host, field=field)
        self.load()

    # ── Utility queries ──────────────────────────────────────────────────────

    def list_raw(self, include_inactive: bool = False) -> list[dict]:
        """
        Return all registry rows as plain dicts.
        Passwords are intentionally excluded from the output.
        Useful for audit, debugging, and the registry_list MCP tool.
        """
        where_clause = "" if include_inactive else "WHERE active = TRUE"
        query = f"""
            SELECT
                cm_id, environment_name, host, port, username,
                use_tls, verify_ssl, api_version, timeout_seconds,
                active, description, registered_at, updated_at,
                cloud_provider, region, tags,
                use_knox, lb_host, lb_port, cluster_name
            FROM {self._cfg.full_table_name}
            {where_clause}
            ORDER BY environment_name, host
        """
        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                rows         = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]
            except Exception as exc:
                raise RegistryQueryError(f"list_raw failed: {exc}") from exc
            finally:
                cur.close()

        return [dict(zip(column_names, row)) for row in rows]

    def get_stats(self) -> dict:
        """
        Return aggregate statistics about the registry:
        total CM count, active count, distinct environments, cloud providers.
        """
        query = f"""
            SELECT
                COUNT(*)                         AS total,
                SUM(CAST(active AS INT))          AS active_count,
                COUNT(DISTINCT environment_name)  AS environments,
                COUNT(DISTINCT cloud_provider)    AS cloud_providers
            FROM {self._cfg.full_table_name}
        """
        with self._pool.acquire() as conn:
            cur: Cursor = conn.cursor()
            try:
                cur.execute(query)
                row = cur.fetchone()
            except Exception as exc:
                raise RegistryQueryError(f"get_stats failed: {exc}") from exc
            finally:
                cur.close()

        return {
            "total_cm":      row[0],
            "active_cm":     row[1],
            "environments":  row[2],
            "cloud_providers": row[3],
            "last_load":     self._last_load,
            "cache_size":    len(self._cache),
        }

    # ── Async wrappers ───────────────────────────────────────────────────────
    # impyla is synchronous; these wrappers run the blocking calls on the
    # default thread pool executor so they do not block the MCP event loop.

    async def async_load(self) -> list[ClouderaManagerSettings]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.load)

    async def async_list_raw(self, include_inactive: bool = False) -> list[dict]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.list_raw, include_inactive)

    async def async_get_stats(self) -> dict:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get_stats)

    async def async_register(self, **kwargs) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.register(**kwargs))

    async def async_deactivate(self, host: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.deactivate, host)

    async def async_update_field(self, host: str, field: str, value: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.update_field, host, field, value)

    # ── Background refresh ───────────────────────────────────────────────────

    def _refresh_loop(self) -> None:
        """
        Daemon thread that periodically reloads the registry cache.
        Exits cleanly when _stop_event is set.
        """
        while not self._stop_event.wait(timeout=self._cfg.refresh_interval_secs):
            try:
                self.load()
                log.debug("cm_registry.auto_refreshed", count=len(self._cache))
            except Exception as exc:
                log.error("cm_registry.refresh_error", error=str(exc))