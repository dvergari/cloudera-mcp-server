"""
config.py — Configuration for the Cloudera Manager MCP Server.

Settings are split into two layers:

  1. ServerSettings / ImpalaSettings  →  read from environment variables or .env file.
     Contains only infrastructure parameters (MCP server behaviour, SQL backend).
     Never contains CM credentials.

  2. ClouderaManagerSettings          →  built at runtime by cm_registry.py.
     Read from the Iceberg table `cdp_registry.cloudera_managers`.
     Plain dataclass, not a BaseSettings subclass, because data comes from the DB.

SQL backend selection (IMPALA_BACKEND):
  "impala"      — Connect directly to an Impala daemon (impalad). Default port: 21050.
                  Transport: binary Thrift.
  "hiveserver2" — Connect directly to HiveServer2. Default port: 10000.
                  Transport: binary Thrift.

Knox gateway (IMPALA_USE_KNOX=true):
  Routes the connection through an Apache Knox gateway instead of connecting
  directly to Impala or HiveServer2. Knox always uses:
    - HTTPS (TLS mandatory)
    - HTTP transport (not binary Thrift)
    - Basic auth (username + password) or JWT token
    - A URL path of the form:
        /gateway/<topology>/<service>
      where <service> is "impala" or "hive" depending on IMPALA_BACKEND.

  When Knox is enabled the effective connection parameters change:
    host   → IMPALA_LB_HOST       (CDP load balancer host)
    port   → IMPALA_LB_PORT       (default 443)
    path   → /gateway/<topology>/<service>
    auth   → BASIC over HTTPS (username + password)
    TLS    → always True; certificate verified via IMPALA_CA_CERT or system store

Both backends (impala / hiveserver2) can be used with or without Knox.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic_settings import BaseSettings


# ─────────────────────────────────────────────────────────────────────────────
# CLOUDERA MANAGER SETTINGS
# Plain dataclass built from Iceberg rows — does NOT read from environment.
# ─────────────────────────────────────────────────────────────────────────────

@pydantic_dataclass
class ClouderaManagerSettings:
    """
    Represents a single Cloudera Manager instance as read from the
    Iceberg registry table cdp_registry.cloudera_managers.

    Two connection modes:

    Direct (use_knox=False):
      Connect straight to the CM host on the CM API port.
      URL: https://<host>:<port>/api/<version>/

    Via Knox gateway (use_knox=True) — CDP Public Cloud:
      All CM API calls go through the CDP load balancer / Knox gateway.
      URL: https://<lb_host>:<lb_port>/<cluster_name>/cdp-proxy-api/cm-api/api/<version>/
      Auth: PLAIN over the HTTPS-secured channel (username + password).
      TLS is always enforced.

    Reference endpoint (CDP Public Cloud):
      https://<load balancer url>:<load balancer port>/<cluster-name>/cdp-proxy-api/cm-api
    """

    # ── Direct connection ─────────────────────────────────────────────────────
    host:              str
    port:              int            = 7183
    username:          str            = "admin"
    password:          str            = ""
    use_tls:           bool           = True
    verify_ssl:        bool           = True
    api_version:       str            = "v51"
    timeout_seconds:   int            = 30
    environment_name:  str            = "default"
    active:            bool           = True

    # ── Knox gateway (CDP Public Cloud load balancer) ─────────────────────────
    use_knox:      bool           = False
    lb_host:       Optional[str]  = None   # load balancer hostname
    lb_port:       int            = 443    # load balancer HTTPS port
    cluster_name:  Optional[str]  = None   # CDP cluster name — used in the URL path

    # ── Derived properties ────────────────────────────────────────────────────

    @property
    def effective_host(self) -> str:
        return self.lb_host if self.use_knox else self.host

    @property
    def effective_port(self) -> int:
        return self.lb_port if self.use_knox else self.port

    @property
    def effective_verify_ssl(self) -> bool:
        """TLS is always verified through Knox / load balancer."""
        return True if self.use_knox else self.verify_ssl

    @property
    def base_url(self) -> str:
        """
        Full base URL for CM REST API calls.

        Direct:
          https://<host>:<port>/api/<version>

        Knox / CDP Public Cloud:
          https://<lb_host>:<lb_port>/<cluster_name>/cdp-proxy-api/cm-api/api/<version>
        """
        if self.use_knox:
            return (
                f"https://{self.lb_host}:{self.lb_port}"
                f"/{self.cluster_name}/cdp-proxy-api/cm-api"
                f"/api/{self.api_version}"
            )
        scheme = "https" if self.use_tls else "http"
        return f"{scheme}://{self.host}:{self.port}/api/{self.api_version}"

    def __repr__(self) -> str:
        if self.use_knox:
            return (
                f"ClouderaManagerSettings(lb={self.lb_host!r}, "
                f"cluster={self.cluster_name!r}, "
                f"env={self.environment_name!r})"
            )
        return (
            f"ClouderaManagerSettings(host={self.host!r}, "
            f"env={self.environment_name!r}, "
            f"api={self.api_version}, tls={self.use_tls})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# SQL BACKEND SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

# Default direct-connection ports (no Knox)
_DEFAULT_PORTS: dict[str, int] = {
    "impala":      21050,
    "hiveserver2": 10000,
}

# Default Kerberos service principal name (direct connection only)
_DEFAULT_KRB_SERVICE: dict[str, str] = {
    "impala":      "impala",
    "hiveserver2": "hive",
}

# cdp-proxy-api service name per backend — used to build the Knox httpPath:
#   <cluster_name>/cdp-proxy-api/<service>
_CDP_PROXY_SERVICE: dict[str, str] = {
    "impala":      "impala",
    "hiveserver2": "hive",
}


class ImpalaSettings(BaseSettings):
    """
    Connection settings for the SQL backend (Impala or HiveServer2)
    that hosts the CM registry Iceberg table.

    Direct connection (IMPALA_USE_KNOX=false, default):
      Binary Thrift transport directly to the backend daemon.
      Auth: PLAIN, LDAP, or GSSAPI (Kerberos).

    CDP Public Cloud load balancer / Knox (IMPALA_USE_KNOX=true):
      HTTP transport through the CDP load balancer.
      Auth: PLAIN over HTTPS.
      The httpPath follows the CDP Public Cloud convention:
        Impala:      <cluster_name>/cdp-proxy-api/impala
        HiveServer2: <cluster_name>/cdp-proxy-api/hive

      Full JDBC reference strings (CDP Public Cloud):
        Impala:
          jdbc:impala://<lb_host>:<lb_port>/;ssl=1;transportMode=http;
          httpPath=<cluster_name>/cdp-proxy-api/impala;AuthMech=3
        HiveServer2:
          jdbc:hive2://<lb_host>:<lb_port>/;ssl=true;transportMode=http;
          httpPath=<cluster_name>/cdp-proxy-api/hive
    """

    # ── Backend selection ─────────────────────────────────────────────────────
    backend: Literal["impala", "hiveserver2"] = Field(
        "impala",
        description=(
            "SQL backend: 'impala' (port 21050) or 'hiveserver2' (port 10000)"
        ),
    )

    # ── Direct connection ─────────────────────────────────────────────────────
    host: str = Field(
        ...,
        description=(
            "Hostname or IP of the backend server (impalad or HiveServer2). "
            "Ignored when use_knox=True — set lb_host instead."
        ),
    )
    port: Optional[int] = Field(
        None,
        description=(
            "Backend port. Defaults to 21050 (impala) or 10000 (hiveserver2). "
            "Ignored when use_knox=True."
        ),
    )
    database: str = Field("cdp_registry", description="Iceberg database name")
    table:    str = Field("cloudera_managers", description="Registry table name")

    # ── Authentication (direct connection) ────────────────────────────────────
    auth_mechanism: str = Field(
        "PLAIN",
        description="Auth mechanism: PLAIN (dev), LDAP, GSSAPI (Kerberos). Ignored when use_knox=True.",
    )
    username: Optional[str] = Field(None, description="Username for PLAIN / LDAP / Knox BASIC auth")
    password: Optional[str] = Field(None, description="Password for PLAIN / LDAP / Knox BASIC auth")

    # Kerberos — only for direct connection with auth_mechanism=GSSAPI
    kerberos_service_name: Optional[str] = Field(
        None,
        description=(
            "Kerberos service principal name. "
            "Defaults to 'impala' or 'hive' by backend. "
            "Not used when connecting through Knox."
        ),
    )

    # ── TLS (direct connection) ───────────────────────────────────────────────
    use_ssl: bool          = Field(True,  description="Enable TLS for direct connections")
    ca_cert: Optional[str] = Field(None,  description="Path to CA certificate bundle (direct and Knox)")

    # ── Knox gateway (CDP Public Cloud load balancer) ─────────────────────────
    use_knox: bool = Field(
        False,
        description=(
            "Route the connection through the CDP Public Cloud load balancer / Knox gateway. "
            "When True, lb_host / lb_port / cluster_name drive the connection "
            "instead of host / port."
        ),
    )
    lb_host: Optional[str] = Field(
        None,
        description=(
            "Load balancer hostname. Required when use_knox=True. "
            "From CDP: Environment → Summary → Load Balancer URL."
        ),
    )
    lb_port: int = Field(
        443,
        description="Load balancer HTTPS port (default 443 in CDP Public Cloud).",
    )
    cluster_name: Optional[str] = Field(
        None,
        description=(
            "CDP DataHub cluster name. Used in the HTTP path. "
            "Impala:      httpPath = <cluster_name>/cdp-proxy-api/impala "
            "HiveServer2: httpPath = <cluster_name>/cdp-proxy-api/hive"
        ),
    )

    # ── Behaviour ─────────────────────────────────────────────────────────────
    timeout_seconds:       int = Field(30,  description="Query timeout in seconds")
    refresh_interval_secs: int = Field(300, description="Cache refresh interval in seconds (0 = never)")
    pool_size:             int = Field(3,   description="Number of connections in the pool")

    # ── Validation ────────────────────────────────────────────────────────────

    @model_validator(mode="after")
    def _validate_knox(self) -> "ImpalaSettings":
        if self.use_knox:
            if not self.lb_host:
                raise ValueError(
                    "IMPALA_LB_HOST must be set when IMPALA_USE_KNOX=true."
                )
            if not self.cluster_name:
                raise ValueError(
                    "IMPALA_CLUSTER_NAME must be set when IMPALA_USE_KNOX=true. "
                    "It is the CDP DataHub cluster name used in the Knox URL path."
                )
            if not (self.username and self.password):
                raise ValueError(
                    "IMPALA_USERNAME and IMPALA_PASSWORD are required for Knox auth."
                )
        return self

    # ── Derived properties ────────────────────────────────────────────────────

    @property
    def effective_host(self) -> str:
        """Connection host: load balancer or direct backend."""
        return self.lb_host if self.use_knox else self.host

    @property
    def effective_port(self) -> int:
        """Connection port: load balancer port or direct backend default."""
        if self.use_knox:
            return self.lb_port
        return self.port if self.port is not None else _DEFAULT_PORTS[self.backend]

    @property
    def effective_http_path(self) -> str:
        """
        HTTP path for impyla's use_http_transport mode.

        CDP Public Cloud (Knox):
          Impala:      <cluster_name>/cdp-proxy-api/impala
          HiveServer2: <cluster_name>/cdp-proxy-api/hive

        Direct HiveServer2 (no Knox):
          cliservice   (HS2 standard binary-Thrift path)

        Direct Impala (no Knox):
          empty string (binary Thrift, no HTTP path needed)

        Reference endpoints:
          Impala:  jdbc:impala://<lb>:<port>/;ssl=1;transportMode=http;
                   httpPath=<cluster>/cdp-proxy-api/impala;AuthMech=3
          Hive:    jdbc:hive2://<lb>:<port>/;ssl=true;transportMode=http;
                   httpPath=<cluster>/cdp-proxy-api/hive
        """
        if self.use_knox:
            service = _CDP_PROXY_SERVICE[self.backend]
            return f"{self.cluster_name}/cdp-proxy-api/{service}"
        if self.is_hiveserver2:
            return "cliservice"
        return ""

    @property
    def effective_use_http_transport(self) -> bool:
        """Knox always requires HTTP transport. Direct connections use binary Thrift."""
        return self.use_knox

    @property
    def effective_auth_mechanism(self) -> str:
        """
        impyla auth mechanisms: PLAIN, LDAP, GSSAPI.
        Knox connections use PLAIN over the HTTPS-secured channel.
        """
        return self.auth_mechanism

    @property
    def effective_use_ssl(self) -> bool:
        """TLS is always enforced through the CDP load balancer."""
        return True if self.use_knox else self.use_ssl

    @property
    def effective_krb_service(self) -> str:
        """Kerberos service name (only relevant for direct GSSAPI connections)."""
        return self.kerberos_service_name or _DEFAULT_KRB_SERVICE[self.backend]

    @property
    def full_table_name(self) -> str:
        """Fully qualified table name: database.table."""
        return f"{self.database}.{self.table}"

    @property
    def is_hiveserver2(self) -> bool:
        return self.backend == "hiveserver2"

    @property
    def is_impala(self) -> bool:
        return self.backend == "impala"

    model_config = {
        "env_file":   ".env",
        "env_prefix": "IMPALA_",
        "extra":      "ignore",
    }


# ─────────────────────────────────────────────────────────────────────────────
# SERVER SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

class ServerSettings(BaseSettings):
    """Global MCP server configuration."""

    server_name:    str   = Field("cloudera-manager-mcp")
    server_version: str   = Field("1.0.0")
    log_level:      str   = Field("INFO")

    # HTTP retry policy for CM API calls
    max_retries:         int   = Field(3)
    retry_wait_seconds:  float = Field(2.0)

    # Concurrency
    max_concurrent_requests: int = Field(10)
    requests_per_minute:     int = Field(60)

    # Log extraction limits
    max_log_lines:       int = Field(5000)
    max_log_range_hours: int = Field(48)

    # Metrics
    default_metric_points: int = Field(100)

    model_config = {
        "env_file": ".env",
        "env_prefix": "MCP_",
        "extra": "ignore",
    }