"""
cm_pool.py — Connection pool for multi-DataHub environments.

Maintains one ClouderaManagerClient per configured CM instance and resolves
which client to use given a cluster name, without callers needing to know
which CM manages which cluster.

Cluster map strategy:
  Knox / CDP Public Cloud (use_knox=True):
    GET /clusters is not available. The cluster name is taken directly from
    the registry field cluster_name — no API call needed.

  Direct connection (use_knox=False):
    GET /clusters is called to discover all clusters managed by the CM.
"""

from __future__ import annotations

from typing import Optional

import structlog

from cm_client import ClouderaManagerClient
from config import ClouderaManagerSettings, ServerSettings

log = structlog.get_logger(__name__)


class CMPool:
    """
    Manages a pool of ClouderaManagerClient instances, one per CM host.

    At startup the pool:
      1. Opens an HTTP connection for each configured CM.
      2. Builds the cluster_name -> client map:
         - For Knox entries: uses cluster_name from the registry config.
         - For direct entries: calls GET /clusters to discover them.

    Callers resolve the right client with get_client_for_cluster(cluster_name).
    """

    def __init__(
        self,
        cm_configs:  list[ClouderaManagerSettings],
        server_cfg:  ServerSettings,
    ) -> None:
        self._configs:     list[ClouderaManagerSettings]    = cm_configs
        self._server_cfg:  ServerSettings                   = server_cfg
        # effective_host -> client  (effective_host = lb_host for Knox, host for direct)
        self._clients:     dict[str, ClouderaManagerClient] = {}
        # cluster_name (lowercase) -> effective_host
        self._cluster_map: dict[str, str]                   = {}
        # effective_host -> ClouderaManagerSettings  (needed for reload logic)
        self._cfg_by_host: dict[str, ClouderaManagerSettings] = {}

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Connect all clients and build the initial cluster map."""
        if not self._configs:
            log.warning(
                "cm_pool.no_instances_configured",
                hint="Add CM entries via the registry_add tool or directly in the Iceberg table.",
            )
            return

        for cfg in self._configs:
            client = ClouderaManagerClient(cfg, self._server_cfg)
            await client.connect()
            key = cfg.effective_host
            self._clients[key]     = client
            self._cfg_by_host[key] = cfg
            log.info(
                "cm_pool.client_ready",
                host=cfg.effective_host,
                env=cfg.environment_name,
                mode="knox" if cfg.use_knox else "direct",
            )

        await self._build_cluster_map()

    async def stop(self) -> None:
        """Close all open HTTP connections."""
        for host, client in self._clients.items():
            await client.close()
            log.info("cm_pool.client_closed", host=host)

    # ── Cluster map ──────────────────────────────────────────────────────────

    async def _build_cluster_map(self) -> None:
        """
        Populate cluster_name -> effective_host.

        Knox (CDP Public Cloud):
          GET /clusters is not available. The cluster name is known from
          the registry — use it directly without any API call.

        Direct:
          Call GET /clusters to discover all clusters managed by this CM.
        """
        for effective_host, client in self._clients.items():
            cfg = self._cfg_by_host[effective_host]

            if cfg.use_knox:
                # Cluster name is pre-configured in the registry — no API call.
                if cfg.cluster_name:
                    key = cfg.cluster_name.lower()
                    self._cluster_map[key] = effective_host
                    log.info(
                        "cm_pool.cluster_mapped_from_registry",
                        cluster=cfg.cluster_name,
                        host=effective_host,
                    )
                else:
                    log.warning(
                        "cm_pool.knox_entry_missing_cluster_name",
                        host=effective_host,
                        hint="Set cluster_name in the registry for this CM entry.",
                    )
            else:
                # Direct connection — discover clusters via GET /clusters.
                try:
                    clusters = await client.list_clusters()
                    for cluster in clusters:
                        name = cluster.get("name", "")
                        if name:
                            self._cluster_map[name.lower()] = effective_host
                            log.debug(
                                "cm_pool.cluster_mapped_from_api",
                                cluster=name,
                                host=effective_host,
                            )
                except Exception as exc:
                    log.error(
                        "cm_pool.cluster_discovery_failed",
                        host=effective_host,
                        error=str(exc),
                    )

    # ── Client resolution ────────────────────────────────────────────────────

    def get_client_for_cluster(self, cluster_name: str) -> ClouderaManagerClient:
        """
        Return the client that manages the given cluster.
        Raises ValueError if the cluster is not in the map.
        """
        host = self._cluster_map.get(cluster_name.lower())
        if not host:
            available = sorted(self._cluster_map.keys())
            raise ValueError(
                f"Cluster '{cluster_name}' not found in the pool. "
                f"Known clusters: {available}"
            )
        return self._clients[host]

    def get_client_for_host(self, cm_host: str) -> ClouderaManagerClient:
        """Return the client for a specific effective host."""
        client = self._clients.get(cm_host)
        if not client:
            raise ValueError(f"No client configured for host: {cm_host}")
        return client

    def get_any_client(self) -> ClouderaManagerClient:
        """Return an arbitrary client — useful for environment-wide operations."""
        if not self._clients:
            raise RuntimeError("Pool is empty. No CM instances are configured.")
        return next(iter(self._clients.values()))

    def all_clients(self) -> list[ClouderaManagerClient]:
        """Return all active clients."""
        return list(self._clients.values())

    # ── Refresh ──────────────────────────────────────────────────────────────

    async def refresh_cluster_map(self) -> None:
        """
        Rebuild the cluster -> host map.
        Call this after provisioning new DataHubs or modifying the CDP environment.
        """
        self._cluster_map.clear()
        await self._build_cluster_map()
        log.info("cm_pool.cluster_map_refreshed", total=len(self._cluster_map))

    async def reload(self, new_instances: list[ClouderaManagerSettings]) -> None:
        """
        Hot-reload the pool with a new list of CM instances.
        Closes stale clients, opens new ones, rebuilds the cluster map.
        """
        new_hosts = {cfg.effective_host for cfg in new_instances}
        old_hosts = set(self._clients.keys())

        # Close clients for hosts no longer in the registry
        for host in old_hosts - new_hosts:
            await self._clients[host].close()
            del self._clients[host]
            self._cfg_by_host.pop(host, None)
            log.info("cm_pool.client_removed", host=host)

        # Open clients for newly registered hosts
        for cfg in new_instances:
            key = cfg.effective_host
            if key not in self._clients:
                client = ClouderaManagerClient(cfg, self._server_cfg)
                await client.connect()
                self._clients[key]     = client
                self._cfg_by_host[key] = cfg
                log.info("cm_pool.client_added", host=key)

        # Rebuild the lookup table
        self._cluster_map.clear()
        await self._build_cluster_map()
        log.info("cm_pool.reloaded", total_clients=len(self._clients))