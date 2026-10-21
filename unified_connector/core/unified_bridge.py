"""Unified Bridge - Enterprise OT/IoT Connector Core.

Orchestrates data flow from protocol sources to Databricks:
    Protocol Clients → Tag Normalization → Backpressure → ZeroBus → Delta Tables

Features:
- Multi-protocol support (OPC-UA, MQTT, Modbus)
- Tag normalization (ISA-95 hierarchies)
- Per-source ZeroBus targets (different tables per source)
- Credential encryption
- Auto-reconnection with exponential backoff
- Backpressure handling with disk spooling
"""

import asyncio
import collections
import dataclasses
import logging
import math
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from unified_connector.protocols.base import ProtocolClient, ProtocolRecord, ProtocolType
from unified_connector.protocols.factory import create_protocol_client
from unified_connector.normalizer import get_normalization_manager
from unified_connector.core.backpressure import BackpressureManager
from unified_connector.core.zerobus_client import ZeroBusClient
from unified_connector.core.confirmed_writer import ConfirmedWriter, ZerobusRowStreams, convert_rows, take_from_backpressure
from unified_connector.core.wal import SEQ_FIELD, WriteAheadLog
from unified_connector.core import targets as source_tables
from unified_connector.sparkplug import raw_row
from unified_connector.sparkplug.edge_node_schemas import BirthNotifier, DatabricksSql, nbirth_procedures, quoted_table
from unified_connector.sparkplug.host import HostState, HostSupervisor
from unified_connector.core.numeric_compression import (
    CompressionPolicy,
    CompressionRule,
    NumericCompressionMode,
    SdtState,
    changed,
    is_numeric,
    meaningful_numeric_change,
    parse_policy_dict,
    parse_rules,
    resolve_effective_policy,
)

logger = logging.getLogger(__name__)

# _record_kind of a queued Sparkplug row (written with the sparkplug_raw columns)
SPARKPLUG_RAW = 'sparkplug_raw'
HEALTHY_SESSION_S = 60.0   # a source connection that lasted this long starts its reconnect backoff again


def _enabled(value: Any) -> bool:
    """A config switch that may come from the environment as a string."""
    return str(value).strip().lower() not in ('false', '0', 'no', 'off', '', 'none')


class UnifiedBridge:
    """
    Unified bridge orchestrating OT/IoT data ingestion to Databricks.

    Architecture:
        Protocol Sources → Tag Normalization → Backpressure Queue → ZeroBus Batching → Delta Tables

    Key Features:
        - Per-source ZeroBus targets (route different sources to different tables)
        - Tag normalization with ISA-95/Purdue Model hierarchies
        - Credential encryption at rest
        - Automatic reconnection with exponential backoff
        - Backpressure with disk spooling for resilience
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize unified bridge.

        Args:
            config: Configuration dictionary from config.yaml
        """
        self.config = config
        self.sources = config.get('sources', [])
        self._source_cfg_by_name: Dict[str, Dict[str, Any]] = {}
        # each source's own numeric_compression, parsed once per change of the sources (not per record)
        self._source_compression: Dict[str, Tuple[Optional[CompressionPolicy], List[CompressionRule]]] = {}
        self._refresh_source_lookup()

        # ZeroBus configuration
        self.zerobus_config = config.get('zerobus', {})
        # Config flag (what user configured)
        self.zerobus_config_enabled = self.zerobus_config.get('enabled', False)
        # Runtime flag (whether client(s)/batchers are running)
        self.zerobus_enabled = False

        # Tag normalization
        self.normalization_config = config.get('normalization', {})
        self.normalization_enabled = self.normalization_config.get('enabled', True)

        # Initialize normalizer if enabled
        if self.normalization_enabled:
            self.norm_manager = get_normalization_manager()
            self.norm_manager.set_enabled(True)
            logger.info("Tag normalization enabled")
        else:
            self.norm_manager = None
            logger.info("Tag normalization disabled")

        # Backpressure: one queue per Zerobus target table (each the configured size, its disk spool in a
        # subdirectory per target), so each target's writer takes only its own sources' rows
        self.backpressure_config = config.get('backpressure', {})
        self.queues: Dict[str, BackpressureManager] = {}
        self._source_queues: Dict[str, BackpressureManager] = {}
        self.backpressure = self._queue(self._get_target_id(self.zerobus_config.get('default_target', {})))

        # ZeroBus clients (one per unique target)
        self.zerobus_clients: Dict[str, ZeroBusClient] = {}

        # Confirmed writes (rows leave the queue only once Zerobus acknowledged them: ConfirmedWriter, one per
        # target, own streams) for every table that gets Sparkplug rows, and with zerobus.confirmed_writes for
        # the others too; otherwise ZeroBusClient.send_batch as before (_confirmed)
        self.confirmed_writes = bool(self.zerobus_config.get('confirmed_writes', False))
        self.drain_timeout_s = float(self.zerobus_config.get('drain_timeout_seconds', 15.0))
        self.writers: Dict[str, ConfirmedWriter] = {}

        # zerobus.wal: a write-ahead log per confirmed-writes target (rows on disk before they are queued,
        # replayed after a crash; core/wal.py), off by default
        self.wal_config = self.zerobus_config.get('wal') or {}
        self.wal_enabled = _enabled(self.wal_config.get('enabled', False))
        self.wals: Dict[str, WriteAheadLog] = {}
        self.wal_tasks: Dict[str, asyncio.Task] = {}
        self._queue_wals: Dict[BackpressureManager, WriteAheadLog] = {}
        # rows the log replays, per target: the writer takes them before the queue (in order, none dropped)
        self._wal_replay: Dict[str, collections.deque] = {}
        # per target, across a Zerobus stop/start: the log sequences taken since the last acknowledgement and
        # the stopped writer's unacknowledged batch (the next writer sends it first)
        self._taken: Dict[str, List[int]] = {}
        self._unacked: Dict[str, List[str]] = {}

        # Sparkplug sources: the primary host's supervisor and the NBIRTH -> create_edge_node_schema caller
        self.host_supervisors: Dict[str, HostSupervisor] = {}
        self.host_tasks: Dict[str, asyncio.Task] = {}
        self.birth_notifiers: Dict[str, BirthNotifier] = {}
        self._notifier_writers: Dict[str, ConfirmedWriter] = {}

        # Protocol clients
        self.clients: Dict[str, ProtocolClient] = {}
        self.client_tasks: Dict[str, asyncio.Task] = {}

        # Batch processing tasks (one per ZeroBus target)
        self.batch_tasks: Dict[str, asyncio.Task] = {}

        # Batch settings
        self.batch_size = self.zerobus_config.get('batch', {}).get('max_records', 1000)
        self.batch_timeout_sec = self.zerobus_config.get('batch', {}).get('timeout_seconds', 5.0)

        # Numeric compression settings (global + per-source override support).
        self.numeric_compression_cfg = config.get("numeric_compression", {}) or {}
        self.global_compression_policy: CompressionPolicy = parse_policy_dict(self.numeric_compression_cfg)
        self.global_compression_rules: List[CompressionRule] = parse_rules(self.numeric_compression_cfg)
        self.last_emitted_by_key: Dict[str, Any] = {}
        self.sdt_state_by_key: Dict[str, SdtState] = {}

        # Reconnection settings
        self.reconnect_enabled = True
        self.reconnect_initial_delay_sec = 1.0
        self.reconnect_max_delay_sec = 300.0
        self.reconnect_multiplier = 2.0

        # Per-protocol-vendor pipeline stages - separate tracking for each protocol+vendor combination
        # This ensures OPC UA and MQTT samples don't compete for the same buffer slots
        from collections import deque, defaultdict
        self.vendor_pipelines = defaultdict(lambda: {
            'raw_protocol': deque(maxlen=3),
            'after_vendor_detection': deque(maxlen=3),
            'after_normalization': deque(maxlen=3),
            'zerobus_batch': deque(maxlen=3),
            'record_count': 0
        })

        # Metrics
        self.metrics = {
            'records_received': 0,
            'records_normalized': 0,
            'vendor_formats': {
                'kepware': 0,
                'sparkplug_b': 0,
                'honeywell': 0,
                'opcua': 0,
                'modbus': 0,
                'generic': 0,
                'unknown': 0
            },
            # Protocol-vendor breakdown: tracks vendor formats per source protocol
            'protocol_vendor_breakdown': {},
            'records_enqueued': 0,
            'records_dropped': 0,
            'batches_sent': 0,
            'reconnections': 0,
            'compression': {
                'mode_default': self.global_compression_policy.mode.value,
                'incoming': 0,
                'emitted': 0,
                'filtered_change_only': 0,
                'filtered_deadband': 0,
                'filtered_sdt': 0,
                'forced_sdt_max_interval': 0,
                'sdt_out_of_order_resets': 0,
            },
        }

        self._shutdown = False
        self.running = False
        self._starting = False
        self._start_lock = asyncio.Lock()

        logger.info(f"UnifiedBridge initialized: {len(self.sources)} sources configured")

    def _refresh_source_lookup(self) -> None:
        self._source_cfg_by_name = {
            str(s.get("name", "")): s
            for s in self.sources
            if isinstance(s, dict) and s.get("name")
        }
        self._source_compression = {}
        for name, source_cfg in self._source_cfg_by_name.items():
            src_comp_cfg = source_cfg.get("numeric_compression") or {}
            self._source_compression[name] = (parse_policy_dict(src_comp_cfg) if src_comp_cfg else None,
                                              parse_rules(src_comp_cfg) if src_comp_cfg else [])

    def _forget_compression_state(self, source_name: str) -> None:
        """A removed or changed source's tags start compression again (nothing kept from its old settings)."""
        prefix = self._compression_key(source_name, "")
        for states in (self.last_emitted_by_key, self.sdt_state_by_key):
            for key in [k for k in states if k.startswith(prefix)]:
                del states[key]



    async def start_infra(self) -> None:
        """Start bridge infrastructure (ZeroBus + batch processors) only.

        Does NOT automatically start all configured sources.
        Use start_source()/stop_source() to control sources individually.
        """
        async with self._start_lock:
            if self.running or self._starting:
                logger.info("UnifiedBridge already running/starting")
                return
            self._starting = True
            self._shutdown = False

        logger.info("Starting UnifiedBridge infrastructure...")

        # Initialize ZeroBus clients if enabled
        if self.zerobus_config_enabled:
            logger.info("ZeroBus enabled - initializing connections...")
            await self._initialize_zerobus_clients()
            self.zerobus_enabled = True
        else:
            logger.info("ZeroBus disabled - data collection only mode")

        # Start batch processors if ZeroBus enabled
        if self.zerobus_enabled:
            for target_id in self.zerobus_clients.keys():
                if target_id in self.batch_tasks and not self.batch_tasks[target_id].done():
                    continue
                task = asyncio.create_task(self._batch_processor(target_id))
                self.batch_tasks[target_id] = task
                logger.info(f"Batch processor started for target: {target_id}")

        self.running = True
        self._starting = False
        logger.info("✓ UnifiedBridge infrastructure started")

    async def start(self):
        """Start all components (infra + all enabled sources)."""
        await self.start_infra()

        # Start protocol clients (enabled sources)
        for source_config in self.sources:
            source_name = source_config.get('name', 'unknown')
            if not source_config.get('enabled', True):
                logger.info(f"Source '{source_name}' is disabled, skipping")
                continue

            if source_name in self.clients:
                continue

            try:
                await self._start_client(source_config)
                logger.info(f"✓ Started source: {source_name}")
            except Exception as e:
                logger.error(f"Failed to start source {source_name}: {e}")

        logger.info(f"✓ UnifiedBridge started with {len(self.clients)} active sources")

    async def _initialize_zerobus_clients(self):
        """Initialize ZeroBus clients for each unique target."""
        self._source_queues.clear()   # the targets may have changed
        # Get default target
        default_target = self.zerobus_config.get('default_target', {})

        # Get per-source targets
        source_targets = self.zerobus_config.get('source_targets', {})

        # Build unique targets
        targets = {}

        # Add default target
        if default_target.get('workspace_host'):
            target_id = self._get_target_id(default_target)
            targets[target_id] = default_target

        # Add per-source targets
        for source_name, target_config in source_targets.items():
            # Merge with default config
            merged_target = {**default_target, **target_config}
            target_id = self._get_target_id(merged_target)
            targets[target_id] = merged_target

        # The enabled sources' own target tables (a disabled source's table is made when it is enabled and
        # started: _ensure_target; until then nothing is opened for it, e.g. the example config's sources)
        for source in self.sources:
            if not source.get('enabled', True):
                continue
            try:
                target_config = source_tables.target_config(source, self.zerobus_config)
            except ValueError as e:
                logger.error(f"[{source.get('name')}] {e}")
                continue
            if target_config.get('workspace_host'):
                targets[self._get_target_id(target_config)] = target_config

        # Create ZeroBus clients for each target
        for target_id, target_config in targets.items():
            await self._create_target(target_id, target_config)
        self._reattach_notifiers()   # Zerobus started again: the running Sparkplug sources' NBIRTH callers

    def _confirmed(self, target_id: str) -> bool:
        """Confirmed writes for this table: it gets Sparkplug rows (they need them: the primary host, the NBIRTH
        call), or zerobus.confirmed_writes is on. Disabled sources don't count."""
        return self.confirmed_writes or any(
            source_tables.is_sparkplug(s) and s.get('enabled', True) and self._get_target_for_source(s.get('name')) == target_id
            for s in self.sources)

    async def _create_target(self, target_id: str, target_config: Dict[str, Any]) -> None:
        """The table's ZeroBus client, and its ConfirmedWriter (with its write-ahead log) if _confirmed."""
        try:
            # Build ZeroBus config for this target
            zb_config = {
                'zerobus': {
                    'enabled': True,
                    'workspace_host': target_config['workspace_host'],
                    'zerobus_endpoint': self.zerobus_config.get('zerobus_endpoint'),
                    'target': {
                        'catalog': target_config.get('catalog', 'main'),
                        'schema': target_config.get('schema', 'iot_data'),
                        'table': target_config.get('table', 'sensor_readings'),
                    },
                    'auth': self.zerobus_config.get('auth', {}),
                    'batch': self.zerobus_config.get('batch', {}),
                }
            }

            client = ZeroBusClient(zb_config)
            if self._confirmed(target_id):
                streams = ZerobusRowStreams(client.zerobus_endpoint, client.workspace_url, client.full_table_name,
                                            client.client_id, client.client_secret)
                await self._open_wal(target_id)
                self.writers[target_id] = self._make_writer(target_id, streams.open)
            else:
                await client.connect()

            self.zerobus_clients[target_id] = client
            logger.info(f"✓ ZeroBus client connected: {target_id}" + (" (confirmed writes)" if target_id in self.writers else ""))

        except Exception as e:
            logger.error(f"Failed to initialize ZeroBus client for {target_id}: {e}")

    async def _ensure_target(self, source_config: Dict[str, Any]) -> None:
        """While Zerobus runs: the table of a source being started (a target_table set from the page is new),
        or confirmed writes for a table on the original path that now gets Sparkplug rows (no other source
        writes there: mixed_tables refuses that, so its queue is idle)."""
        if not self.zerobus_enabled:
            return
        target_config = source_tables.target_config(source_config, self.zerobus_config)
        if not target_config.get('workspace_host'):
            return
        target_id = self._get_target_id(target_config)
        if target_id in self.zerobus_clients:
            if target_id in self.writers or not self._confirmed(target_id):
                return
            task = self.batch_tasks.pop(target_id, None)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            old = self.zerobus_clients.pop(target_id)
            try:
                await old.close()
            except Exception as e:  # noqa: BLE001 - the original path's stream may not close cleanly
                logger.debug(f"closing the ZeroBus client of {target_id}: {e}")
            logger.info(f"{target_id}: confirmed writes from now on (it gets Sparkplug rows)")
        self._source_queues.clear()
        await self._create_target(target_id, target_config)
        if target_id in self.zerobus_clients:
            self.batch_tasks[target_id] = asyncio.create_task(self._batch_processor(target_id))

    def target_conflicts(self, sources: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """Tables that would get Sparkplug rows and other rows (core/targets.py), for these sources (default:
        the configured ones); a source with a target_table that isn't catalog.schema.table counts as an error."""
        problems = []
        resolvable = []
        for s in self.sources if sources is None else sources:
            try:
                source_tables.target_config(s, self.zerobus_config)
                resolvable.append(s)
            except ValueError as e:
                problems.append(f"{s.get('name')}: {e}")
        return problems + source_tables.mixed_tables(resolvable, self._table_of)

    def _table_of(self, source: Dict[str, Any]):
        """(target id, catalog.schema.table) of a source; ValueError for a malformed target_table."""
        target_config = source_tables.target_config(source, self.zerobus_config)
        return self._get_target_id(target_config), source_tables.table_name(target_config)

    async def _open_wal(self, target_id: str) -> None:
        """The target's write-ahead log (once per process: Zerobus may be stopped and started again), its rows
        not acknowledged before the last stop written first."""
        if not self.wal_enabled or target_id in self.wals:
            return
        cfg = self.wal_config
        wal = WriteAheadLog(Path(cfg.get('path', 'wal')) / self._target_dir(target_id),
                            fsync_interval_s=float(cfg.get('fsync_interval_seconds', 0.2)),
                            segment_bytes=int(float(cfg.get('segment_mb', 16)) * (1 << 20)),
                            max_bytes=int(float(cfg.get('max_size_mb', 1024)) * (1 << 20)), name=target_id)
        pending = wal.open()
        queue = self._queue(target_id)
        self.wals[target_id] = wal
        self._queue_wals[queue] = wal
        queue.on_drop = lambda record: wal.done([record[SEQ_FIELD]] if SEQ_FIELD in record else [])
        # Not through the bounded queue: nothing drains it yet, so rows beyond its size would be dropped (and
        # count as done) or refused. The writer takes them first, before any newer row (a pipeline's watermark
        # on inserted_at would drop them if newer rows were written before them).
        self._wal_replay[target_id] = collections.deque(pending)
        self.wal_tasks[target_id] = asyncio.create_task(wal.run())
        logger.info(f"Write-ahead log for {target_id}: {wal.directory} ({len(pending)} rows to write first)")

    def _make_writer(self, target_id: str, open_stream) -> ConfirmedWriter:
        """A ConfirmedWriter for one target, taking batches from the backpressure queue."""
        batch = self.zerobus_config.get('batch', {})
        bp = self._queue(target_id)
        wal = self.wals.get(target_id)
        taken = self._taken.setdefault(target_id, [])   # log sequences since the last acknowledgement (one batch in flight)

        replay = self._wal_replay.setdefault(target_id, collections.deque())

        def to_record(record: Dict[str, Any]) -> Dict[str, Any]:
            if SEQ_FIELD in record:
                taken.append(record[SEQ_FIELD])
            return self._to_zerobus_message(record)

        def bad_row(topic: Any, e: Exception) -> None:
            writer.skip_row(topic, e)

        async def take(n: int, linger: float, wait: float) -> List[Dict[str, Any]]:
            if replay:
                return convert_rows([replay.popleft() for _ in range(min(n, len(replay)))], to_record, bad_row)
            kept: List[Dict[str, Any]] = []   # this take's rows: back in front if it is cancelled part-way

            def keep(record: Dict[str, Any]) -> Dict[str, Any]:
                out = to_record(record)
                kept.append(record)
                return out
            try:
                return await take_from_backpressure(bp, keep, n, linger, wait, bad_row)
            except asyncio.CancelledError:
                for record in reversed(kept):
                    if SEQ_FIELD in record and record[SEQ_FIELD] in taken:
                        taken.remove(record[SEQ_FIELD])   # not sent: not done with the next acknowledgement
                    replay.appendleft(record)             # the next writer's first rows
                raise

        writer = ConfirmedWriter(
            take_batch=take,
            open_stream=open_stream,
            batch_size=int(batch.get('max_records', 500)),
            linger_s=float(batch.get('linger_seconds', 0.2)),
            queue_depth=lambda: len(replay) + bp.memory_queue.qsize() + len(bp.spool_files),
            queue_high_water=int(bp.max_queue_size * 0.8),
            name=target_id,
            batch=self._unacked.pop(target_id, None),
        )
        if wal is not None:
            def acknowledged(_batch: List[str]) -> None:
                # also covers rows of an earlier take skipped as too large (taken, never sent)
                wal.done(taken)
                taken.clear()
            writer.add_listener(acknowledged)
        return writer

    def _target_dir(self, target_id: str) -> str:
        """A target id as a directory name (spool, write-ahead log)."""
        return re.sub(r'[^A-Za-z0-9._-]', '_', target_id).lstrip('.') or 'default'

    def _queue(self, target_id: str) -> BackpressureManager:
        """The backpressure queue of one target (created on first use)."""
        queue = self.queues.get(target_id)
        if queue is None:
            spool = dict(self.backpressure_config.get('disk_spool', {}))
            base = Path(spool.get('path', 'spool'))
            spool['path'] = str(base / self._target_dir(target_id))
            spool.setdefault('encryption_key_file', str(base.parent / 'certs' / 'spool_encryption.key'))
            queue = self.queues[target_id] = BackpressureManager({**self.backpressure_config, 'disk_spool': spool})
        return queue

    def _queue_for_source(self, source_name: Optional[str]) -> BackpressureManager:
        """The queue of the target a source writes to (the default target's for an unknown source)."""
        queue = self._source_queues.get(source_name)
        if queue is None:
            queue = self._source_queues[source_name] = self._queue(self._get_target_for_source(source_name or ''))
        return queue

    def _get_target_id(self, target_config: Dict[str, Any]) -> str:
        """Generate unique ID for ZeroBus target.

        Args:
            target_config: Target configuration dict

        Returns:
            Unique target ID string
        """
        workspace = target_config.get('workspace_host', '')
        catalog = target_config.get('catalog', 'main')
        schema = target_config.get('schema', 'iot_data')
        table = target_config.get('table', 'sensor_readings')

        # Extract workspace ID from URL
        workspace_id = workspace.split('//')[1].split('.')[0] if '//' in workspace else workspace

        return f"{workspace_id}.{catalog}.{schema}.{table}"

    def _get_target_for_source(self, source_name: str) -> str:
        """Get ZeroBus target ID for a source.

        Args:
            source_name: Name of the source

        Returns:
            Target ID
        """
        # The source's target_table, else zerobus.source_targets.<name>, else the default (core/targets.py)
        source = next((s for s in self.sources if s.get('name') == source_name), {'name': source_name})
        try:
            return self._get_target_id(source_tables.target_config(source, self.zerobus_config))
        except ValueError:          # a target_table that isn't catalog.schema.table: refused at start
            return self._get_target_id(self.zerobus_config.get('default_target', {}))

    async def _start_client(self, source_config: Dict[str, Any]):
        """Start a single protocol client with reconnection loop.

        Args:
            source_config: Source configuration dict
        """
        source_name = source_config.get('name', 'unknown')
        protocol_type = source_config.get('protocol', 'opcua')
        key, table = self._table_of(source_config)   # ValueError: target_table malformed
        if source_tables.is_sparkplug(source_config):
            # its rows and other sources' rows in one table would block each other (core/targets.py)
            others = []
            for s in self.sources:
                try:
                    if not source_tables.is_sparkplug(s) and self._table_of(s)[0] == key:
                        others.append(str(s.get('name')))
                except ValueError:
                    pass
            if others:
                raise ValueError(f"not started: its table {table} also gets other rows ({', '.join(others)}); "
                                 "give it a target table of its own (target_table)")
        await self._ensure_target(source_config)

        # Add normalization flag to source config
        source_config['normalization_enabled'] = self.normalization_enabled

        # Create protocol client
        client = create_protocol_client(
            protocol_type=protocol_type,
            source_name=source_name,
            endpoint=source_config['endpoint'],
            config=source_config,
            on_record=lambda record: self._on_record(record, source_name),
            on_stats=self._on_stats
        )

        self.clients[source_name] = client
        if str(protocol_type).lower() in ('sparkplug', 'sparkplug_b'):   # as the factory: any case
            self._attach_sparkplug(source_name, source_config, client)

        # Start client with reconnection loop
        task = asyncio.create_task(self._client_lifecycle(source_name, client))
        self.client_tasks[source_name] = task

    def _attach_sparkplug(self, source_name: str, source_config: Dict[str, Any], client) -> None:
        """A Sparkplug source's primary host supervisor and NBIRTH caller, on its target's confirmed writer."""
        target_id = self._get_target_for_source(source_name)
        if client.primary_host_id:
            # follows the table's writer of the moment (a Sparkplug table always has confirmed writes): none
            # (Zerobus stopped or not started) is OFFLINE, so the edges buffer
            supervisor = HostSupervisor(client, lambda: self._writer_status(target_id),
                                        HostState(client.offline_after_seconds, client.online_after_seconds), name=source_name)
            self.host_supervisors[source_name] = supervisor
            self.host_tasks[source_name] = asyncio.create_task(supervisor.run())
        self._attach_notifier(source_name, source_config)

    def _writer_status(self, target_id: str):
        writer = self.writers.get(target_id)
        return None if writer is None else writer.status()

    def _reattach_notifiers(self) -> None:
        """After the writers were (re)created: each running Sparkplug source's NBIRTH caller on its target's new writer."""
        configs = {s.get('name'): s for s in self.sources}
        for name, client in self.clients.items():
            if client.protocol_type.value == 'sparkplug' and name in configs:
                try:
                    self._attach_notifier(name, configs[name])
                except Exception as e:  # noqa: BLE001 - one source must not stop Zerobus from starting
                    logger.error(f"[{name}] NBIRTH procedure calls not set up: {e}")

    def _attach_notifier(self, source_name: str, source_config: Dict[str, Any]) -> None:
        """The NBIRTH caller, listening on the target's current writer (moved there if it listened on an
        earlier one; created once a writer exists)."""
        schemas = source_config.get('edge_node_schemas')
        if schemas and _enabled(schemas.get('enabled', True)):
            target_id = self._get_target_for_source(source_name)
            writer = self.writers.get(target_id)
            notifier = self.birth_notifiers.get(source_name)
            if notifier is not None:
                old = self._notifier_writers.pop(source_name, None)
                if old is not None and old is not writer:
                    old.remove_listener(notifier.accept)
                if writer is not None:
                    if old is not writer:
                        writer.add_listener(notifier.accept)
                    self._notifier_writers[source_name] = writer
                return
            zerobus_client = self.zerobus_clients.get(target_id)
            if writer is None or zerobus_client is None:
                logger.warning(f"[{source_name}] no NBIRTH procedure calls until Zerobus runs")
                return
            try:
                procedures = nbirth_procedures(schemas)
                quoted_table(zerobus_client.full_table_name)
            except ValueError as e:                 # the source still writes its rows
                logger.error(f"[{source_name}] {e}; no NBIRTH procedure calls")
                return
            warehouse_id = str(schemas.get('warehouse_id') or '').strip()
            if not warehouse_id:
                logger.error(f"[{source_name}] edge_node_schemas needs warehouse_id (a SQL warehouse); no NBIRTH procedure calls")
                return
            sql = DatabricksSql(zerobus_client.workspace_url, zerobus_client.client_id, zerobus_client.client_secret,
                                warehouse_id)
            notifier = BirthNotifier(sql, zerobus_client.full_table_name, procedures,
                                     prefix=str(schemas.get('schema_prefix') or '').strip() or 'nb')
            writer.add_listener(notifier.accept)
            self.birth_notifiers[source_name] = notifier
            self._notifier_writers[source_name] = writer
            logger.info(f"[{source_name}] on NBIRTH: CALL {', '.join(procedures)}")

    async def _client_lifecycle(self, source_name: str, client: ProtocolClient):
        """Manage protocol client lifecycle with automatic reconnection.

        Args:
            source_name: Name of the source
            client: Protocol client instance
        """
        reconnect_delay = self.reconnect_initial_delay_sec

        while not self._shutdown:
            connected_at = None
            try:
                logger.info(f"[{source_name}] Connecting to {client.endpoint}...")
                await client.connect()
                logger.info(f"[{source_name}] ✓ Connected")
                connected_at = time.monotonic()

                # Start subscription/polling
                logger.info(f"[{source_name}] Starting data collection...")
                await client.subscribe()

                # Keep running until shutdown
                while not self._shutdown:
                    await asyncio.sleep(1)

                # Cleanup on shutdown
                await client.disconnect()
                reconnect_delay = self.reconnect_initial_delay_sec

                if self._shutdown:
                    break

            except Exception as e:
                logger.error(f"[{source_name}] Error: {e}")

                # Cleanup
                try:
                    await client.stop()
                    await client.disconnect()
                except Exception:
                    pass

                if not self.reconnect_enabled or self._shutdown:
                    break

                # Exponential backoff with jitter; a connection that lasted starts it again (one failing right
                # after each connect, e.g. a refused subscribe, backs off more each time)
                if connected_at is not None and time.monotonic() - connected_at >= HEALTHY_SESSION_S:
                    reconnect_delay = self.reconnect_initial_delay_sec
                jitter = reconnect_delay * 0.1 * (2 * (time.time() % 1) - 0.5)
                sleep_time = min(reconnect_delay + jitter, self.reconnect_max_delay_sec)

                logger.warning(f"[{source_name}] Reconnecting in {sleep_time:.1f}s...")
                self.metrics['reconnections'] += 1

                await asyncio.sleep(sleep_time)

                # Increase backoff
                reconnect_delay = min(
                    reconnect_delay * self.reconnect_multiplier,
                    self.reconnect_max_delay_sec
                )

        logger.info(f"[{source_name}] Client lifecycle ended")


    async def start_source(self, source_name: str) -> Dict[str, Any]:
        """Start a single configured source (does not modify config)."""
        if self._shutdown:
            return {'status': 'error', 'message': 'Bridge is shut down'}

        if source_name in self.clients:
            return {'status': 'ok', 'message': f"Source already active: {source_name}"}

        cfg = next((s for s in self.sources if s.get('name') == source_name), None)
        if not cfg:
            return {'status': 'error', 'message': f"Source not found: {source_name}"}

        if not cfg.get('enabled', True):
            return {'status': 'error', 'message': f"Source is disabled in config: {source_name}"}

        try:
            await self._start_client(cfg)
            logger.info(f"✓ Started source: {source_name}")
            return {'status': 'ok', 'message': f"Source started: {source_name}"}
        except Exception as e:
            logger.error(f"Failed to start source {source_name}: {e}", exc_info=True)
            return {'status': 'error', 'message': str(e)}

    async def stop_source(self, source_name: str) -> Dict[str, Any]:
        """Stop a single active source (does not delete it from config)."""
        if source_name not in self.clients:
            return {'status': 'ok', 'message': f"Source already stopped: {source_name}"}

        host_task = self.host_tasks.pop(source_name, None)
        if host_task is not None:
            host_task.cancel()
            await asyncio.gather(host_task, return_exceptions=True)
        self.host_supervisors.pop(source_name, None)
        client = self.clients.get(source_name)
        try:
            if client is not None:
                await client.stop()
                await client.disconnect()
        except Exception as e:
            logger.warning(f"Error stopping source {source_name}: {e}")
        # its NBIRTH caller off the writer, or a restarted source would add a second one
        notifier = self.birth_notifiers.pop(source_name, None)
        writer = self._notifier_writers.pop(source_name, None)
        if notifier is not None:
            if writer is not None:
                writer.remove_listener(notifier.accept)
            await notifier.stop()

        if source_name in self.client_tasks:
            task = self.client_tasks[source_name]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self.client_tasks.pop(source_name, None)

        self.clients.pop(source_name, None)
        return {'status': 'ok', 'message': f"Source stopped: {source_name}"}

    def _detect_vendor_format(self, record) -> str:
        """Detect vendor format from record characteristics.

        Args:
            record: Protocol record (ProtocolRecord object or dict)

        Returns:
            Detected format: 'kepware', 'sparkplug_b', 'honeywell', 'opcua', 'modbus', 'generic', or 'unknown'
        """
        # Handle both dict and ProtocolRecord
        if isinstance(record, dict):
            topic = record.get('topic_or_path', '') or ""
            metadata = record.get('metadata', {}) or {}
            protocol = record.get('protocol', '') or record.get('protocol_type', '')
        else:
            topic = getattr(record, 'topic_or_path', '') or ""
            metadata = getattr(record, 'metadata', {}) or {}
            protocol_type = getattr(record, 'protocol_type', None)
            protocol = protocol_type.value if hasattr(protocol_type, 'value') else str(protocol_type) if protocol_type else ''

        # Sparkplug B detection: SparkplugB namespace or bdSeq/seq metadata
        if "SparkplugB" in topic or "sparkplug" in topic.lower():
            return "sparkplug_b"
        if "bdSeq" in metadata or "seq" in metadata:
            return "sparkplug_b"
        if any(msgtype in topic for msgtype in ["NBIRTH", "DBIRTH", "NDATA", "DDATA", "NDEATH", "DDEATH"]):
            return "sparkplug_b"

        # Kepware detection: Check for kepware in path (supports both MQTT and OPC UA)
        if "kepware" in topic.lower():
            return "kepware"
        # Kepware detection: Channel.Device.Tag pattern (MQTT-based)
        if "Siemens_S7" in topic or "Allen_Bradley" in topic or "Modicon" in topic:
            return "kepware"
        # Kepware IoT Gateway format check
        if "." in topic and topic.count(".") >= 2:  # Channel.Device.Tag has at least 2 dots
            parts = topic.split(".")
            if len(parts) >= 3 and all(p.replace("_", "").replace("-", "").isalnum() for p in parts[:3]):
                return "kepware"

        # Honeywell detection: Check for honeywell in path (supports both MQTT and OPC UA)
        if "honeywell" in topic.lower():
            return "honeywell"
        if any(suffix in topic for suffix in [".PV", ".SP", ".OP", ".MODE", "/PV", "/SP", "/OP", "/MODE"]):
            return "honeywell"
        if "FIM" in topic or "AIM" in topic or "PID" in topic or "EXPERION" in topic:
            return "honeywell"

        # Protocol-based detection for native protocols (OPC UA, Modbus)
        if protocol and 'opcua' in protocol.lower():
            return "opcua"
        if protocol and 'modbus' in protocol.lower():
            return "modbus"

        # Generic format
        if "/" in topic:  # Simple path-based format
            return "generic"

        return "unknown"

    def _on_record(self, record: ProtocolRecord, source_name: Optional[str] = None):
        """Callback when protocol client receives a record.

        Args:
            record: Protocol record, or a Sparkplug ``raw_row.RawRow``
            source_name: the source it came from (picks the target's queue; else the record's own)
        """
        if source_name is None:
            source_name = record.get('source_name') if isinstance(record, dict) else getattr(record, 'source_name', None)
        queue = self._queue_for_source(source_name)
        if isinstance(record, raw_row.RawRow):
            self._on_raw_row(record, queue)
            return

        self.metrics['records_received'] += 1

        # Detect vendor format first
        vendor_format = self._detect_vendor_format(record)
        self.metrics['vendor_formats'][vendor_format] += 1

        # Track protocol-vendor breakdown
        # Extract protocol from record
        if isinstance(record, dict):
            protocol = record.get('protocol_type', record.get('protocol', 'unknown'))
        else:
            protocol_type = getattr(record, 'protocol_type', None)
            protocol = protocol_type.value if hasattr(protocol_type, 'value') else str(protocol_type) if protocol_type else 'unknown'

        # Initialize protocol entry if needed
        if protocol not in self.metrics['protocol_vendor_breakdown']:
            self.metrics['protocol_vendor_breakdown'][protocol] = {}

        # Initialize vendor entry for this protocol if needed
        if vendor_format not in self.metrics['protocol_vendor_breakdown'][protocol]:
            self.metrics['protocol_vendor_breakdown'][protocol][vendor_format] = 0

        # Increment protocol-vendor counter
        self.metrics['protocol_vendor_breakdown'][protocol][vendor_format] += 1

        # Create protocol-vendor key for separate buffering
        protocol_vendor_key = f"{protocol}_{vendor_format}"

        # Increment per-protocol-vendor record count
        self.vendor_pipelines[protocol_vendor_key]['record_count'] += 1

        # Stage 1: Capture raw protocol message (per-protocol-vendor)
        try:
            # Handle both ProtocolRecord objects and dict objects
            if isinstance(record, dict):
                raw_message = {
                    'timestamp': time.time(),
                    'source_name': record.get('source_name', 'unknown'),
                    'protocol': record.get('protocol', 'unknown'),
                    'topic_or_path': record.get('topic_or_path', ''),
                    'value': record.get('value'),
                    'quality': record.get('quality', 'UNKNOWN'),
                    'metadata': record.get('metadata', {}),
                    'vendor_format': vendor_format
                }
            else:
                raw_message = {
                    'timestamp': time.time(),
                    'source_name': getattr(record, 'source_name', 'unknown'),
                    'protocol': record.protocol_type.value if hasattr(record, 'protocol_type') else 'unknown',
                    'topic_or_path': getattr(record, 'topic_or_path', ''),
                    'value': getattr(record, 'value', None),
                    'quality': getattr(record, 'quality', 'UNKNOWN'),
                    'metadata': getattr(record, 'metadata', {}) or {},
                    'vendor_format': vendor_format
                }
            self.vendor_pipelines[protocol_vendor_key]['raw_protocol'].append(raw_message)
        except Exception as e:
            logger.error(f"[DEBUG] EXCEPTION in sample capture: {e}", exc_info=True)

        # Convert to dict using the record's to_dict method
        record_dict = record.to_dict()

        # Add vendor format and protocol to record for downstream processing
        record_dict['vendor_format'] = vendor_format
        record_dict['protocol'] = protocol  # Store protocol for zerobus_batch stage

        # Stage 2: Capture after vendor detection (per-protocol-vendor)
        after_vendor = record_dict.copy()
        after_vendor['timestamp'] = time.time()
        after_vendor['stage'] = 'after_vendor_detection'
        self.vendor_pipelines[protocol_vendor_key]['after_vendor_detection'].append(after_vendor)

        # Stage 3: Apply normalization if enabled
        after_norm = record_dict.copy()
        after_norm['timestamp'] = time.time()
        after_norm['stage'] = 'after_normalization'

        if self.normalization_enabled and self.norm_manager:
            try:
                normalizer = self.norm_manager.get_normalizer(record.protocol_type.value)
                if normalizer:
                    # Create raw data format expected by normalizer
                    raw_data = self._protocol_record_to_raw_data(record)
                    normalized_tag = normalizer.normalize(raw_data)

                    # Update record with normalized values
                    record_dict['tag_path'] = normalized_tag.tag_path
                    record_dict['tag_id'] = normalized_tag.tag_id
                    record_dict['data_type'] = normalized_tag.data_type.value
                    record_dict['quality'] = normalized_tag.quality.value

                    # Update after_norm sample with normalized values
                    after_norm.update({
                        'tag_path': normalized_tag.tag_path,
                        'tag_id': normalized_tag.tag_id,
                        'data_type': normalized_tag.data_type.value,
                        'quality': normalized_tag.quality.value,
                        'normalized': True
                    })

                    self.metrics['records_normalized'] += 1
                else:
                    after_norm['normalized'] = False
                    after_norm['normalized_reason'] = 'No normalizer for protocol'
            except Exception as e:
                logger.warning(f"Normalization failed: {e}")
                after_norm['normalized'] = False
                after_norm['normalized_error'] = str(e)
        else:
            after_norm['normalized'] = False
            after_norm['normalized_reason'] = 'Normalization disabled'

        # Stage 3: Capture after normalization (per-protocol-vendor)
        self.vendor_pipelines[protocol_vendor_key]['after_normalization'].append(after_norm)

        # Stage 3.5: Numeric compression/filtering (uses source event time; keyed by normalized path).
        compressed = self._apply_numeric_compression(record, record_dict)
        if compressed is None:
            return
        record_dict = compressed

        # Enqueue for ZeroBus
        self._enqueue_fast(record_dict, queue)

    def _on_raw_row(self, row: "raw_row.RawRow", queue: BackpressureManager) -> None:
        """A Sparkplug row goes to Zerobus unchanged: no vendor detection, no normalization."""
        self.metrics['records_received'] += 1
        self.metrics['vendor_formats']['sparkplug_b'] += 1
        breakdown = self.metrics['protocol_vendor_breakdown'].setdefault('sparkplug', {})
        breakdown['sparkplug_b'] = breakdown.get('sparkplug_b', 0) + 1
        pipeline = self.vendor_pipelines['sparkplug_sparkplug_b']
        pipeline['record_count'] += 1
        pipeline['raw_protocol'].append({'timestamp': time.time(), 'protocol': 'sparkplug', 'topic_or_path': row.msg_topic,
                                         'message_type': row.message_type, 'vendor_format': 'sparkplug_b'})
        self._enqueue_fast({'_record_kind': SPARKPLUG_RAW, 'vendor_format': 'sparkplug_b', 'protocol': 'sparkplug',
                            **row.to_zerobus_record()}, queue)

    def _enqueue_fast(self, record_dict: Dict[str, Any], queue: BackpressureManager) -> None:
        """Enqueue for ZeroBus without a task per record (one per record can starve the event loop)."""
        wal = self._queue_wals.get(queue)
        if wal is not None:
            seq = wal.append(record_dict)   # on disk before it is queued
            if seq is not None:
                record_dict[SEQ_FIELD] = seq
        try:
            queue.memory_queue.put_nowait(record_dict)
            queue.metrics['records_enqueued'] += 1
            queue.metrics['current_queue_depth'] = queue.memory_queue.qsize()
            self.metrics['records_enqueued'] += 1
        except asyncio.QueueFull:
            # Slow-path: fall back to async enqueue (disk spooling / drop policy)
            asyncio.create_task(self._enqueue_record(record_dict, queue))

    def _resolve_compression_policy_for_record(self, source_name: str, tag_path: str,
                                               raw_path: Optional[str] = None) -> CompressionPolicy:
        src_policy, src_rules = self._source_compression.get(source_name, (None, []))
        return resolve_effective_policy(
            self.global_compression_policy,
            self.global_compression_rules,
            src_policy,
            src_rules,
            tag_path,
            raw_path,
        )

    def _compression_key(self, source_name: str, path: str) -> str:
        # The source's own path (unique per tag), source-scoped to avoid accidental cross-source coupling.
        return f"{source_name}::{path}"

    def _apply_numeric_compression(self, record: ProtocolRecord, record_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source_name = str(record_dict.get("source_name") or record.source_name or "")
        raw_path = str(record_dict.get("topic_or_path") or "")
        tag_path = str(record_dict.get("tag_path") or raw_path)
        # Keyed by the source's own path, not tag_path: normalization may give several tags one tag_path (e.g.
        # its "<site>/<protocol>" fallback), and their readings must not be compared with each other.
        key = self._compression_key(source_name, raw_path or tag_path)
        event_time_ms = int(record.event_time_ms)
        policy = self._resolve_compression_policy_for_record(source_name, tag_path, raw_path)
        self.metrics['compression']['incoming'] += 1

        value = record_dict.get("value")
        last = self.last_emitted_by_key.get(key)

        # Non-numeric values, and NaN or infinity (no distance to measure), always use change-only semantics;
        # the tag's SDT starts again at its next finite number.
        if not is_numeric(value) or not math.isfinite(value):
            self.sdt_state_by_key.pop(key, None)
            if not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        # Numeric modes.
        if policy.mode == NumericCompressionMode.NONE:
            if policy.change_only and not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        if policy.mode == NumericCompressionMode.CHANGE_ONLY:
            if not changed(last, value):
                self.metrics['compression']['filtered_change_only'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        if policy.mode == NumericCompressionMode.DEADBAND:
            if not meaningful_numeric_change(last, value, policy.deadband):
                self.metrics['compression']['filtered_deadband'] += 1
                return None
            self.last_emitted_by_key[key] = value
            self.metrics['compression']['emitted'] += 1
            return record_dict

        # SDT mode.
        if policy.mode == NumericCompressionMode.SDT:
            # Defensive fallback for bad config values.
            if policy.sdt_deviation <= 0:
                if not changed(last, value):
                    self.metrics['compression']['filtered_change_only'] += 1
                    return None
                self.last_emitted_by_key[key] = value
                self.metrics['compression']['emitted'] += 1
                return record_dict

            state = self.sdt_state_by_key.setdefault(key, SdtState())
            out = state.offer(
                payload=record_dict,
                t_ms=event_time_ms,
                numeric_value=float(value),
                deviation=policy.sdt_deviation,
                max_interval_ms=max(0, int(policy.sdt_max_interval_ms)),
                min_interval_ms=max(0, int(policy.sdt_min_interval_ms)),
            )
            if out.filtered or out.emit_payload is None:
                self.metrics['compression']['filtered_sdt'] += 1
                return None

            if out.forced_by_max_interval:
                self.metrics['compression']['forced_sdt_max_interval'] += 1
            if out.reset_due_to_out_of_order:
                self.metrics['compression']['sdt_out_of_order_resets'] += 1

            emitted_payload = dict(out.emit_payload)
            emitted_value = emitted_payload.get("value")
            self.last_emitted_by_key[key] = emitted_value
            self.metrics['compression']['emitted'] += 1
            return emitted_payload

        # Unknown mode fallback: emit.
        self.last_emitted_by_key[key] = value
        self.metrics['compression']['emitted'] += 1
        return record_dict

    def _protocol_record_to_raw_data(self, record: ProtocolRecord) -> Dict[str, Any]:
        """Convert ProtocolRecord to raw data format for normalizers.

        Args:
            record: Protocol record

        Returns:
            Raw data dict
        """
        return {
            'value': record.value,
            'timestamp': record.event_time_ms / 1000.0,  # Convert ms to seconds
            'quality': record.status,  # ProtocolRecord has 'status', not 'quality'
            'tag_name': record.topic_or_path,  # Use topic_or_path as tag_name
            'tag_id': record.topic_or_path,  # Use topic_or_path as tag_id
            'source_name': record.source_name,
            'metadata': record.metadata or {},
        }

    async def _enqueue_record(self, record: Dict[str, Any], queue: BackpressureManager):
        """Enqueue record to backpressure queue.

        Args:
            record: Record dict
            queue: the target's queue
        """
        try:
            success = await queue.enqueue(record)
        except ValueError:          # drop_policy reject: queue and spool full
            queue.dropped(record)
            success = False
        except Exception as e:  # noqa: BLE001 - e.g. the spool's disk: dropped and done in the log, not lost in the task
            logger.error(f"Record dropped: the queue couldn't take it: {type(e).__name__}: {e}")
            queue.dropped(record)
            success = False
        if success:
            self.metrics['records_enqueued'] += 1
        else:
            self.metrics['records_dropped'] += 1

    def _on_stats(self, stats: Dict[str, Any]):
        """Callback for protocol client statistics.

        Args:
            stats: Statistics dict
        """
        logger.debug(f"Protocol stats: {stats}")

    async def _batch_processor(self, target_id: str):
        """Process records from backpressure queue in batches to ZeroBus.

        Args:
            target_id: ZeroBus target identifier
        """
        writer = self.writers.get(target_id)
        if writer is not None:
            logger.info(f"Confirmed writer started for {target_id}")
            await writer.run()
            logger.info(f"Confirmed writer stopped for {target_id}")
            return

        logger.info(f"Batch processor started for {target_id}")
        queue = self._queue(target_id)
        batch: List[Any] = []
        last_flush_time = time.time()

        zerobus_client = self.zerobus_clients.get(target_id)
        if not zerobus_client:
            logger.error(f"No ZeroBus client for target {target_id}")
            return

        while not self._shutdown:
            try:
                # Try to dequeue with timeout
                try:
                    record = await asyncio.wait_for(
                        queue.dequeue(),
                        timeout=1.0
                    )

                    if record is not None:
                        # Convert to ZeroBus message format
                        message = self._to_zerobus_message(record)
                        batch.append(message)

                        # Stage 4: Capture ZeroBus batch stage sample (per-protocol-vendor)
                        vendor_format = record.get('vendor_format', 'unknown')
                        protocol = record.get('protocol', 'unknown')
                        protocol_vendor_key = f"{protocol}_{vendor_format}"

                        zerobus_sample = message.copy()
                        zerobus_sample['timestamp'] = time.time()
                        zerobus_sample['stage'] = 'zerobus_batch'
                        self.vendor_pipelines[protocol_vendor_key]['zerobus_batch'].append(zerobus_sample)

                    else:
                        # Nothing to send; avoid a hot loop that starves the web UI
                        await asyncio.sleep(0.1)

                except asyncio.TimeoutError:
                    pass

                # Flush batch if size or timeout reached
                current_time = time.time()
                should_flush = (
                    len(batch) >= self.batch_size or
                    (len(batch) > 0 and (current_time - last_flush_time) >= self.batch_timeout_sec)
                )

                if should_flush:
                    logger.debug(f"Flushing batch of {len(batch)} records to {target_id}")
                    success = await zerobus_client.send_batch(batch)

                    if success:
                        self.metrics['batches_sent'] += 1
                        batch.clear()
                        last_flush_time = current_time
                    else:
                        logger.warning(f"ZeroBus send failed for {target_id}, will retry")
                        await asyncio.sleep(5.0)

            except Exception as e:
                logger.error(f"Batch processor error for {target_id}: {e}")
                await asyncio.sleep(1.0)

        # Flush remaining records on shutdown
        if batch:
            logger.info(f"Flushing final batch of {len(batch)} records to {target_id}")
            await zerobus_client.send_batch(batch)

        logger.info(f"Batch processor stopped for {target_id}")


    def _to_zerobus_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal record dict to the UC OPC-UA bronze schema (a Sparkplug row: the sparkplug_raw columns)."""
        import re

        if record.get('_record_kind') == SPARKPLUG_RAW:
            return {column: record[column] for column in raw_row.COLUMNS}

        metadata = record.get('metadata') or {}

        # event_time/ingest_time are already microseconds in ProtocolRecord.to_dict
        event_time = int(record.get('event_time') or int(time.time() * 1_000_000))
        ingest_time = int(record.get('ingest_time') or int(time.time() * 1_000_000))

        endpoint = record.get('endpoint', '')
        source_name = record.get('source_name', '')

        raw_node_id = metadata.get('node_id') or record.get('node_id') or ''

        namespace = 0
        node_id_out = str(raw_node_id)

        # Normalize asyncua NodeId(...) -> ns=X;i=Y when possible
        m = re.search(r"Identifier=(\d+),\s*NamespaceIndex=(\d+)", str(raw_node_id))
        if m:
            ident = int(m.group(1))
            namespace = int(m.group(2))
            node_id_out = f"ns={namespace};i={ident}"
        else:
            m2 = re.search(r"NamespaceIndex=(\d+)", str(raw_node_id))
            if m2:
                namespace = int(m2.group(1))

        browse_path = record.get('topic_or_path') or metadata.get('tag_name') or metadata.get('browse_path') or ''

        # Detect vendor format from browse_path/topic
        vendor_format = record.get('vendor_format', '')
        if not vendor_format:
            # Detect from browse_path if not already set
            topic_lower = str(browse_path).lower()
            protocol_type = record.get('protocol_type', '') or record.get('protocol', '')

            # Check for vendor-specific patterns in browse_path FIRST
            if "sparkplugb" in topic_lower or "sparkplug" in topic_lower:
                vendor_format = "sparkplug_b"
            elif "siemens_s7" in topic_lower or "allen_bradley" in topic_lower or "modicon" in topic_lower or "kepware" in topic_lower:
                vendor_format = "kepware"
            elif topic_lower.startswith("honeywell/") or "experion" in topic_lower or "pks" in topic_lower:
                vendor_format = "honeywell"
            elif "channel" in topic_lower and "device" in topic_lower:
                # Kepware pattern: Channel1.Device1.Tag
                vendor_format = "kepware"
            # Then check protocol type as fallback
            elif 'modbus' in str(protocol_type).lower():
                vendor_format = "modbus"
            elif 'mqtt' in str(protocol_type).lower() and "/" in str(browse_path):
                vendor_format = "mqtt_generic"
            elif 'opcua' in str(protocol_type).lower():
                # Native OPC UA (no vendor wrapper)
                vendor_format = "opcua_native"
            elif "/" in str(browse_path):
                vendor_format = "generic"
            else:
                vendor_format = "unknown"

        msg: Dict[str, Any] = {
            'event_time': event_time,
            'ingest_time': ingest_time,
            'source_name': source_name,
            'endpoint': endpoint,
            'namespace': int(namespace),
            'node_id': node_id_out,
            'browse_path': str(browse_path),
            'status_code': int(record.get('status_code') or 0),
            'status': str(record.get('status') or 'Good'),
            'value_type': str(record.get('value_type') or ''),
            'value': str(record.get('value', '')),
            'value_num': record.get('value_num'),
            # bytes field in JSON form is base64; empty is valid
            'raw': '',
            'plc_name': str(metadata.get('plc_name') or ''),
            'plc_vendor': str(metadata.get('plc_vendor') or ''),
            'plc_model': str(metadata.get('plc_model') or ''),
            'vendor_format': str(vendor_format),  # Vendor format (kepware, honeywell, sparkplug_b, etc.)
        }

        value_num = msg.get('value_num')
        if value_num is None or (isinstance(value_num, float) and not math.isfinite(value_num)):
            msg.pop('value_num', None)   # also NaN and infinity: JSON has none (the value column keeps it as text)

        return msg


    async def stop(self):
        """Stop all components gracefully."""
        logger.info("Stopping UnifiedBridge...")
        self._shutdown = True
        self._starting = False

        # Sparkplug primary hosts: no supervisor may go ONLINE again while stopping
        for task in self.host_tasks.values():
            task.cancel()
        if self.host_tasks:
            await asyncio.gather(*self.host_tasks.values(), return_exceptions=True)

        # Stop all protocol clients (a Sparkplug primary host publishes OFFLINE first, so the edges start buffering)
        for source_name, client in self.clients.items():
            try:
                logger.info(f"Stopping source: {source_name}")
                await client.stop()
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error stopping {source_name}: {e}")

        # Wait for client tasks (one waiting to go ONLINE is cancelled)
        if self.client_tasks:
            _, pending = await asyncio.wait(list(self.client_tasks.values()), timeout=5.0)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self.client_tasks.values(), return_exceptions=True)

        # Confirmed writers: write what is queued (up to drain_timeout_seconds), then stop
        for writer in self.writers.values():
            writer.finish(self.drain_timeout_s)
        if self.writers and self.batch_tasks:
            await asyncio.wait(list(self.batch_tasks.values()), timeout=self.drain_timeout_s + 5.0)

        # Stop batch processors
        if self.batch_tasks:
            for task in self.batch_tasks.values():
                task.cancel()
            await asyncio.gather(*self.batch_tasks.values(), return_exceptions=True)

        # Write-ahead logs: fsync and close (rows not acknowledged stay in them for the next start)
        for task in self.wal_tasks.values():
            task.cancel()
        await asyncio.gather(*self.wal_tasks.values(), return_exceptions=True)
        for wal in self.wals.values():
            wal.close()

        # Close ZeroBus clients
        for target_id, client in self.zerobus_clients.items():
            try:
                await client.close()
                logger.info(f"Closed ZeroBus client: {target_id}")
            except Exception as e:
                logger.error(f"Error closing ZeroBus client {target_id}: {e}")

        for notifier in self.birth_notifiers.values():
            await notifier.stop()

        logger.info("✓ UnifiedBridge stopped")

    def sparkplug_health(self) -> Dict[str, Dict[str, Any]]:
        """Per Sparkplug source: host state, MQTT, rows, Zerobus writer, NBIRTH calls, write-ahead log."""
        def iso(t):
            return None if t is None else time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + f".{int(t % 1 * 1000):03d}Z"
        out: Dict[str, Dict[str, Any]] = {}
        for name, client in self.clients.items():
            if client.protocol_type.value != 'sparkplug':
                continue
            s = client.stats()
            supervisor = self.host_supervisors.get(name)
            h: Dict[str, Any] = {
                'state': supervisor.state.mode if supervisor else ('ONLINE' if client.is_connected else 'OFFLINE'),
                'primary_host_id': client.primary_host_id, 'mqtt_connected': client.is_connected,
                'mqtt_online': client.is_online, 'state_topic': client.state_topic,
                'messages_received': s['messages_received'], 'rows_queued': s['rows'],
                'messages_skipped': sum(v for k, v in s.items() if k.startswith('skipped_')),
                'topics_malformed': s['skipped_malformed_topic'], 'payloads_undecodable': s['skipped_undecodable_payload'],
                'state_republished': s['state_republished'], 'rows_failed': s['rows_failed'],
            }
            writer = self.writers.get(self._get_target_for_source(name))
            if writer is not None:
                w = writer.status()
                h.update({'queue_depth': w.queue_depth, 'queue_high_water': w.queue_high_water,
                          'queue_dropped': self._queue_for_source(name).metrics.get('records_dropped', 0),
                          'zerobus_stream_open': w.stream_open, 'rows_written': w.written, 'rows_too_large': w.rows_too_large,
                          'rows_unconvertible': w.rows_unconvertible,
                          'zerobus_failures': w.failures, 'batch_in_flight': w.batch_in_flight,
                          'zerobus_failing_since': iso(w.failing_since), 'batch_pending_since': iso(w.pending_since),
                          'zerobus_last_success': iso(w.last_success), 'last_error': w.last_error})
            wal = self.wals.get(self._get_target_for_source(name))
            if wal is not None:
                st = wal.status()
                h.update({'wal_pending': st.pending, 'wal_rows_appended': st.rows_appended,
                          'wal_rows_replayed': st.rows_replayed, 'wal_rows_not_logged': st.rows_not_logged,
                          'wal_bytes': st.bytes, 'wal_failures': st.failures, 'wal_last_error': st.last_error})
            notifier = self.birth_notifiers.get(name)
            if notifier is not None:
                n = notifier.status()
                h.update({'nbirths_written': n.births, 'edge_node_schema_calls': n.calls,
                          'edge_node_schema_failures': n.failures, 'edge_node_schema_gave_up': n.gave_up,
                          'edge_node_schema_pending': n.pending, 'edge_node_schema_last_error': n.last_error})
            out[name] = h
        return out

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics from all components.

        Returns:
            Metrics dict
        """
        return {
            'bridge': self.metrics,
            'backpressure': self._backpressure_metrics(),
            'wal': {target_id: dataclasses.asdict(wal.status()) for target_id, wal in self.wals.items()},
            'zerobus_targets': {
                target_id: (dataclasses.asdict(self.writers[target_id].status()) if target_id in self.writers
                            else client.get_metrics())
                for target_id, client in self.zerobus_clients.items()
            },
            'clients': {
                name: {
                    'connected': client._client is not None if hasattr(client, '_client') else False,
                    'protocol': client.protocol_type.value,
                    'endpoint': client.endpoint,
                    **({'sparkplug': client.stats()} if client.protocol_type.value == 'sparkplug' else {}),
                }
                for name, client in self.clients.items()
            }
        }

    def _backpressure_metrics(self) -> Dict[str, Any]:
        """The queues' metrics summed (the keys of one queue), and per target under ``targets``."""
        per_target = {target_id: queue.get_metrics() for target_id, queue in self.queues.items()}
        keys = next(iter(per_target.values()), {}).keys()
        return {**{k: sum(m.get(k, 0) for m in per_target.values()) for k in keys}, 'targets': per_target}

    def get_pipeline_diagnostics(self) -> Dict[str, Any]:
        """Get message transformation pipeline diagnostics per vendor format.

        Returns:
            Pipeline diagnostics with per-vendor pipeline stages
        """
        vendor_pipelines_data = {}

        for vendor_name, pipeline in self.vendor_pipelines.items():
            vendor_pipelines_data[vendor_name] = {
                'vendor_name': vendor_name,
                'record_count': pipeline['record_count'],
                'pipeline_stages': [
                    {
                        'stage': 'raw_protocol',
                        'name': 'Raw Protocol',
                        'description': 'Messages as received from OPC UA/MQTT/Modbus',
                        'sample_count': len(pipeline['raw_protocol']),
                        'samples': list(pipeline['raw_protocol'])
                    },
                    {
                        'stage': 'after_vendor_detection',
                        'name': 'Vendor Detection',
                        'description': f'After detecting {vendor_name} format',
                        'sample_count': len(pipeline['after_vendor_detection']),
                        'samples': list(pipeline['after_vendor_detection'])
                    },
                    {
                        'stage': 'after_normalization',
                        'name': 'ISA-95 Normalization',
                        'description': 'After applying ISA-95 tag standardization',
                        'sample_count': len(pipeline['after_normalization']),
                        'samples': list(pipeline['after_normalization'])
                    },
                    {
                        'stage': 'zerobus_batch',
                        'name': 'ZeroBus Batch',
                        'description': 'Final format sent to Databricks Delta table',
                        'sample_count': len(pipeline['zerobus_batch']),
                        'samples': list(pipeline['zerobus_batch'])
                    }
                ]
            }

        return {
            'vendor_pipelines': vendor_pipelines_data,
            'vendor_format_summary': self.metrics['vendor_formats'],
            'protocol_vendor_breakdown': self.metrics['protocol_vendor_breakdown'],
            'total_records_received': self.metrics['records_received'],
            'normalization_enabled': self.normalization_enabled
        }

    async def add_source(self, source_name: str, source_type: str, source_config: Dict[str, Any]):
        """Add a new data source dynamically.

        Args:
            source_name: Unique name for the source
            source_type: Protocol type ('opcua', 'mqtt', 'modbus')
            source_config: Source configuration dict

        Raises:
            ValueError: If source already exists or config is invalid
        """
        if source_name in self.clients:
            raise ValueError(f"Source '{source_name}' already exists")

        logger.info(f"Adding new source: {source_name} (type: {source_type})")

        # Build full source config
        full_config = {
            'name': source_name,
            'protocol': source_type,
            'enabled': True,
            **source_config
        }

        # Add to internal sources list
        self.sources.append(full_config)
        self._refresh_source_lookup()

        # Start the client if bridge is running
        if self.running and not self._shutdown:
            try:
                await self._start_client(full_config)
                logger.info(f"✓ Source '{source_name}' added and started successfully")
            except Exception as e:
                # Remove from sources list if start failed
                self.sources.remove(full_config)
                self._refresh_source_lookup()
                logger.error(f"Failed to start source '{source_name}': {e}")
                raise ValueError(f"Failed to start source: {e}")
        else:
            logger.info(f"✓ Source '{source_name}' added (will start when bridge starts)")

    async def remove_source(self, source_name: str):
        """Remove an existing data source dynamically.

        Args:
            source_name: Name of the source to remove

        Raises:
            ValueError: If source doesn't exist
        """
        if source_name not in self.clients and not any(s.get('name') == source_name for s in self.sources):
            raise ValueError(f"Source '{source_name}' not found")

        logger.info(f"Removing source: {source_name}")

        # Stop it if it runs (a stopped source is removed from the configuration only)
        if source_name in self.clients:
            await self.stop_source(source_name)
        self._source_queues.pop(source_name, None)

        # Remove from sources list (in place: the web server lists the same list)
        self.sources[:] = [s for s in self.sources if s.get('name') != source_name]
        self._refresh_source_lookup()
        self._forget_compression_state(source_name)

        logger.info(f"✓ Source '{source_name}' removed successfully")

    async def update_source(self, source_name: str, source_config: Dict[str, Any]) -> None:
        """Replace a source's configuration; a running source is restarted with it."""
        index = next((i for i, s in enumerate(self.sources) if s.get('name') == source_name), None)
        if index is None:
            raise ValueError(f"Source '{source_name}' not found")
        was_active = source_name in self.clients
        if was_active:
            await self.stop_source(source_name)
        self._source_queues.pop(source_name, None)
        old = self.sources[index]
        self.sources[index] = source_config
        self._refresh_source_lookup()
        self._forget_compression_state(source_name)
        if was_active and source_config.get('enabled', True):
            try:
                await self._start_client(source_config)
            except Exception:
                # runs on as before (the page shows the error, nothing is saved)
                self.sources[index] = old
                self._refresh_source_lookup()
                self._source_queues.pop(source_name, None)
                if old.get('enabled', True):
                    await self._start_client(old)
                raise
        logger.info(f"✓ Source '{source_name}' updated" + (" and restarted" if was_active else ""))

    async def start_zerobus(self) -> Dict[str, Any]:
        """Start ZeroBus streaming dynamically.

        Returns:
            dict: Status message with success/error
        """
        try:
            if self.zerobus_enabled and self.zerobus_clients:
                return {'status': 'ok', 'message': 'ZeroBus already enabled'}

            logger.info("Starting ZeroBus streaming...")

            # Initialize ZeroBus clients
            await self._initialize_zerobus_clients()

            # Start batch processors
            for target_id in self.zerobus_clients.keys():
                task = asyncio.create_task(self._batch_processor(target_id))
                self.batch_tasks[target_id] = task
                logger.info(f"Batch processor started for target: {target_id}")

            self.zerobus_enabled = True

            logger.info("✓ ZeroBus streaming started successfully")
            return {'status': 'ok', 'message': 'ZeroBus streaming started successfully'}

        except Exception as e:
            logger.error(f"Failed to start ZeroBus: {e}", exc_info=True)
            return {'status': 'error', 'message': f'Failed to start ZeroBus: {str(e)}'}

    async def stop_zerobus(self) -> Dict[str, Any]:
        """Stop ZeroBus streaming dynamically.

        Returns:
            dict: Status message with success/error
        """
        try:
            if not self.zerobus_enabled:
                return {'status': 'ok', 'message': 'ZeroBus already disabled'}

            logger.info("Stopping ZeroBus streaming...")

            # Confirmed writers end after their current step (a take in progress completes; a batch not
            # acknowledged is kept for the next writer), then the batch processors are cancelled
            for writer in self.writers.values():
                writer.finish(0)
            if self.writers and self.batch_tasks:
                await asyncio.wait(list(self.batch_tasks.values()), timeout=5.0)

            # Stop batch processors
            if self.batch_tasks:
                for task in self.batch_tasks.values():
                    task.cancel()
                await asyncio.gather(*self.batch_tasks.values(), return_exceptions=True)
                self.batch_tasks.clear()
                logger.info("Batch processors stopped")

            # Close ZeroBus clients
            for target_id, client in self.zerobus_clients.items():
                try:
                    await client.close()
                    logger.info(f"Closed ZeroBus client: {target_id}")
                except Exception as e:
                    logger.error(f"Error closing ZeroBus client {target_id}: {e}")

            self.zerobus_clients.clear()
            for target_id, writer in self.writers.items():
                self._unacked[target_id] = writer.unacknowledged()   # sent first by the next writer
            for name, writer in list(self._notifier_writers.items()):
                writer.remove_listener(self.birth_notifiers[name].accept)
                del self._notifier_writers[name]
            self.writers.clear()   # the write-ahead logs stay open: rows are still queued and logged
            self.zerobus_enabled = False

            logger.info("✓ ZeroBus streaming stopped successfully")
            return {'status': 'ok', 'message': 'ZeroBus streaming stopped successfully'}

        except Exception as e:
            logger.error(f"Failed to stop ZeroBus: {e}", exc_info=True)
            return {'status': 'error', 'message': f'Failed to stop ZeroBus: {str(e)}'}

    def get_status(self) -> Dict[str, Any]:
        """Get current status of all components.

        Returns:
            Status dict
        """
        return {
            'configured_sources': len(self.sources),
            'active_sources': len(self.clients),
            'bridge_running': bool(getattr(self, 'running', False)),
            'bridge_starting': bool(getattr(self, '_starting', False)) and not bool(getattr(self, 'running', False)),
            'zerobus_config_enabled': self.zerobus_config_enabled,
            'zerobus_connected': bool(self.zerobus_clients),
            'zerobus_enabled': self.zerobus_enabled,
            'zerobus_targets': list(self.zerobus_clients.keys()),
            'normalization_enabled': self.normalization_enabled,
            'backpressure_queue_size': self._backpressure_metrics().get('queue_size', 0),
            'metrics': self.get_metrics(),
        }
