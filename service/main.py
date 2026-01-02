import asyncio
import json
import logging
import os
import signal
import sqlite3
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from aiohttp import web
from bleak import BleakClient, BleakScanner

AUTHENTICATION_SERVICE_UUID = "64ac0000-4a4b-4b58-9f37-94d3c52ffdf7"
APP_CHALLENGE_UUID = "64ac0002-4a4b-4b58-9f37-94d3c52ffdf7"
DEVICE_CHALLENGE_UUID = "64ac0003-4a4b-4b58-9f37-94d3c52ffdf7"
DEVICE_RESPONSE_UUID = "64ac0004-4a4b-4b58-9f37-94d3c52ffdf7"

IGRILL_MINI_TEMPERATURE_SERVICE_UUID = "63c70000-4a82-4261-95ff-92cf32477861"
IDEVICES_KITCHEN_TEMPERATURE_SERVICE_UUID = "19450000-9b05-40bb-80d8-7c85840aec34"
IGRILL_MINIV2_TEMPERATURE_SERVICE_UUID = "9d610c43-ae1d-41a9-9b09-3c7ecd5c6035"
IGRILLV2_TEMPERATURE_SERVICE_UUID = "a5c50000-f186-4bd6-97f2-7ebacba0d708"
IGRILLV202_TEMPERATURE_SERVICE_UUID = "ada7590f-2e6d-469e-8f7b-1822b386a5e9"
IGRILLV3_TEMPERATURE_SERVICE_UUID = "6e910000-58dc-41c7-943f-518b278cea88"
PULSE_1000_TEMPERATURE_SERVICE_UUID = "7e920000-68dc-41c7-943f-518b278cea87"
PULSE_2000_TEMPERATURE_SERVICE_UUID = "7e920000-68dc-41c7-943f-518b278cea88"
PULSE_ELEMENT_SERVICE_UUID = "6c910000-58dc-41c7-943f-518b278ceaaa"

TEMPERATURE_UNIT_UUID = "06ef0001-2e06-4b79-9e33-fce2c42805ec"
PROBE_TEMPERATURE_UUIDS = [
    "06ef0002-2e06-4b79-9e33-fce2c42805ec",
    "06ef0004-2e06-4b79-9e33-fce2c42805ec",
    "06ef0006-2e06-4b79-9e33-fce2c42805ec",
    "06ef0008-2e06-4b79-9e33-fce2c42805ec",
]
PULSE_ELEMENT_UUID = "6c91000a-58dc-41c7-943f-518b278ceaaa"

PROPANE_LEVEL_SERVICE_UUID = "f5d40000-3548-4c22-9947-f3673fce3cd9"
PROPANE_LEVEL_UUID = "f5d40001-3548-4c22-9947-f3673fce3cd9"

BATTERY_SERVICE_UUID = "180f"
BATTERY_LEVEL_UUID = "2a19"

UNPLUGGED_PROBE_CONSTANT = 63536

DEFAULT_PORT = 39120
DEFAULT_POLL_INTERVAL = 15
DEFAULT_TIMEOUT = 30
MIN_POLL_INTERVAL = 5
MAX_POLL_INTERVAL = 60
DEFAULT_SCAN_INTERVAL = 60
DEFAULT_SCAN_TIMEOUT = 5
DEFAULT_RECONNECT_GRACE = 60
DEFAULT_DB_PATH = "/data/igrill.db"

LOG = logging.getLogger("igrill")


@dataclass(frozen=True)
class ModelInfo:
    model_id: str
    label: str
    service_uuid: str
    probe_count: int
    is_pulse: bool = False


MODELS: List[ModelInfo] = [
    ModelInfo("igrill_mini", "IGrill mini", IGRILL_MINI_TEMPERATURE_SERVICE_UUID, 1),
    ModelInfo("igrill_miniv2", "IGrill mini V2", IGRILL_MINIV2_TEMPERATURE_SERVICE_UUID, 1),
    ModelInfo("igrill_v2", "IGrill V2", IGRILLV2_TEMPERATURE_SERVICE_UUID, 4),
    ModelInfo("igrill_v202", "IGrill V202", IGRILLV202_TEMPERATURE_SERVICE_UUID, 4),
    ModelInfo("igrill_v3", "IGrill V3", IGRILLV3_TEMPERATURE_SERVICE_UUID, 4),
    ModelInfo("idevices_kitchen", "iDevices Kitchen", IDEVICES_KITCHEN_TEMPERATURE_SERVICE_UUID, 2),
    ModelInfo("pulse_1000", "Pulse 1000", PULSE_1000_TEMPERATURE_SERVICE_UUID, 2, True),
    ModelInfo("pulse_2000", "Pulse 2000", PULSE_2000_TEMPERATURE_SERVICE_UUID, 4, True),
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(timestamp: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(timestamp)
    except (TypeError, ValueError):
        return None


def clamp_int(value: int, min_value: int, max_value: int) -> int:
    return max(min_value, min(max_value, value))


def read_int_env(name: str, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        LOG.warning("Invalid %s value %r, using %d", name, raw, default)
        return default
    if min_value is not None and max_value is not None:
        return clamp_int(value, min_value, max_value)
    return value


class DeviceStore:
    def __init__(self) -> None:
        self._devices: Dict[str, Dict[str, object]] = {}
        self._lock = asyncio.Lock()
        self._reading_queue: asyncio.Queue[Dict[str, object]] = asyncio.Queue(maxsize=1000)

    async def upsert(self, address: str, **fields: object) -> None:
        async with self._lock:
            entry = self._devices.setdefault(
                address,
                {
                    "address": address,
                    "name": None,
                    "model": None,
                    "model_name": None,
                    "connected": False,
                    "session_id": None,
                    "last_seen": None,
                    "last_update": None,
                    "unit": None,
                    "battery_percent": None,
                    "propane_percent": None,
                    "probes": [],
                    "pulse": {},
                    "connected_probes": [],
                    "probe_status": "unknown",
                    "error": None,
                    "rssi": None,
                },
            )
            entry.update(fields)

    async def snapshot(self) -> Dict[str, Dict[str, object]]:
        async with self._lock:
            return {key: dict(value) for key, value in self._devices.items()}

    async def get_device(self, address: str) -> Optional[Dict[str, object]]:
        async with self._lock:
            if address not in self._devices:
                return None
            return dict(self._devices[address])

    async def publish_reading(self, reading: Dict[str, object]) -> None:
        if self._reading_queue.full():
            try:
                self._reading_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self._reading_queue.put_nowait(reading)

    async def next_reading(self) -> Dict[str, object]:
        return await self._reading_queue.get()


class HistoryStore:
    def __init__(self, db_path: str, reconnect_grace: int) -> None:
        self._db_path = db_path
        self._reconnect_grace = reconnect_grace
        self._lock = asyncio.Lock()
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT NOT NULL,
                name TEXT,
                model TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                seq INTEGER,
                unit TEXT,
                battery_percent REAL,
                propane_percent REAL,
                pulse_json TEXT,
                probes_json TEXT,
                data_json TEXT,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_address ON sessions(address)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_session ON readings(session_id)")
        self._ensure_column("readings", "seq", "seq INTEGER")
        self._ensure_column("readings", "data_json", "data_json TEXT")
        self._conn.commit()

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {
            row["name"]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    async def ensure_session(
        self,
        address: str,
        name: Optional[str],
        model: Optional[str],
        now_value: str,
    ) -> int:
        now_dt = parse_iso(now_value) or datetime.now().astimezone()
        async with self._lock:
            row = self._conn.execute(
                "SELECT id, ended_at FROM sessions WHERE address = ? ORDER BY started_at DESC LIMIT 1",
                (address,),
            ).fetchone()
            session_id = None
            if row:
                ended_at = row["ended_at"]
                if ended_at is None:
                    last_read = self._conn.execute(
                        "SELECT recorded_at FROM readings WHERE session_id = ? ORDER BY recorded_at DESC LIMIT 1",
                        (row["id"],),
                    ).fetchone()
                    last_read_dt = parse_iso(last_read["recorded_at"]) if last_read else None
                    if last_read_dt and (now_dt - last_read_dt).total_seconds() > self._reconnect_grace:
                        self._conn.execute(
                            "UPDATE sessions SET ended_at = ? WHERE id = ?",
                            (last_read["recorded_at"], row["id"]),
                        )
                    else:
                        session_id = row["id"]
                else:
                    ended_dt = parse_iso(ended_at)
                    if ended_dt and (now_dt - ended_dt).total_seconds() <= self._reconnect_grace:
                        self._conn.execute("UPDATE sessions SET ended_at = NULL WHERE id = ?", (row["id"],))
                        session_id = row["id"]

            if session_id is None:
                self._conn.execute(
                    "INSERT INTO sessions (address, name, model, started_at) VALUES (?, ?, ?, ?)",
                    (address, name, model, now_value),
                )
                session_id = self._conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            else:
                self._conn.execute(
                    "UPDATE sessions SET name = ?, model = ? WHERE id = ?",
                    (name, model, session_id),
                )
            self._conn.commit()
            return int(session_id)

    async def close_session(self, address: str, ended_at: str) -> None:
        async with self._lock:
            self._conn.execute(
                """
                UPDATE sessions
                SET ended_at = ?
                WHERE id = (
                    SELECT id FROM sessions
                    WHERE address = ? AND ended_at IS NULL
                    ORDER BY started_at DESC
                    LIMIT 1
                )
                """,
                (ended_at, address),
            )
            self._conn.commit()

    async def record_reading(
        self,
        session_id: int,
        address: str,
        payload: Dict[str, object],
        reading_data: Dict[str, object],
        seq: int,
    ) -> None:
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO readings (
                    session_id,
                    address,
                    recorded_at,
                    seq,
                    unit,
                    battery_percent,
                    propane_percent,
                    pulse_json,
                    probes_json,
                    data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    address,
                    payload.get("last_update"),
                    seq,
                    payload.get("unit"),
                    payload.get("battery_percent"),
                    payload.get("propane_percent"),
                    json.dumps(payload.get("pulse", {})),
                    json.dumps(payload.get("probes", [])),
                    json.dumps(reading_data),
                ),
            )
            self._conn.commit()

    async def get_history(self, address: Optional[str] = None) -> List[Dict[str, object]]:
        async with self._lock:
            if address:
                session_rows = self._conn.execute(
                    "SELECT * FROM sessions WHERE address = ? ORDER BY started_at ASC",
                    (address,),
                ).fetchall()
            else:
                session_rows = self._conn.execute(
                    "SELECT * FROM sessions ORDER BY started_at ASC"
                ).fetchall()
            sessions = []
            for session in session_rows:
                readings = self._conn.execute(
                    "SELECT * FROM readings WHERE session_id = ? ORDER BY recorded_at ASC",
                    (session["id"],),
                ).fetchall()
                sessions.append(
                    {
                        "session_id": session["id"],
                        "address": session["address"],
                        "name": session["name"],
                        "model": session["model"],
                        "started_at": session["started_at"],
                        "ended_at": session["ended_at"],
                        "readings": [
                            {
                                "recorded_at": reading["recorded_at"],
                                "unit": reading["unit"],
                                "battery_percent": reading["battery_percent"],
                                "propane_percent": reading["propane_percent"],
                                "pulse": json.loads(reading["pulse_json"] or "{}"),
                                "probes": json.loads(reading["probes_json"] or "[]"),
                            }
                            for reading in readings
                        ],
                    }
                )
            return sessions

    async def has_history(self) -> bool:
        async with self._lock:
            row = self._conn.execute("SELECT 1 FROM readings LIMIT 1").fetchone()
            return row is not None

    async def latest_ts(self) -> Optional[str]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT recorded_at FROM readings ORDER BY recorded_at DESC LIMIT 1"
            ).fetchone()
            if row:
                return row["recorded_at"]
            return None

    async def get_history_items(
        self,
        since_ts: Optional[str],
        until_ts: Optional[str],
        limit: Optional[int],
    ) -> List[Dict[str, object]]:
        query = "SELECT * FROM readings WHERE 1=1"
        params: List[object] = []
        if since_ts:
            query += " AND recorded_at >= ?"
            params.append(since_ts)
        if until_ts:
            query += " AND recorded_at <= ?"
            params.append(until_ts)
        query += " ORDER BY recorded_at ASC"
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        async with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            data_json = row["data_json"]
            if data_json:
                data = json.loads(data_json)
            else:
                data = {
                    "sensorId": row["address"],
                    "data": {
                        "unit": row["unit"],
                        "battery_percent": row["battery_percent"],
                        "propane_percent": row["propane_percent"],
                        "pulse": json.loads(row["pulse_json"] or "{}"),
                        "probes": json.loads(row["probes_json"] or "[]"),
                    },
                }
            items.append(
                {
                    "ts": row["recorded_at"],
                    "seq": row["seq"],
                    "data": data,
                }
            )
        return items


class DeviceWorker:
    def __init__(
        self,
        address: str,
        name: Optional[str],
        store: DeviceStore,
        history: HistoryStore,
        poll_interval: int,
        timeout: int,
    ) -> None:
        self.address = address
        self.name = name
        self.store = store
        self.history = history
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._model: Optional[ModelInfo] = None
        self._stop = asyncio.Event()
        self._connected_logged = False
        self._session_id: Optional[int] = None
        self._seq = 0

    def update_name(self, name: Optional[str]) -> None:
        if name:
            self.name = name

    async def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                LOG.debug("Connecting to %s (%s)", self.address, self.name or "unknown")
                async with BleakClient(self.address, timeout=self.timeout) as client:
                    self._connected_logged = False
                    services = client.services
                    if services is None:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=FutureWarning)
                            services = await client.get_services()
                    if services is None:
                        LOG.warning("No services discovered for %s", self.address)
                        await self.store.upsert(self.address, connected=False, error="services_unavailable")
                        await asyncio.sleep(3)
                        continue
                    self._model = detect_model(services)
                    await self._update_model_state()
                    await self._authenticate(client, services)
                    await self._poll_loop(client, services)
                    await self.store.upsert(self.address, connected=False)
                    await self.history.close_session(self.address, now_iso())
                    self._session_id = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("Device %s error: %s", self.address, exc)
                await self.store.upsert(
                    self.address,
                    connected=False,
                    error=str(exc),
                )
                if self._session_id is not None:
                    await self.history.close_session(self.address, now_iso())
                    self._session_id = None
                await asyncio.sleep(3)

    async def _update_model_state(self) -> None:
        if self._model:
            await self.store.upsert(
                self.address,
                connected=True,
                model=self._model.model_id,
                model_name=self._model.label,
                error=None,
            )
        else:
            await self.store.upsert(
                self.address,
                connected=True,
                model="unknown",
                model_name="Unknown",
                error=None,
            )

    async def _authenticate(self, client: BleakClient, services) -> None:
        if (
            services.get_characteristic(APP_CHALLENGE_UUID) is None
            or services.get_characteristic(DEVICE_CHALLENGE_UUID) is None
            or services.get_characteristic(DEVICE_RESPONSE_UUID) is None
        ):
            LOG.warning("Device %s missing authentication characteristics", self.address)
            return
        LOG.debug("Sending auth challenge to %s", self.address)
        await client.write_gatt_char(APP_CHALLENGE_UUID, bytes(16), response=True)
        challenge = await client.read_gatt_char(DEVICE_CHALLENGE_UUID)
        LOG.debug("Received device challenge from %s: %s", self.address, challenge.hex())
        await client.write_gatt_char(DEVICE_RESPONSE_UUID, challenge, response=True)

    async def _poll_loop(self, client: BleakClient, services) -> None:
        probe_uuids = []
        if self._model:
            probe_uuids = PROBE_TEMPERATURE_UUIDS[: self._model.probe_count]
        session_id = await self.history.ensure_session(
            self.address,
            self.name,
            self._model.model_id if self._model else None,
            now_iso(),
        )
        self._session_id = session_id
        await self.store.upsert(self.address, session_id=session_id)
        while client.is_connected and not self._stop.is_set():
            payload = await self._read_metrics(client, services, probe_uuids)
            payload["session_id"] = session_id
            await self.store.upsert(self.address, **payload)
            self._seq += 1
            device_entry = await self.store.get_device(self.address)
            if device_entry is None:
                await asyncio.sleep(self.poll_interval)
                continue
            reading_payload = build_reading_payload(device_entry)
            await self.store.publish_reading(
                {
                    "seq": self._seq,
                    "payload": reading_payload,
                }
            )
            await self.history.record_reading(session_id, self.address, payload, reading_payload, self._seq)
            await asyncio.sleep(self.poll_interval)

    async def _read_metrics(self, client: BleakClient, services, probe_uuids: List[str]) -> Dict[str, object]:
        payload: Dict[str, object] = {"last_update": now_iso(), "connected": True, "error": None}

        unit_data = await self._read_char(client, TEMPERATURE_UNIT_UUID, services)
        if unit_data:
            payload["unit"] = "C" if unit_data[0] == 0 else "F"

        battery_data = await self._read_char(client, BATTERY_LEVEL_UUID, services)
        if battery_data:
            payload["battery_percent"] = battery_data[0]

        propane_data = await self._read_char(client, PROPANE_LEVEL_UUID, services)
        if propane_data:
            payload["propane_percent"] = propane_data[0] * 25

        probes = []
        for index, uuid in enumerate(probe_uuids, start=1):
            probe_data = await self._read_char(client, uuid, services)
            if not probe_data:
                continue
            probe = parse_temperature_probe(index, probe_data)
            probes.append(probe)
        payload["probes"] = probes
        connected_probes = [probe["index"] for probe in probes if probe.get("unplugged") is False]
        payload["connected_probes"] = connected_probes
        payload["probe_status"] = "probes_connected" if connected_probes else "no_probes_connected"
        device_label = self.name or (self._model.label if self._model else "unknown")
        if not self._connected_logged:
            LOG.info(
                "%s mac_address: %s connected_probes: %s",
                device_label,
                self.address,
                json.dumps(connected_probes),
            )
            self._connected_logged = True
        LOG.info(
            "%s mac_address: %s last_update: %s probes: %s",
            device_label,
            self.address,
            payload["last_update"],
            json.dumps(probes),
        )
        if probes:
            LOG.debug(
                "%s mac_address: %s last_update: %s probes: %s",
                device_label,
                self.address,
                payload["last_update"],
                json.dumps(probes),
            )

        if self._model and self._model.is_pulse:
            pulse_data = await self._read_char(client, PULSE_ELEMENT_UUID, services)
            if pulse_data:
                pulse = parse_pulse_element(pulse_data)
                payload["pulse"] = pulse
        return payload

    async def _read_char(self, client: BleakClient, uuid: str, services) -> Optional[bytes]:
        if services.get_characteristic(uuid) is None:
            return None
        try:
            data = await asyncio.wait_for(client.read_gatt_char(uuid), timeout=self.timeout)
            LOG.debug("Read %s from %s: %s", uuid, self.address, data.hex())
            return data
        except asyncio.TimeoutError:
            LOG.warning("Timeout reading %s from %s", uuid, self.address)
            return None
        except Exception as exc:
            LOG.warning("Read error %s from %s: %s", uuid, self.address, exc)
            return None


class DeviceManager:
    def __init__(
        self,
        store: DeviceStore,
        history: HistoryStore,
        poll_interval: int,
        timeout: int,
        mac_prefix: str,
        scan_interval: int,
        scan_timeout: int,
    ) -> None:
        self.store = store
        self.history = history
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.mac_prefix = mac_prefix.lower()
        self.scan_interval = scan_interval
        self.scan_timeout = scan_timeout
        self._workers: Dict[str, DeviceWorker] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    async def scan_loop(self) -> None:
        while True:
            try:
                devices = await BleakScanner.discover(timeout=self.scan_timeout, return_adv=True)
                scan_items = []
                if isinstance(devices, dict):
                    for key, value in devices.items():
                        device = None
                        adv_data = None
                        if isinstance(value, tuple):
                            device = value[0]
                            adv_data = value[1] if len(value) > 1 else None
                        elif hasattr(value, "address"):
                            device = value
                        else:
                            adv_data = value
                        address = getattr(device, "address", None) or (key if isinstance(key, str) else None)
                        name = getattr(device, "name", None) or (getattr(adv_data, "local_name", None) if adv_data else None)
                        rssi = getattr(adv_data, "rssi", None) if adv_data else None
                        if address:
                            scan_items.append((address, name, rssi))
                else:
                    for entry in devices:
                        device = None
                        adv_data = None
                        address = None
                        name = None
                        rssi = None
                        if isinstance(entry, tuple):
                            device = entry[0]
                            adv_data = entry[1] if len(entry) > 1 else None
                            address = getattr(device, "address", None)
                            name = getattr(device, "name", None) or (getattr(adv_data, "local_name", None) if adv_data else None)
                            rssi = getattr(adv_data, "rssi", None) if adv_data else None
                        elif hasattr(entry, "address"):
                            device = entry
                            address = getattr(device, "address", None)
                            name = getattr(device, "name", None)
                        elif isinstance(entry, str):
                            address = entry
                        if address:
                            scan_items.append((address, name, rssi))
                for address, name, rssi in scan_items:
                    if not address.lower().startswith(self.mac_prefix):
                        continue
                    LOG.debug("Discovered %s (%s) rssi=%s", address, name, rssi)
                    await self.store.upsert(
                        address,
                        name=name,
                        last_seen=now_iso(),
                        rssi=rssi,
                    )
                    if address not in self._workers:
                        worker = DeviceWorker(
                            address,
                            name,
                            self.store,
                            self.history,
                            self.poll_interval,
                            self.timeout,
                        )
                        self._workers[address] = worker
                        self._tasks[address] = asyncio.create_task(worker.run())
                    else:
                        self._workers[address].update_name(name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("Scan error: %s", exc)
            await asyncio.sleep(self.scan_interval)

    async def stop(self) -> None:
        for worker in self._workers.values():
            await worker.stop()
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)


class WebSocketClient:
    def __init__(self, ws: web.WebSocketResponse, queue_size: int = 1) -> None:
        self.ws = ws
        self.queue: asyncio.Queue[Dict[str, object]] = asyncio.Queue(maxsize=queue_size)
        self.task = asyncio.create_task(self._sender())

    async def _sender(self) -> None:
        while True:
            message = await self.queue.get()
            if self.ws.closed:
                break
            await self.ws.send_json(message)

    def enqueue(self, message: Dict[str, object]) -> None:
        if self.queue.full():
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        self.queue.put_nowait(message)

    async def close(self) -> None:
        if not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class WebSocketHub:
    def __init__(self) -> None:
        self.clients: set[WebSocketClient] = set()

    def add(self, client: WebSocketClient) -> None:
        self.clients.add(client)

    async def remove(self, client: WebSocketClient) -> None:
        if client in self.clients:
            self.clients.remove(client)
        await client.close()

    def broadcast(self, message: Dict[str, object]) -> None:
        for client in list(self.clients):
            if client.ws.closed:
                self.clients.discard(client)
                continue
            client.enqueue(message)


def detect_model(services) -> Optional[ModelInfo]:
    service_uuids = {service.uuid.lower() for service in services}
    for model in MODELS:
        if model.service_uuid in service_uuids:
            return model
    return None


def parse_temperature_probe(index: int, data: bytes) -> Dict[str, object]:
    if len(data) < 2:
        return {"index": index, "temperature": None, "raw": None, "unplugged": None}
    raw = data[0] | (data[1] << 8)
    unplugged = raw == UNPLUGGED_PROBE_CONSTANT
    return {
        "index": index,
        "temperature": None if unplugged else float(raw),
        "raw": raw,
        "unplugged": unplugged,
    }


def parse_pulse_element(data: bytes) -> Dict[str, Optional[int]]:
    def parse_slice(start: int) -> Optional[int]:
        if len(data) < start + 3:
            return None
        try:
            return int(data[start : start + 3].decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            return None

    return {
        "heating_actual1": parse_slice(1),
        "heating_actual2": parse_slice(5),
        "heating_setpoint1": parse_slice(9),
        "heating_setpoint2": parse_slice(13),
    }


def build_reading_payload(device_entry: Dict[str, object]) -> Dict[str, object]:
    data = {
        "name": device_entry.get("name"),
        "model": device_entry.get("model"),
        "model_name": device_entry.get("model_name"),
        "session_id": device_entry.get("session_id"),
        "last_update": device_entry.get("last_update"),
        "unit": device_entry.get("unit"),
        "battery_percent": device_entry.get("battery_percent"),
        "propane_percent": device_entry.get("propane_percent"),
        "probes": device_entry.get("probes", []),
        "connected_probes": device_entry.get("connected_probes", []),
        "probe_status": device_entry.get("probe_status"),
        "pulse": device_entry.get("pulse", {}),
        "error": device_entry.get("error"),
    }
    payload = {
        "sensorId": device_entry.get("address"),
        "data": data,
    }
    q = {
        "rssi": device_entry.get("rssi"),
        "batteryPct": device_entry.get("battery_percent"),
    }
    if q["rssi"] is not None or q["batteryPct"] is not None:
        payload["q"] = q
    return payload


def make_envelope(
    msg_type: str,
    payload: Dict[str, object],
    request_id: Optional[str] = None,
    seq: Optional[int] = None,
) -> Dict[str, object]:
    envelope: Dict[str, object] = {
        "v": 1,
        "type": msg_type,
        "ts": now_iso_utc(),
        "payload": payload,
    }
    if request_id:
        envelope["requestId"] = request_id
    if seq is not None:
        envelope["seq"] = seq
    return envelope


async def send_envelope(
    ws: web.WebSocketResponse,
    msg_type: str,
    payload: Dict[str, object],
    request_id: Optional[str] = None,
    seq: Optional[int] = None,
) -> None:
    await ws.send_json(make_envelope(msg_type, payload, request_id=request_id, seq=seq))


async def send_error(
    ws: web.WebSocketResponse,
    code: str,
    message: str,
    request_id: Optional[str] = None,
    details: Optional[Dict[str, object]] = None,
) -> None:
    payload: Dict[str, object] = {"code": code, "message": message}
    if details:
        payload["details"] = details
    await send_envelope(ws, "error", payload, request_id=request_id)


async def metrics_handler(request: web.Request) -> web.Response:
    store: DeviceStore = request.app["store"]
    snapshot = await store.snapshot()
    return web.json_response(
        {
            "generated_at": now_iso(),
            "device_count": len(snapshot),
            "devices": list(snapshot.values()),
        }
    )

async def history_handler(request: web.Request) -> web.Response:
    history: HistoryStore = request.app["history"]
    address = request.query.get("mac")
    sessions = await history.get_history(address)
    return web.json_response(
        {
            "generated_at": now_iso(),
            "session_count": len(sessions),
            "sessions": sessions,
        }
    )


# WebSocket examples:
# Client status request:
# {"v":1,"type":"status_request","requestId":"s-1","payload":{}}
# Server status response:
# {"v":1,"type":"status","ts":"2026-01-02T01:00:00Z","requestId":"s-1","payload":{"hasData":true,"latestTs":"2026-01-02T01:00:00+00:00","historyAvailable":true}}
# Client history request:
# {"v":1,"type":"history_request","requestId":"h-1","payload":{"sinceTs":"2026-01-02T00:00:00+00:00","chunkSize":200}}
# Server history chunk:
# {"v":1,"type":"history_chunk","ts":"2026-01-02T01:00:00Z","requestId":"h-1","payload":{"items":[{"ts":"2026-01-02T01:00:00+00:00","seq":1,"data":{"sensorId":"70:91:8F:...","data":{"probes":[]}}}]}}
async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    store: DeviceStore = request.app["store"]
    history: HistoryStore = request.app["history"]
    hub: WebSocketHub = request.app["hub"]
    poll_interval: int = request.app["poll_interval"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    client = WebSocketClient(ws)
    hub.add(client)
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    await send_error(ws, "invalid_json", "Message must be valid JSON.")
                    continue
                if not isinstance(data, dict):
                    await send_error(ws, "invalid_message", "Message must be a JSON object.")
                    continue
                if data.get("v") != 1:
                    await send_error(ws, "unsupported_version", "Unsupported message version.")
                    continue
                msg_type = data.get("type")
                request_id = data.get("requestId")
                payload = data.get("payload") or {}
                if msg_type == "status_request":
                    if not request_id:
                        await send_error(ws, "missing_request_id", "status_request requires requestId.")
                        continue
                    snapshot = await store.snapshot()
                    has_data = any(device.get("last_update") for device in snapshot.values())
                    latest_ts = None
                    if has_data:
                        latest_ts = max(
                            device.get("last_update")
                            for device in snapshot.values()
                            if device.get("last_update")
                        )
                    history_available = await history.has_history()
                    if latest_ts is None and history_available:
                        latest_ts = await history.latest_ts()
                    any_connected = any(device.get("connected") for device in snapshot.values())
                    any_error = any(device.get("error") for device in snapshot.values())
                    if any_error:
                        device_state = "error"
                    elif any_connected and has_data:
                        device_state = "ok"
                    elif any_connected and not has_data:
                        device_state = "warming_up"
                    else:
                        device_state = "offline"
                    status_payload = {
                        "hasData": has_data,
                        "latestTs": latest_ts,
                        "sampleRateHz": round(1.0 / poll_interval, 4),
                        "historyAvailable": history_available,
                        "deviceState": device_state,
                    }
                    await send_envelope(ws, "status", status_payload, request_id=request_id)
                elif msg_type == "history_request":
                    if not request_id:
                        await send_error(ws, "missing_request_id", "history_request requires requestId.")
                        continue
                    if not isinstance(payload, dict):
                        await send_error(ws, "invalid_payload", "history_request payload must be an object.", request_id)
                        continue
                    since_ts = payload.get("sinceTs")
                    until_ts = payload.get("untilTs")
                    limit = payload.get("limit")
                    chunk_size = payload.get("chunkSize", 200)
                    try:
                        if limit is not None:
                            limit = int(limit)
                        chunk_size = int(chunk_size)
                    except (TypeError, ValueError):
                        await send_error(ws, "invalid_payload", "limit and chunkSize must be integers.", request_id)
                        continue
                    if limit is not None and limit <= 0:
                        limit = None
                    if chunk_size <= 0:
                        chunk_size = 200
                    items = await history.get_history_items(since_ts, until_ts, limit)
                    count = 0
                    latest_ts = None
                    chunk: List[Dict[str, object]] = []
                    for item in items:
                        chunk.append(item)
                        count += 1
                        latest_ts = item.get("ts") or latest_ts
                        if len(chunk) >= chunk_size:
                            await send_envelope(
                                ws,
                                "history_chunk",
                                {"items": chunk},
                                request_id=request_id,
                            )
                            chunk = []
                    if chunk:
                        await send_envelope(
                            ws,
                            "history_chunk",
                            {"items": chunk},
                            request_id=request_id,
                        )
                    await send_envelope(
                        ws,
                        "history_end",
                        {"count": count, "latestTs": latest_ts},
                        request_id=request_id,
                    )
                else:
                    await send_error(
                        ws,
                        "unknown_type",
                        f"Unsupported message type: {msg_type}",
                        request_id=request_id,
                    )
            elif msg.type == web.WSMsgType.ERROR:
                LOG.debug("WebSocket error: %s", ws.exception())
    finally:
        await hub.remove(client)
    return ws


async def broadcast_readings(app: web.Application) -> None:
    store: DeviceStore = app["store"]
    hub: WebSocketHub = app["hub"]
    while True:
        reading = await store.next_reading()
        message = make_envelope("reading", reading["payload"], seq=reading.get("seq"))
        hub.broadcast(message)


async def start_server(
    store: DeviceStore,
    history: HistoryStore,
    host: str,
    port: int,
) -> tuple[web.AppRunner, web.Application]:
    app = web.Application()
    app["store"] = store
    app["history"] = history
    app["hub"] = WebSocketHub()
    app.router.add_get("/metrics", metrics_handler)
    app.router.add_get("/history", history_handler)
    app.router.add_get("/ws", websocket_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    LOG.info("HTTP server listening on %s:%d", host, port)
    return runner, app


async def run() -> None:
    log_level_name = os.getenv("IGRILL_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=log_level_name,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("bleak").setLevel(log_level_name)
    port = read_int_env("IGRILL_PORT", DEFAULT_PORT)
    poll_interval = read_int_env("IGRILL_POLL_INTERVAL", DEFAULT_POLL_INTERVAL, MIN_POLL_INTERVAL, MAX_POLL_INTERVAL)
    timeout = read_int_env("IGRILL_TIMEOUT", DEFAULT_TIMEOUT)
    scan_interval = read_int_env("IGRILL_SCAN_INTERVAL", DEFAULT_SCAN_INTERVAL)
    scan_timeout = read_int_env("IGRILL_SCAN_TIMEOUT", DEFAULT_SCAN_TIMEOUT)
    reconnect_grace = read_int_env("IGRILL_RECONNECT_GRACE", DEFAULT_RECONNECT_GRACE)
    db_path = os.getenv("IGRILL_DB_PATH", DEFAULT_DB_PATH)
    mac_prefix = os.getenv("IGRILL_MAC_PREFIX", "70:91:8F")
    bind_address = os.getenv("IGRILL_BIND_ADDRESS", "0.0.0.0")

    LOG.info(
        "Config: port=%d poll=%ds timeout=%ds scan_interval=%ds scan_timeout=%ds mac_prefix=%s reconnect_grace=%ds db_path=%s",
        port,
        poll_interval,
        timeout,
        scan_interval,
        scan_timeout,
        mac_prefix,
        reconnect_grace,
        db_path,
    )

    store = DeviceStore()
    history = HistoryStore(db_path, reconnect_grace)
    manager = DeviceManager(
        store=store,
        history=history,
        poll_interval=poll_interval,
        timeout=timeout,
        mac_prefix=mac_prefix,
        scan_interval=scan_interval,
        scan_timeout=scan_timeout,
    )

    runner, app = await start_server(store, history, bind_address, port)
    app["poll_interval"] = poll_interval
    broadcast_task = asyncio.create_task(broadcast_readings(app))
    scan_task = asyncio.create_task(manager.scan_loop())

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, sig), stop_event.set)
        except (AttributeError, NotImplementedError):
            pass

    await stop_event.wait()
    scan_task.cancel()
    broadcast_task.cancel()
    await asyncio.gather(scan_task, return_exceptions=True)
    await asyncio.gather(broadcast_task, return_exceptions=True)
    for client in list(app["hub"].clients):
        await app["hub"].remove(client)
    await manager.stop()
    await runner.cleanup()


if __name__ == "__main__":
    import signal

    asyncio.run(run())
