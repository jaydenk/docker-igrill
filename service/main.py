import asyncio
import json
import logging
import os
import signal
import sqlite3
from dataclasses import dataclass
from datetime import datetime
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
        self._updates: asyncio.Queue[Dict[str, object]] = asyncio.Queue(maxsize=1000)

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
                    "error": None,
                    "rssi": None,
                },
            )
            entry.update(fields)
            if "last_update" in fields:
                payload = {
                    "type": "device_update",
                    "generated_at": now_iso(),
                    "device": dict(entry),
                }
                if self._updates.full():
                    try:
                        self._updates.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                self._updates.put_nowait(payload)

    async def snapshot(self) -> Dict[str, Dict[str, object]]:
        async with self._lock:
            return {key: dict(value) for key, value in self._devices.items()}

    async def next_update(self) -> Dict[str, object]:
        return await self._updates.get()


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
                unit TEXT,
                battery_percent REAL,
                propane_percent REAL,
                pulse_json TEXT,
                probes_json TEXT,
                FOREIGN KEY(session_id) REFERENCES sessions(id)
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_address ON sessions(address)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_session ON readings(session_id)")
        self._conn.commit()

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

    async def record_reading(self, session_id: int, address: str, payload: Dict[str, object]) -> None:
        async with self._lock:
            self._conn.execute(
                """
                INSERT INTO readings (
                    session_id,
                    address,
                    recorded_at,
                    unit,
                    battery_percent,
                    propane_percent,
                    pulse_json,
                    probes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    address,
                    payload.get("last_update"),
                    payload.get("unit"),
                    payload.get("battery_percent"),
                    payload.get("propane_percent"),
                    json.dumps(payload.get("pulse", {})),
                    json.dumps(payload.get("probes", [])),
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
            await self.history.record_reading(session_id, self.address, payload)
            await asyncio.sleep(self.poll_interval)

    async def _read_metrics(self, client: BleakClient, services, probe_uuids: List[str]) -> Dict[str, object]:
        payload: Dict[str, object] = {"last_update": now_iso(), "connected": True, "error": None}

        unit_data = await self._read_char(client, TEMPERATURE_UNIT_UUID, services)
        if unit_data:
            payload["unit"] = "F" if unit_data[0] == 0 else "C"

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
        device_label = self.name or (self._model.label if self._model else "unknown")
        if not self._connected_logged:
            connected_probes = [
                probe["index"]
                for probe in probes
                if probe.get("unplugged") is False
            ]
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
                for device, adv_data in devices:
                    if not device.address:
                        continue
                    address = device.address
                    name = device.name or getattr(adv_data, "local_name", None)
                    rssi = getattr(adv_data, "rssi", None)
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


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    store: DeviceStore = request.app["store"]
    history: HistoryStore = request.app["history"]
    websockets: set = request.app["websockets"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    websockets.add(ws)
    snapshot = await store.snapshot()
    await ws.send_json(
        {
            "type": "snapshot",
            "generated_at": now_iso(),
            "devices": list(snapshot.values()),
        }
    )
    sessions = await history.get_history()
    await ws.send_json(
        {
            "type": "history",
            "generated_at": now_iso(),
            "sessions": sessions,
        }
    )
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.ERROR:
                LOG.debug("WebSocket error: %s", ws.exception())
    finally:
        websockets.discard(ws)
    return ws


async def broadcast_updates(app: web.Application) -> None:
    store: DeviceStore = app["store"]
    websockets: set = app["websockets"]
    while True:
        update = await store.next_update()
        if not websockets:
            continue
        stale = []
        for ws in websockets:
            if ws.closed:
                stale.append(ws)
                continue
            try:
                await ws.send_json(update)
            except Exception:
                stale.append(ws)
        for ws in stale:
            websockets.discard(ws)


async def start_server(
    store: DeviceStore,
    history: HistoryStore,
    host: str,
    port: int,
) -> tuple[web.AppRunner, web.Application]:
    app = web.Application()
    app["store"] = store
    app["history"] = history
    app["websockets"] = set()
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
    broadcast_task = asyncio.create_task(broadcast_updates(app))
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
    for ws in list(app["websockets"]):
        await ws.close()
    await manager.stop()
    await runner.cleanup()


if __name__ == "__main__":
    import signal

    asyncio.run(run())
