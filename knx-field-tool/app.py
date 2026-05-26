# -*- coding: utf-8 -*-
"""
KNX Field Tool - Flask Backend
Web server with SocketIO for real-time KNX Group Monitor.
"""

# Use threading mode — eventlet conflicts with asyncio (used for KNX operations)
_ASYNC_MODE = 'threading'

import os
import sys
import json
import threading
import asyncio
import queue
import time
from datetime import datetime

from flask import Flask, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS

from knx_parser import parse_knxproj


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _get_xknx_version() -> str:
    """Return the installed xknx version as a plain string."""
    # importlib.metadata is the correct way — getattr(module, '__version__') can
    # return a sub-module object in some xknx packaging layouts.
    try:
        import importlib.metadata
        return importlib.metadata.version('xknx')
    except Exception:
        pass
    try:
        import xknx as _x
        v = getattr(_x, '__version__', None)
        if v is not None and isinstance(v, str):
            return v
    except Exception:
        pass
    return 'unknown'

# ──────────────────────────────────────────────
# App setup
# ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')

# When running as HA add-on, projects are stored in /data/projects (persistent volume).
# Locally, store next to app.py.
PROJECTS_DIR = os.environ.get('PROJECTS_DIR', os.path.join(BASE_DIR, 'projects'))
os.makedirs(PROJECTS_DIR, exist_ok=True)

# HA add-on: /config is the HA config directory (mapped read-only)
# KNX_PROJECT_FOLDER is a subfolder inside /config (default: knx-field-tool)
# KNX_PROJECT_FILE   is the .knxproj filename inside that folder
KNX_CONFIG_DIR     = os.environ.get('KNX_CONFIG_DIR', '')
KNX_PROJECT_FOLDER = os.environ.get('KNX_PROJECT_FOLDER', 'knx-field-tool').strip()
KNX_PROJECT_FILE   = os.environ.get('KNX_PROJECT_FILE', '').strip()

# Full path to the projects folder inside /config
KNX_PROJECTS_PATH = os.path.join(KNX_CONFIG_DIR, KNX_PROJECT_FOLDER) if KNX_CONFIG_DIR else ''

# Default KNX gateway from HA add-on options (can be overridden in UI)
DEFAULT_KNX_HOST = os.environ.get('KNX_DEFAULT_HOST', '')
try:
    DEFAULT_KNX_PORT = int(os.environ.get('KNX_DEFAULT_PORT', '') or 3671)
except (ValueError, TypeError):
    DEFAULT_KNX_PORT = 3671

# Ping/diagnosis timeout — configured in add-on options (knx_ping_timeout)
try:
    KNX_PING_TIMEOUT = max(1, min(30, int(os.environ.get('KNX_PING_TIMEOUT', '') or 3)))
except (ValueError, TypeError):
    KNX_PING_TIMEOUT = 3

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path='')
app.config['SECRET_KEY'] = 'knx-field-tool-secret'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max upload

CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode=_ASYNC_MODE)

# ──────────────────────────────────────────────
# Global state
# ──────────────────────────────────────────────
project_data = {}          # Parsed project data (in-memory)
ga_lookup = {}             # address_int -> GA info (for Group Monitor decode)
monitor_state = {
    'running': False,
    'thread': None,
    'loop': None,
    'xknx_instance': None,
    'host': '',
    'port': 3671,
    'telegram_count': 0,
    # Diagnostic counters — updated inside _async_monitor
    'cb_invoked': 0,    # times telegram_received callback was called
    'cb_errors':  0,    # times callback raised an exception
    'cb_last_error': '',
    'local_ip': '',     # IP xknx will advertise in the KNX/IP HPAI
}
telegram_queue = queue.Queue()  # Thread-safe queue for outgoing telegrams

# ── Telegram buffer for REST polling ──────────────────────────────────────────
# HA Ingress reverse-proxy can delay or drop SocketIO push events from background
# threads. Keeping a rolling buffer allows the frontend to poll for new telegrams
# via GET /api/monitor/telegrams?since=<seq> — reliable through any HTTP proxy.
from collections import deque as _deque
_telegram_buffer      = _deque(maxlen=1000)   # last 1 000 telegrams
_telegram_buffer_lock = threading.Lock()
_telegram_seq         = 0                     # monotonically increasing counter


# ──────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'index.html')


@app.route('/api/config', methods=['GET'])
def get_config():
    """Expose add-on configuration (gateway + project file info)."""
    return jsonify({
        'knx_default_host': DEFAULT_KNX_HOST,
        'knx_default_port': DEFAULT_KNX_PORT,
        'knx_ping_timeout': KNX_PING_TIMEOUT,
        'knx_project_folder': KNX_PROJECT_FOLDER,
        'knx_project_file': KNX_PROJECT_FILE,
        'project_loaded': bool(project_data),
        'project_name': project_data.get('project', {}).get('name', '') if project_data else '',
    })


@app.route('/api/upload', methods=['POST'])
def upload_project():
    """Upload and parse a .knxproj file. Also persists file to PROJECTS_DIR."""
    global project_data, ga_lookup

    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    f = request.files['file']
    if not f.filename.endswith('.knxproj'):
        return jsonify({'error': 'File must be a .knxproj file'}), 400

    try:
        file_bytes = f.read()

        # Persist to PROJECTS_DIR so it survives container restarts
        save_path = os.path.join(PROJECTS_DIR, 'last_project.knxproj')
        with open(save_path, 'wb') as fp:
            fp.write(file_bytes)

        result = parse_knxproj(file_bytes)
        project_data = result

        # Build GA lookup by integer address for Group Monitor decoding
        ga_lookup = {}
        for ga in result.get('group_addresses', []):
            ga_lookup[ga['address_int']] = ga

        return jsonify({
            'success': True,
            'project': result['project'],
            'topology': result['topology'],
            'buildings': result['buildings'],
            'cabinets': result['cabinets'],
            'ga_tree': result['ga_tree'],
            'ip_connections': result['ip_connections'],
            'stats': {
                'devices': result['project']['total_devices'],
                'group_addresses': result['project']['total_group_addresses'],
                'ip_connections': len(result['ip_connections']),
            }
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


@app.route('/api/project', methods=['GET'])
def get_project():
    """Get the currently loaded project data."""
    if not project_data:
        return jsonify({'error': 'No project loaded'}), 404
    return jsonify(project_data)


@app.route('/api/ga_lookup', methods=['GET'])
def get_ga_lookup():
    """Get GA lookup table for Group Monitor."""
    return jsonify(ga_lookup)


@app.route('/api/monitor/status', methods=['GET'])
def monitor_status():
    """Get Group Monitor connection status."""
    return jsonify({
        'running': monitor_state['running'],
        'host': monitor_state['host'],
        'port': monitor_state['port'],
        'telegram_count': monitor_state['telegram_count'],
        'buffer_size': len(_telegram_buffer),
        'last_seq': _telegram_seq,
        'xknx_version': _get_xknx_version(),
    })


@app.route('/api/monitor/debug', methods=['GET'])
def monitor_debug():
    """Return diagnostic information for troubleshooting."""
    with _telegram_buffer_lock:
        last_5 = list(_telegram_buffer)[-5:]
    return jsonify({
        'monitor_running': monitor_state['running'],
        'host': monitor_state['host'],
        'port': monitor_state['port'],
        'local_ip': monitor_state.get('local_ip', ''),
        'telegram_count': monitor_state['telegram_count'],
        'cb_invoked': monitor_state.get('cb_invoked', 0),
        'cb_errors':  monitor_state.get('cb_errors', 0),
        'cb_last_error': monitor_state.get('cb_last_error', ''),
        'buffer_size': len(_telegram_buffer),
        'last_seq': _telegram_seq,
        'xknx_version': _get_xknx_version(),
        'last_5_telegrams': last_5,
    })


@app.route('/api/monitor/telegrams', methods=['GET'])
def get_telegrams():
    """Return telegrams buffered since a given sequence number.

    Query param: since=<int>  (default 0 → return all buffered telegrams)
    Response:    { telegrams: [...], last_seq: <int> }
    """
    since = int(request.args.get('since', 0))
    with _telegram_buffer_lock:
        new_tgs  = [t for t in _telegram_buffer if t.get('seq', 0) > since]
        last_seq = _telegram_seq
    return jsonify({'telegrams': new_tgs, 'last_seq': last_seq})


@app.route('/api/monitor/start', methods=['POST'])
def monitor_start():
    """Start the KNX Group Monitor."""
    data = request.json or {}
    host = data.get('host', '')
    port = int(data.get('port', 3671))
    local_ip = data.get('local_ip', None)
    tunnel_type = data.get('tunnel_type', 'udp')

    if not host:
        # Try to get from loaded project
        if project_data and project_data.get('ip_connections'):
            ip = project_data['ip_connections'][0]
            host = ip['host']
            port = ip['port']

    if not host and DEFAULT_KNX_HOST:
        host = DEFAULT_KNX_HOST
        port = DEFAULT_KNX_PORT

    if not host:
        return jsonify({'error': 'No KNX IP gateway address provided'}), 400

    # If a previous thread is still winding down, wait for it to finish
    # so the DISCONNECT_REQUEST reaches the gateway before we send a new
    # CONNECT_REQUEST on a fresh socket.
    old_t = monitor_state.get('thread')
    if old_t and old_t.is_alive():
        old_t.join(timeout=2.0)

    if monitor_state['running']:
        return jsonify({'error': 'Monitor already running'}), 400

    monitor_state['host'] = host
    monitor_state['port'] = port
    monitor_state['telegram_count'] = 0

    # Start monitor in background thread
    t = threading.Thread(
        target=_run_monitor_thread,
        args=(host, port, local_ip),
        daemon=True
    )
    monitor_state['thread'] = t
    t.start()

    return jsonify({'success': True, 'host': host, 'port': port})


@app.route('/api/monitor/stop', methods=['POST'])
def monitor_stop():
    """Stop the KNX Group Monitor.

    Waits for the monitor thread to finish so that the DISCONNECT_REQUEST
    is fully sent to the gateway before this response reaches the frontend.
    Without this wait, a rapid disconnect→connect cycle arrives at the
    gateway while the old channel is still open, causing the new connection
    to receive no TUNNELING_REQUESTs.
    """
    _stop_monitor()
    t = monitor_state.get('thread')
    if t and t.is_alive():
        t.join(timeout=2.0)   # 2 s is generous; disconnect takes < 300 ms
    return jsonify({'success': True})


# ──────────────────────────────────────────────
# SocketIO events
# ──────────────────────────────────────────────
@socketio.on('connect')
def on_connect():
    emit('status', {'connected': True, 'monitor_running': monitor_state['running']})


@socketio.on('monitor_start')
def ws_monitor_start(data):
    """Start monitor via WebSocket."""
    host = data.get('host', '')
    port = int(data.get('port', 3671))
    local_ip = data.get('local_ip', None)

    if not host and project_data and project_data.get('ip_connections'):
        ip = project_data['ip_connections'][0]
        host = ip['host']
        port = ip['port']

    if not host and DEFAULT_KNX_HOST:
        host = DEFAULT_KNX_HOST
        port = DEFAULT_KNX_PORT

    if not host:
        emit('monitor_error', {'message': 'No KNX IP gateway address provided'})
        return

    if monitor_state['running']:
        emit('monitor_error', {'message': 'Monitor already running'})
        return

    monitor_state['host'] = host
    monitor_state['port'] = port
    monitor_state['telegram_count'] = 0

    t = threading.Thread(
        target=_run_monitor_thread,
        args=(host, port, local_ip),
        daemon=True
    )
    monitor_state['thread'] = t
    t.start()

    emit('monitor_started', {'host': host, 'port': port})


@socketio.on('monitor_stop')
def ws_monitor_stop(data=None):
    """Stop monitor via WebSocket."""
    _stop_monitor()
    emit('monitor_stopped', {})


@socketio.on('send_telegram')
def ws_send_telegram(data):
    """Send a KNX group telegram (write/read)."""
    ga_str = data.get('address', '')
    value = data.get('value', None)
    action = data.get('action', 'write')

    if not monitor_state['running']:
        emit('monitor_error', {'message': 'Monitor not connected'})
        return

    # Queue a send request
    telegram_queue.put({'_type': 'send', 'address': ga_str, 'value': value, 'action': action})


# ──────────────────────────────────────────────
# Monitor background thread
# ──────────────────────────────────────────────
def _run_monitor_thread(host, port, local_ip=None):
    """Background thread that runs the async KNX monitor."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    monitor_state['loop'] = loop

    try:
        loop.run_until_complete(_async_monitor(host, port, local_ip))
    except Exception as e:
        _emit_safe('monitor_error', {'message': f'Monitor error: {str(e)}'})
    finally:
        monitor_state['running'] = False
        monitor_state['loop'] = None
        loop.close()
        _emit_safe('monitor_stopped', {'reason': 'disconnected'})


async def _async_monitor(host, port, local_ip=None):
    """
    KNX/IP Group Monitor — raw UDP tunneling implementation.

    Diagnosis showed that, even with the correct local_ip in the HPAI,
    the xknx telegram_queue callback (register_telegram_received_cb) is
    never invoked for incoming group telegrams in xknx 3.x. Root cause:
    an internal xknx architectural change means the callback is only
    triggered when xknx Device objects process the telegram, not for
    raw/unfiltered frames.

    Solution: bypass xknx entirely for receiving and implement the
    KNX/IP Tunneling protocol directly with a raw UDP socket. This gives
    us full control and works regardless of xknx version.
    xknx is still used for outgoing WRITE telegrams (via write_ga endpoint
    which creates its own temporary connection).
    """
    import socket as _socket
    import struct

    GW = (host, port)

    # ── Detect local IP ───────────────────────────────────────────────────────
    _lip = local_ip or ''
    if not _lip:
        try:
            _s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            _s.connect((host, int(port)))
            _lip = _s.getsockname()[0]
            _s.close()
        except Exception:
            _lip = '0.0.0.0'
    monitor_state['local_ip'] = _lip
    print(f'[monitor] local_ip={_lip!r}  gateway={host}:{port}', flush=True)

    # ── UDP socket ────────────────────────────────────────────────────────────
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', 0))
    sock.setblocking(False)
    _, MY_PORT = sock.getsockname()

    try:
        MY_IP_B = _socket.inet_aton(_lip)
    except Exception:
        MY_IP_B = bytes(4)

    # ── KNX/IP protocol helpers ───────────────────────────────────────────────
    def _hdr(service, body: bytes) -> bytes:
        """KNXnet/IP header (6 bytes) + body."""
        return struct.pack('!BBHH', 6, 0x10, service, 6 + len(body)) + body

    def _hpai(ip_b: bytes, p: int) -> bytes:
        """Host Protocol Address Information (8 bytes, IPv4/UDP)."""
        return struct.pack('!BB4sH', 8, 0x01, ip_b, p)

    HPAI_SELF = _hpai(MY_IP_B, MY_PORT)

    # CRI — Tunneling, Link Layer (4 bytes)
    CRI = bytes([0x04, 0x04, 0x02, 0x00])

    # ── CONNECT_REQUEST (0x0205) ──────────────────────────────────────────────
    sock.sendto(_hdr(0x0205, HPAI_SELF + HPAI_SELF + CRI), GW)
    print(f'[monitor] CONNECT_REQUEST → {GW}  local_port={MY_PORT}', flush=True)

    # ── Wait for CONNECT_RESPONSE (0x0206) ────────────────────────────────────
    channel_id = None
    t0 = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - t0 < 10:
        try:
            data, _ = sock.recvfrom(1024)
            if len(data) >= 8 and struct.unpack('!H', data[2:4])[0] == 0x0206:
                status = data[7]
                if status == 0x00:
                    channel_id = data[6]
                    print(f'[monitor] CONNECT_RESPONSE OK  channel={channel_id}', flush=True)
                else:
                    msg = {
                        0x21: 'No more free tunnelling connections (gateway is full)',
                        0x23: 'Connection type not supported',
                        0x24: 'Connection option not supported',
                        0x25: 'No more free data connections',
                        0x26: 'Tunnelling layer not supported',
                    }.get(status, f'Gateway error 0x{status:02X}')
                    print(f'[monitor] CONNECT_RESPONSE error: {msg}', flush=True)
                    _emit_safe('monitor_error', {'message': msg})
                    sock.close()
                    return
                break
        except BlockingIOError:
            pass
        await asyncio.sleep(0.05)

    if channel_id is None:
        msg = (f'Timeout: no CONNECT_RESPONSE from {host}:{port} (10 s).\n'
               f'• Check IP and port are correct\n'
               f'• Gateway may already have a client connected')
        print(f'[monitor] {msg}', flush=True)
        _emit_safe('monitor_error', {'message': msg})
        sock.close()
        return

    # ── Connected ─────────────────────────────────────────────────────────────
    _reset_telegram_buffer()
    monitor_state['running']      = True
    monitor_state['xknx_instance'] = None   # raw mode — no xknx instance
    monitor_state['cb_invoked']    = 0
    monitor_state['cb_errors']     = 0
    monitor_state['cb_last_error'] = ''

    _emit_safe('monitor_connected', {'host': host, 'port': port,
                                     'xknx_version': f'raw-udp/{_get_xknx_version()}'})

    last_hb   = asyncio.get_event_loop().time()
    pkt_count = 0

    # ── Main receive loop ─────────────────────────────────────────────────────
    while monitor_state['running']:
        now = asyncio.get_event_loop().time()

        # CONNECTIONSTATE_REQUEST (0x0207) every 60 s
        if now - last_hb >= 60:
            cs_body = bytes([channel_id, 0x00]) + HPAI_SELF
            sock.sendto(_hdr(0x0207, cs_body), GW)
            last_hb = now

        try:
            data, addr = sock.recvfrom(1024)
        except BlockingIOError:
            await asyncio.sleep(0.005)
            continue
        except Exception as e:
            print(f'[monitor] recv error: {e}', flush=True)
            await asyncio.sleep(0.1)
            continue

        if len(data) < 6:
            continue

        svc = struct.unpack('!H', data[2:4])[0]

        if svc == 0x0420 and len(data) >= 10:   # TUNNELING_REQUEST
            # KNX/IP connection header (4 bytes after the 6-byte KNXnet/IP header):
            #   data[6]  = structure_length  (always 0x04)
            #   data[7]  = channel_id        (assigned by gateway in CONNECT_RESPONSE)
            #   data[8]  = sequence_counter
            #   data[9]  = reserved (0x00)
            #   data[10] = start of cEMI frame
            ch = data[7]   # ← channel_id  (NOT data[6] which is structure_length = 0x04)
            sn = data[8]   # ← sequence_counter
            if ch == channel_id:
                # ACK immediately (TUNNELING_ACK 0x0421)
                # Body: structure_length=4, channel_id, sequence_counter, status=0
                ack_body = bytes([4, ch, sn, 0x00])
                sock.sendto(_hdr(0x0421, ack_body), addr)
                # cEMI frame starts at byte 10
                _parse_cemi(data[10:], addr[0])
                pkt_count += 1
                monitor_state['cb_invoked'] += 1

        elif svc == 0x0208:                      # CONNECTIONSTATE_RESPONSE
            status = data[7] if len(data) > 7 else 0
            if status != 0x00:
                print(f'[monitor] CONNECTIONSTATE_RESPONSE error 0x{status:02X}', flush=True)

        elif svc == 0x0209:                      # DISCONNECT_REQUEST from gateway
            print('[monitor] gateway requested disconnect', flush=True)
            disc_resp = _hdr(0x020A, bytes([channel_id, 0x00]))
            sock.sendto(disc_resp, GW)
            monitor_state['running'] = False
            break

        # Yield to the event loop so other asyncio tasks can run
        # (without this the loop can starve the event loop on heavy traffic)
        await asyncio.sleep(0)

    # ── Graceful disconnect ───────────────────────────────────────────────────
    print(f'[monitor] stopping — pkts_received={pkt_count}  '
          f'telegrams_emitted={len(_telegram_buffer)}', flush=True)
    try:
        disc_body = bytes([channel_id, 0x00]) + HPAI_SELF
        sock.sendto(_hdr(0x0209, disc_body), GW)
        await asyncio.sleep(0.15)   # wait for DISCONNECT_RESPONSE
    except Exception:
        pass
    sock.close()


async def _send_telegram_async(xknx, req):
    """Send a KNX telegram via xknx."""
    try:
        from xknx.core.value_reader import ValueReader
        from xknx.telegram import GroupAddress, Telegram
        from xknx.telegram.apci import GroupValueWrite
        from xknx.dpt import DPTBinary

        ga_str = req['address']
        value = req['value']

        if req['action'] == 'write' and value is not None:
            ga = GroupAddress(ga_str)
            if isinstance(value, bool):
                payload = GroupValueWrite(DPTBinary(1 if value else 0))
            elif isinstance(value, int):
                payload = GroupValueWrite(DPTBinary(value))
            else:
                payload = GroupValueWrite(DPTBinary(0))

            telegram = Telegram(destination_address=ga, payload=payload)
            await xknx.telegrams.put(telegram)
    except Exception as e:
        _emit_safe('monitor_error', {'message': f'Send failed: {str(e)}'})


async def _raw_udp_monitor(host, port):
    """Fallback: listen on KNX IP multicast for routing telegrams."""
    import socket
    import struct

    MULTICAST_ADDR = '224.0.23.12'
    MULTICAST_PORT = 3671

    monitor_state['running'] = True
    _emit_safe('monitor_connected', {
        'host': host,
        'port': port,
        'mode': 'multicast',
        'warning': 'xknx not available. Listening on KNX IP multicast (routing mode only).'
    })

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('', MULTICAST_PORT))

        group = socket.inet_aton(MULTICAST_ADDR)
        mreq = group + socket.inet_aton('0.0.0.0')
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setblocking(False)

        while monitor_state['running']:
            try:
                data, addr = sock.recvfrom(1024)
                _process_raw_knxip(data, addr[0])
            except BlockingIOError:
                pass
            await asyncio.sleep(0.01)

        sock.close()
    except Exception as e:
        _emit_safe('monitor_error', {'message': f'UDP monitor error: {str(e)}'})
        monitor_state['running'] = False


def _process_raw_knxip(data, source_ip):
    """Parse raw KNX/IP packet (routing indication)."""
    try:
        if len(data) < 6:
            return
        # KNXnet/IP header: 0x06 0x10 service_type(2) total_length(2)
        if data[0] != 0x06 or data[1] != 0x10:
            return

        service_type = (data[2] << 8) | data[3]

        # Routing indication: 0x0530
        # Tunneling request: 0x0420
        if service_type == 0x0530:
            # cEMI starts at offset 6
            _parse_cemi(data[6:], source_ip)
        elif service_type == 0x0420:
            # Tunneling request: structure_length(1) channel_id(1) seq_counter(1) reserved(1) = 4 bytes header
            if len(data) > 10:
                _parse_cemi(data[10:], source_ip)
    except Exception:
        pass


def _parse_cemi(cemi, source_ip=''):
    """Parse cEMI L_DATA frame and emit telegram.

    KNX cEMI frame layout (from 03_06_03 EMI_IMI standard):
      byte 0      : message code  (0x29=L_DATA.ind, 0x2E=L_DATA.con, 0x11=L_DATA.req)
      byte 1      : add_info_length
      [add_info]  : add_info_length bytes
      ctrl1       : 1 byte
      ctrl2       : 1 byte (bit7 = addr_type: 1=group, 0=individual)
      src_addr    : 2 bytes (individual address)
      dst_addr    : 2 bytes
      data_length : 1 byte = total NPDU octets - 1
                    (a 1-bit DPT-1 telegram has data_length = 1, NPDU = 2 bytes)
      NPDU        : data_length + 1  bytes
                    byte 0: TPCI (7 bits) + APCI high (1 bit)
                    byte 1: APCI (4 bits) + value-for-small-payloads (6 bits)
                    bytes 2+: data bytes for payloads > 6 bits
    """
    try:
        if len(cemi) < 6:
            return

        msg_code = cemi[0]
        # 0x29 = L_DATA.ind, 0x2E = L_DATA.con, 0x11 = L_DATA.req
        if msg_code not in (0x29, 0x2E, 0x11):
            return

        # Skip add_info_length
        add_info_len = cemi[1]
        offset = 2 + add_info_len

        if len(cemi) <= offset + 7:
            return

        ctrl1    = cemi[offset]
        ctrl2    = cemi[offset + 1]
        src_high = cemi[offset + 2]
        src_low  = cemi[offset + 3]
        dst_high = cemi[offset + 4]
        dst_low  = cemi[offset + 5]

        # data_length = NPDU octets - 1  (standard KNX cEMI encoding)
        # Full NPDU size = data_length + 1
        data_length = cemi[offset + 6]
        npdu_size   = data_length + 1

        # Source individual address
        src_addr = f"{(src_high >> 4)}.{src_high & 0x0F}.{src_low}"

        # Destination: group or individual based on ctrl2 bit 7
        addr_type = (ctrl2 >> 7) & 1
        dst_int   = (dst_high << 8) | dst_low

        if addr_type == 1:  # Group address
            dst_addr = f"{dst_int >> 11}/{(dst_int >> 8) & 0x07}/{dst_int & 0xFF}"
        else:
            dst_addr = f"{dst_high >> 4}.{dst_high & 0x0F}.{dst_low}"

        # NPDU (TPCI + APCI + data)
        if len(cemi) < offset + 7 + npdu_size:
            return

        npdu = cemi[offset + 7: offset + 7 + npdu_size]
        if len(npdu) < 2:
            return

        # APCI encoded across first 2 bytes of NPDU:
        #   npdu[0] bits 1-0 = APCI high
        #   npdu[1]           = APCI low (+ value bits for small payloads)
        apci    = ((npdu[0] & 0x03) << 8) | npdu[1]
        service = (apci >> 6) & 0x0F

        services = {
            0x00: 'Read',
            0x01: 'Response',
            0x02: 'Write',
        }
        svc_name = services.get(service, f'0x{service:02X}')

        # Value extraction
        raw_value = None
        if data_length == 1:
            # Small value (up to 6 bits) is in the last 6 bits of npdu[1]
            # Covers DPT-1 (1-bit), DPT-2 (2-bit), DPT-3 (4-bit)
            raw_value = npdu[1] & 0x3F
        elif data_length > 1 and len(npdu) > 2:
            # Payload bytes follow the 2-byte APCI
            raw_value = list(npdu[2:])

        telegram_info = _build_telegram_info(
            src_addr, dst_int, dst_addr, svc_name, raw_value, addr_type == 1,
            ctrl1=ctrl1, ctrl2=ctrl2
        )
        _emit_telegram(telegram_info)

    except Exception:
        pass


def _unwrap_dpt(value):
    """Convert xknx 3.x DPTBinary/DPTArray to plain int or list.

    In xknx 2.x GroupValueWrite.value was already a plain int/list.
    In xknx 3.x it is wrapped in DPTBinary (int) or DPTArray (tuple).
    """
    if value is None:
        return None
    try:
        from xknx.dpt import DPTBinary, DPTArray
        if isinstance(value, DPTBinary):
            return value.value          # int
        if isinstance(value, DPTArray):
            return list(value.value)    # tuple → list
    except ImportError:
        pass
    # Already plain
    if isinstance(value, (int, list)):
        return value
    if isinstance(value, tuple):
        return list(value)
    return value


def _process_telegram(telegram):
    """Process a telegram received from xknx."""
    try:
        from xknx.telegram.apci import GroupValueWrite, GroupValueRead, GroupValueResponse

        dst = telegram.destination_address
        src = telegram.source_address
        payload = telegram.payload

        # Convert xknx GroupAddress string "M/S/D" to integer
        dst_int = 0
        try:
            dst_str = str(dst)
            parts = [int(p) for p in dst_str.split('/')]
            if len(parts) == 3:
                dst_int = (parts[0] << 11) | (parts[1] << 8) | parts[2]
            elif len(parts) == 2:
                dst_int = (parts[0] << 11) | parts[1]
        except Exception:
            dst_int = 0

        dst_addr = str(dst)
        src_addr = str(src) if src else ''

        if isinstance(payload, GroupValueWrite):
            svc_name = 'Write'
            raw_value = _unwrap_dpt(payload.value)
        elif isinstance(payload, GroupValueRead):
            svc_name = 'Read'
            raw_value = None
        elif isinstance(payload, GroupValueResponse):
            svc_name = 'Response'
            raw_value = _unwrap_dpt(payload.value)
        else:
            svc_name = type(payload).__name__
            raw_value = None

        telegram_info = _build_telegram_info(
            src_addr, dst_int, dst_addr, svc_name, raw_value, True
        )
        _emit_telegram(telegram_info)

    except Exception as e:
        import traceback
        print(f'[monitor] _process_telegram error: {e}\n{traceback.format_exc()}', flush=True)


def _build_telegram_info(src_addr, dst_int, dst_addr, service, raw_value, is_group,
                         ctrl1=0xBC, ctrl2=0xE0):
    """Build telegram info dict with GA name and decoded value.

    ctrl1/ctrl2 are cEMI control bytes; defaults represent a first-attempt
    standard frame, Low priority, group address destination (the most common
    L_DATA.ind pattern on healthy KNX TP — ctrl1=0xBC, ctrl2=0xE0).
    """
    ga_info = ga_lookup.get(dst_int, {}) if is_group else {}
    ga_name = ga_info.get('name', '')
    dpt     = ga_info.get('dpt', '')
    decoded = _decode_value(raw_value, dpt)

    monitor_state['telegram_count'] += 1

    # ctrl1 bit 6 (0x40): 0 = original frame, 1 = retransmission (sender had no ACK)
    repeat     = bool(ctrl1 & 0x40)
    # ctrl1 bits [4,3]: priority — 0=System 1=Alarm 2=Normal 3=Low
    priority_n = (ctrl1 >> 3) & 0x03
    # ctrl2 bits [6,4]: remaining hop count — 6=same line, decremented by each router
    hop_count  = (ctrl2 >> 4) & 0x07

    return {
        'timestamp':       datetime.now().strftime('%H:%M:%S.%f')[:-3],
        'ts':              time.time(),
        'source':          src_addr,
        'destination':     dst_addr,
        'destination_int': dst_int,
        'service':         service,
        'ga_name':         ga_name,
        'dpt':             dpt,
        'raw_value':       str(raw_value) if raw_value is not None else '',
        'decoded_value':   decoded,
        'count':           monitor_state['telegram_count'],
        'repeat':          repeat,
        'priority_n':      priority_n,
        'hop_count':       hop_count,
    }


def _decode_value(raw_value, dpt):
    """Decode a raw DPT value to human-readable string."""
    if raw_value is None:
        return ''

    try:
        dpt_str = str(dpt) if dpt else ''

        # 1-bit values
        if 'DPST-1-' in dpt_str or dpt_str == 'DPT-1':
            v = raw_value if isinstance(raw_value, int) else (raw_value[0] if raw_value else 0)
            bit = v & 0x01
            if 'DPST-1-1' in dpt_str:  # Switch
                return 'ON' if bit else 'OFF'
            elif 'DPST-1-8' in dpt_str:  # Up/Down
                return 'DOWN' if bit else 'UP'
            elif 'DPST-1-9' in dpt_str:  # Open/Close
                return 'CLOSE' if bit else 'OPEN'
            return '1' if bit else '0'

        # 2-bit (dimming control)
        if 'DPST-3-' in dpt_str or dpt_str == 'DPT-3':
            v = raw_value if isinstance(raw_value, int) else (raw_value[0] if raw_value else 0)
            direction = (v >> 3) & 0x01
            speed = v & 0x07
            return f"{'Up' if direction else 'Down'} speed={speed}"

        # 1-byte unsigned (0-255 or 0-100%)
        if 'DPST-5-' in dpt_str or dpt_str == 'DPT-5':
            if isinstance(raw_value, list) and len(raw_value) >= 1:
                v = raw_value[0]
            elif isinstance(raw_value, int):
                v = raw_value
            else:
                return str(raw_value)
            if 'DPST-5-1' in dpt_str:  # Percentage 0-100%
                return f"{round(v / 2.55, 1)}%"
            return str(v)

        # 2-byte float (EIS5 / DPT-9)
        if 'DPST-9-' in dpt_str or dpt_str == 'DPT-9':
            if isinstance(raw_value, list) and len(raw_value) >= 2:
                byte1, byte2 = raw_value[0], raw_value[1]
                # KNX 16-bit float: SEEEEMMM MMMMMMMM
                sign = (byte1 >> 7) & 1
                exp = (byte1 >> 3) & 0x0F
                mant = ((byte1 & 0x07) << 8) | byte2
                if sign:
                    mant = mant - 2048
                value = 0.01 * mant * (2 ** exp)
                suffix = ''
                if 'DPST-9-1' in dpt_str:
                    suffix = '°C'
                elif 'DPST-9-4' in dpt_str:
                    suffix = ' lux'
                elif 'DPST-9-5' in dpt_str:
                    suffix = ' m/s'
                return f"{value:.1f}{suffix}"

        # 2-byte unsigned int
        if 'DPST-7-' in dpt_str or dpt_str == 'DPT-7':
            if isinstance(raw_value, list) and len(raw_value) >= 2:
                return str((raw_value[0] << 8) | raw_value[1])

        # 4-byte float
        if 'DPST-14-' in dpt_str or dpt_str == 'DPT-14':
            if isinstance(raw_value, list) and len(raw_value) >= 4:
                import struct
                v = struct.unpack('>f', bytes(raw_value[:4]))[0]
                return f"{v:.2f}"

        # Scene number
        if 'DPST-17-' in dpt_str or dpt_str == 'DPT-17':
            if isinstance(raw_value, list) and raw_value:
                return f"Scene {(raw_value[0] & 0x3F) + 1}"

        # String fallback
        if isinstance(raw_value, list):
            if len(raw_value) == 1:
                return str(raw_value[0])
            return ' '.join(f'{b:02X}' for b in raw_value)
        elif isinstance(raw_value, int):
            return str(raw_value)

        return str(raw_value)

    except Exception:
        return str(raw_value) if raw_value is not None else ''


def _emit_telegram(telegram_info):
    """Store telegram in buffer (for REST polling) and push via SocketIO."""
    global _telegram_seq
    with _telegram_buffer_lock:
        _telegram_seq += 1
        telegram_info['seq'] = _telegram_seq
        _telegram_buffer.append(telegram_info)
    # SocketIO push — works when not behind a buffering proxy
    socketio.emit('telegram', telegram_info)


def _reset_telegram_buffer():
    global _telegram_seq
    with _telegram_buffer_lock:
        _telegram_buffer.clear()
        _telegram_seq = 0


# ── Bus metrics ────────────────────────────────────────────────────────────────

@app.route('/api/monitor/metrics', methods=['GET'])
def monitor_metrics():
    """Compute KNX bus quality metrics from the current telegram buffer."""
    from collections import Counter

    with _telegram_buffer_lock:
        tgs = list(_telegram_buffer)

    n = len(tgs)
    if n == 0:
        return jsonify({'empty': True, 'total': 0})

    # ── Time window & rate ────────────────────────────────────────────────────
    ts_vals  = [t['ts'] for t in tgs if 'ts' in t]
    duration = (max(ts_vals) - min(ts_vals)) if len(ts_vals) > 1 else 0
    rate     = round(n / duration, 1) if duration > 0 else 0.0

    # ── Repeat rate ───────────────────────────────────────────────────────────
    repeat_count = sum(1 for t in tgs if t.get('repeat'))
    repeat_rate  = round(repeat_count / n * 100, 1)

    # ── Bus load estimate (KNX TP 9600 baud) ─────────────────────────────────
    # A typical telegram occupies ~30 ms on the bus (frame tx ~22 ms +
    # ACK slot ~4 ms + inter-frame gap ~4 ms) → theoretical max ~33 tg/s.
    bus_load = round(min(rate / 33.0 * 100, 100.0), 1) if rate > 0 else 0.0

    # ── Priority distribution ─────────────────────────────────────────────────
    _prio = {0: 'System', 1: 'Alarm', 2: 'Normal', 3: 'Low'}
    prio_counts = dict(Counter(_prio.get(t.get('priority_n', 3), 'Low') for t in tgs))

    # ── Hop count distribution ────────────────────────────────────────────────
    hop_counts = {str(k): v for k, v in Counter(t.get('hop_count', 6) for t in tgs).items()}

    # ── Source analysis ───────────────────────────────────────────────────────
    src_counter = Counter(t.get('source', '') for t in tgs if t.get('source'))
    top_sources = [
        {'source': s, 'count': c, 'pct': round(c / n * 100, 1)}
        for s, c in src_counter.most_common(10)
    ]

    # Mark known/unknown against the loaded ETS project
    known_ias = set()
    if project_data:
        for dev in project_data.get('devices', []):
            ia = dev.get('individual_address', '')
            if ia:
                known_ias.add(ia)
    for s in top_sources:
        s['known'] = (s['source'] in known_ias) if known_ias else True

    unknown_srcs = sorted({
        t.get('source', '') for t in tgs
        if t.get('source') and known_ias and t.get('source') not in known_ias
    })

    # ── Noisy sources: > 5 msg/s sustained AND > 20 % of all traffic ─────────
    noisy = []
    if duration > 0:
        for s in top_sources:
            if (s['count'] / duration) > 5 and s['pct'] > 20:
                noisy.append(s['source'])

    # ── Active group addresses ────────────────────────────────────────────────
    active_gas = len({t.get('destination', '') for t in tgs if t.get('destination')})

    return jsonify({
        'total':           n,
        'duration_s':      round(duration, 1),
        'rate':            rate,
        'repeat_count':    repeat_count,
        'repeat_rate':     repeat_rate,
        'bus_load_pct':    bus_load,
        'priority':        prio_counts,
        'hop_count':       hop_counts,
        'top_sources':     top_sources,
        'unknown_sources': unknown_srcs,
        'noisy_sources':   noisy,
        'active_gas':      active_gas,
    })


def _emit_safe(event, data):
    """Thread-safe SocketIO emit."""
    socketio.emit(event, data)


def _stop_monitor():
    """Stop the running monitor."""
    monitor_state['running'] = False

    # Stop xknx if running
    xknx = monitor_state.get('xknx_instance')
    loop = monitor_state.get('loop')
    if xknx and loop and not loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(xknx.stop(), loop)
        except Exception:
            pass

    monitor_state['xknx_instance'] = None




# ──────────────────────────────────────────────
# DPT encoding helpers
# ──────────────────────────────────────────────
def _encode_dpt9(value):
    """Encode a float to KNX 2-byte float (DPT-9) format."""
    sign = 1 if value < 0 else 0
    f = value * 100.0
    exp = 0
    while abs(f) > 2047 and exp < 15:
        f /= 2.0
        exp += 1
    while abs(f) < 1.0 and exp > 0 and f != 0:
        f *= 2.0
        exp -= 1
    m = int(round(f))
    if m < 0:
        m = m + 2048
    b1 = (sign << 7) | ((exp & 0x0F) << 3) | ((m >> 8) & 0x07)
    b2 = m & 0xFF
    return [b1, b2]


def _encode_dpt(value, dpt):
    """Encode user value to (mode, data) for KNX GroupWrite.
    mode 'binary' -> small value fits in APCI (0-63)
    mode 'array'  -> list of bytes as payload
    """
    d = str(dpt) if dpt else ''

    # DPT-1: 1-bit boolean
    if d.startswith('DPT-1') or 'DPST-1-' in d:
        v = 1 if str(value).lower() in ('1', 'true', 'on', 'yes') else 0
        return ('binary', v & 0x01)

    # DPT-3: 4-bit dimming / blinds control
    if d.startswith('DPT-3') or 'DPST-3-' in d:
        try:
            v = int(value) & 0x0F
        except Exception:
            v = 0
        return ('binary', v)

    # DPT-5: 1-byte unsigned (0-255)
    if d.startswith('DPT-5') or 'DPST-5-' in d:
        try:
            fv = float(value)
            if 'DPST-5-1' in d:          # percentage 0-100 → raw 0-255
                fv = fv * 255.0 / 100.0
            v = min(255, max(0, int(round(fv))))
        except Exception:
            v = 0
        return ('array', [v])

    # DPT-6: 1-byte signed (-128..127)
    if d.startswith('DPT-6') or 'DPST-6-' in d:
        try:
            v = min(127, max(-128, int(float(value))))
            if v < 0:
                v += 256
        except Exception:
            v = 0
        return ('array', [v & 0xFF])

    # DPT-7: 2-byte unsigned int (0-65535)
    if d.startswith('DPT-7') or 'DPST-7-' in d:
        try:
            v = min(65535, max(0, int(float(value))))
        except Exception:
            v = 0
        return ('array', [(v >> 8) & 0xFF, v & 0xFF])

    # DPT-8: 2-byte signed int (-32768..32767)
    if d.startswith('DPT-8') or 'DPST-8-' in d:
        try:
            v = min(32767, max(-32768, int(float(value))))
            if v < 0:
                v += 65536
        except Exception:
            v = 0
        return ('array', [(v >> 8) & 0xFF, v & 0xFF])

    # DPT-9: 2-byte float
    if d.startswith('DPT-9') or 'DPST-9-' in d:
        try:
            f = float(value)
        except Exception:
            f = 0.0
        return ('array', _encode_dpt9(f))

    # DPT-13: 4-byte signed int
    if d.startswith('DPT-13') or 'DPST-13-' in d:
        try:
            v = int(float(value))
            v = v & 0xFFFFFFFF
        except Exception:
            v = 0
        return ('array', [(v >> 24) & 0xFF, (v >> 16) & 0xFF,
                          (v >> 8) & 0xFF, v & 0xFF])

    # DPT-17 / DPT-18: Scene number (1-64)
    if d.startswith('DPT-17') or 'DPST-17-' in d or        d.startswith('DPT-18') or 'DPST-18-' in d:
        try:
            v = min(64, max(1, int(float(value)))) - 1
        except Exception:
            v = 0
        return ('array', [v & 0x3F])

    # Fallback: try int
    try:
        v = int(float(value))
        if 0 <= v <= 63:
            return ('binary', v)
        return ('array', [v & 0xFF])
    except Exception:
        return ('binary', 0)


async def _do_write_async(xknx_inst, address_str, value, dpt):
    """Send a GroupWrite telegram via an existing xknx instance."""
    from xknx.telegram import GroupAddress, Telegram
    from xknx.telegram.apci import GroupValueWrite
    from xknx.dpt import DPTBinary, DPTArray

    mode, data = _encode_dpt(value, dpt)
    ga = GroupAddress(address_str)
    if mode == 'binary':
        payload = GroupValueWrite(DPTBinary(data))
    else:
        payload = GroupValueWrite(DPTArray(data))
    telegram = Telegram(destination_address=ga, payload=payload)
    await xknx_inst.telegrams.put(telegram)
    await asyncio.sleep(0.3)


async def _temp_write_async(host, port, address_str, value, dpt):
    """One-shot: connect, write, disconnect."""
    from xknx import XKNX
    from xknx.io import ConnectionConfig, ConnectionType
    conn_config = ConnectionConfig(
        connection_type=ConnectionType.TUNNELING,
        gateway_ip=host,
        gateway_port=port,
    )
    xknx_inst = XKNX(connection_config=conn_config)
    await xknx_inst.start()
    try:
        await _do_write_async(xknx_inst, address_str, value, dpt)
    finally:
        await xknx_inst.stop()


# ──────────────────────────────────────────────
# Ping device (DeviceDescriptorRead via management layer)
# ──────────────────────────────────────────────

async def _do_ping_async(xknx_inst, ia_str: str, timeout: float = 3.0) -> dict:
    """
    KNX 'ping' using xknx management layer (P2P connection + DeviceDescriptorRead).

    Root cause of the old broken approach: DeviceDescriptorResponse telegrams are
    addressed to xknx.current_address (our tunnel address), so the CEMIHandler routes
    them to xknx.management.process() — NOT through telegram_queue callbacks.
    The correct approach is xknx.management.connection() → connection.request(), which
    is exactly what nm_individual_address_check() does in xknx.management.procedures.

    Returns {'online': bool, 'address': str}.
    """
    try:
        from xknx.management.procedures import nm_individual_address_check
    except ImportError as e:
        return {'online': False, 'address': ia_str, 'error': f'xknx import: {e}'}

    try:
        online = await asyncio.wait_for(
            nm_individual_address_check(xknx_inst, ia_str),
            timeout=timeout + 3   # nm_individual_address_check has internal 6s timeout
        )
        return {'online': online, 'address': ia_str}
    except asyncio.TimeoutError:
        return {'online': False, 'address': ia_str}
    except Exception as e:
        return {'online': False, 'address': ia_str, 'error': str(e)}


async def _temp_ping_async(host: str, port: int, ia_str: str, timeout: float = 3.0) -> dict:
    """One-shot connection to ping a device."""
    from xknx import XKNX
    from xknx.io import ConnectionConfig, ConnectionType
    xknx_inst = XKNX(connection_config=ConnectionConfig(
        connection_type=ConnectionType.TUNNELING,
        gateway_ip=host,
        gateway_port=port,
    ))
    await xknx_inst.start()
    try:
        return await _do_ping_async(xknx_inst, ia_str, timeout)
    finally:
        await xknx_inst.stop()


@app.route('/api/ping_device', methods=['POST'])
def ping_device():
    """Ping a KNX device by individual address using DeviceDescriptorRead."""
    data    = request.json or {}
    address = data.get('address', '').strip()
    host    = data.get('host', '').strip()
    port    = int(data.get('port', 3671))
    timeout = float(KNX_PING_TIMEOUT)

    if not address:
        return jsonify({'error': 'Endereço individual inválido'}), 400

    # Reuse active monitor connection if available
    if monitor_state['running'] and monitor_state.get('xknx_instance') and monitor_state.get('loop'):
        try:
            future = asyncio.run_coroutine_threadsafe(
                _do_ping_async(monitor_state['xknx_instance'], address, timeout),
                monitor_state['loop']
            )
            return jsonify(future.result(timeout=timeout + 4))
        except Exception as e:
            return jsonify({'error': str(e), 'online': False}), 500

    if not host and DEFAULT_KNX_HOST:
        host = DEFAULT_KNX_HOST
        port = DEFAULT_KNX_PORT

    if not host:
        return jsonify({'error': 'Sem ligação KNX activa. Active o Group Monitor ou forneça o IP do gateway.'}), 400

    try:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(_temp_ping_async(host, port, address, timeout))
        loop.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e), 'online': False}), 500


# ──────────────────────────────────────────────
# Diagnose all devices from ETS project
# ──────────────────────────────────────────────

# Flag de abort do diagnóstico (acesso thread-safe — escrita simples de bool em Python é atómica)
diag_abort    = False
diag_progress = {'current': 0, 'total': 0, 'name': ''}


@app.route('/api/diagnose/stop', methods=['POST'])
def diagnose_stop():
    """Abort a running diagnosis."""
    global diag_abort
    diag_abort = True
    return jsonify({'success': True})


@app.route('/api/diagnose/progress', methods=['GET'])
def diagnose_progress():
    """Return current diagnosis progress."""
    return jsonify(diag_progress)


@app.route('/api/diagnose', methods=['POST'])
def diagnose_devices():
    """Check online status of all devices from the loaded ETS project."""
    global diag_abort
    diag_abort = False  # reset flag at start of new diagnosis

    data    = request.json or {}
    host    = data.get('host', '').strip()
    port    = int(data.get('port', 3671))
    timeout = float(KNX_PING_TIMEOUT)

    if not project_data:
        return jsonify({'error': 'Sem projecto carregado'}), 400

    # Collect all devices with individual addresses from topology
    devices = []
    for area in project_data.get('topology', []):
        for line in area.get('lines', []):
            for dev in line.get('devices', []):
                ia = dev.get('individual_address', '').strip()
                if ia and ia != '0.0.0':
                    devices.append({
                        'address': ia,
                        'name': dev.get('name') or dev.get('description') or ia,
                        'manufacturer': dev.get('manufacturer', ''),
                        'line': f"{area.get('name','')} / {line.get('name','')}",
                    })

    if not devices:
        return jsonify({'error': 'Sem dispositivos com endereço individual no projecto'}), 400

    async def _run_diagnosis(xknx_inst):
        global diag_progress
        diag_progress = {'current': 0, 'total': len(devices), 'name': ''}
        results = []
        for i, dev in enumerate(devices, 1):
            if diag_abort:
                break
            diag_progress = {'current': i, 'total': len(devices), 'name': dev['name']}
            r = await _do_ping_async(xknx_inst, dev['address'], timeout)
            r['name'] = dev['name']
            r['manufacturer'] = dev['manufacturer']
            r['line'] = dev['line']
            results.append(r)
        diag_progress = {'current': 0, 'total': 0, 'name': ''}
        return results

    # Use active monitor connection if available
    if monitor_state['running'] and monitor_state.get('xknx_instance') and monitor_state.get('loop'):
        try:
            future = asyncio.run_coroutine_threadsafe(
                _run_diagnosis(monitor_state['xknx_instance']),
                monitor_state['loop']
            )
            per_device = timeout + 4   # ping timeout + management overhead
            results = future.result(timeout=len(devices) * per_device + 10)
            return jsonify({'results': results, 'total': len(results), 'aborted': diag_abort})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    if not host and DEFAULT_KNX_HOST:
        host = DEFAULT_KNX_HOST
        port = DEFAULT_KNX_PORT

    if not host:
        return jsonify({'error': 'Sem ligação KNX activa. Active o Group Monitor ou forneça o IP do gateway.'}), 400

    async def _run_with_temp_conn():
        from xknx import XKNX
        from xknx.io import ConnectionConfig, ConnectionType
        xknx_inst = XKNX(connection_config=ConnectionConfig(
            connection_type=ConnectionType.TUNNELING,
            gateway_ip=host,
            gateway_port=port,
        ))
        await xknx_inst.start()
        try:
            return await _run_diagnosis(xknx_inst)
        finally:
            await xknx_inst.stop()

    try:
        loop = asyncio.new_event_loop()
        per_device = timeout + 4
        results = loop.run_until_complete(
            asyncio.wait_for(_run_with_temp_conn(),
                             timeout=len(devices) * per_device + 15)
        )
        loop.close()
        return jsonify({'results': results, 'total': len(results), 'aborted': diag_abort})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ──────────────────────────────────────────────
# Write GA endpoint
# ──────────────────────────────────────────────
@app.route('/api/write_ga', methods=['POST'])
def write_ga():
    """Write a value to a KNX group address."""
    data    = request.json or {}
    address = data.get('address', '').strip()
    value   = data.get('value')
    dpt     = data.get('dpt', '')
    host    = data.get('host', '').strip()
    port    = int(data.get('port', 3671))

    print(f'[write_ga] address={address!r}  value={value!r}  dpt={dpt!r}  host={host!r}  port={port}', flush=True)

    if not address:
        return jsonify({'error': 'Endereço de grupo não especificado'}), 400
    if value is None:
        return jsonify({'error': 'Valor não especificado'}), 400

    # Use existing monitor connection if active
    if monitor_state['running'] and monitor_state.get('xknx_instance') and monitor_state.get('loop'):
        try:
            future = asyncio.run_coroutine_threadsafe(
                _do_write_async(monitor_state['xknx_instance'], address, value, dpt),
                monitor_state['loop']
            )
            future.result(timeout=5)
            return jsonify({'success': True, 'via': 'monitor'})
        except Exception as e:
            return jsonify({'error': f'Erro ao enviar via monitor: {str(e)}'}), 500

    # Temporary connection
    if not host and project_data and project_data.get('ip_connections'):
        ip   = project_data['ip_connections'][0]
        host = ip['host']
        port = ip['port']

    if not host and DEFAULT_KNX_HOST:
        host = DEFAULT_KNX_HOST
        port = DEFAULT_KNX_PORT

    if not host:
        return jsonify({'error': 'Sem ligação KNX activa e sem gateway configurado'}), 400

    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_temp_write_async(host, port, address, value, dpt))
        loop.close()
        return jsonify({'success': True, 'via': 'temp', 'host': host, 'port': port})
    except Exception as e:
        import traceback
        return jsonify({'error': f'Ligação a {host}:{port} falhou — {str(e)}'}), 500

# ──────────────────────────────────────────────

# -----------------------------------------------
# Auto-load last project on startup (HA add-on)
# -----------------------------------------------
def _autoload_last_project():
    global project_data, ga_lookup

    # Priority 1: file configured in add-on options (from /config/<folder>/<file>)
    load_path = None
    if KNX_PROJECT_FILE and KNX_PROJECTS_PATH:
        candidate = os.path.join(KNX_PROJECTS_PATH, KNX_PROJECT_FILE)
        if os.path.exists(candidate):
            load_path = candidate
            print("  A carregar projecto das opcoes: " + candidate, flush=True)
        else:
            print("  AVISO: ficheiro nao encontrado em " + candidate, flush=True)
            print("  Certifica-te que a pasta /config/" + KNX_PROJECT_FOLDER + "/ existe e contem o ficheiro.", flush=True)

    # Priority 2: last uploaded file cached in /data/projects
    if not load_path:
        cached = os.path.join(PROJECTS_DIR, 'last_project.knxproj')
        if os.path.exists(cached):
            load_path = cached
            print("  A restaurar projecto em cache: " + cached, flush=True)

    if not load_path:
        print("  Nenhum projecto configurado. Defina knx_project_file nas opcoes do add-on.", flush=True)
        return

    try:
        with open(load_path, 'rb') as fp:
            file_bytes = fp.read()
        result = parse_knxproj(file_bytes)
        project_data = result
        ga_lookup = {}
        for ga in result.get('group_addresses', []):
            ga_lookup[ga['address_int']] = ga
        print("  Projecto carregado: " + str(result['project'].get('name', '?')), flush=True)
    except Exception as e:
        print("  Erro ao carregar projecto: " + str(e), flush=True)

_autoload_last_project()

# -----------------------------------------------
# Main
# -----------------------------------------------
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    port = int(os.environ.get('PORT', 5000))
    host = os.environ.get('HOST', '0.0.0.0')
    is_ha = os.environ.get('PROJECTS_DIR', '') != ''

    if not is_ha:
        import webbrowser
        import socket as _socket
        try:
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            local_ip = s.getsockname()[0]
            s.close()
        except Exception:
            local_ip = '127.0.0.1'
        print("=" * 60)
        print("  KNX Field Tool")
        print("=" * 60)
        print("  Local:   http://localhost:" + str(port))
        print("  Rede:    http://" + local_ip + ":" + str(port))
        print("=" * 60)
        def open_browser():
            import time
            time.sleep(1.5)
            webbrowser.open('http://localhost:' + str(port))
        threading.Thread(target=open_browser, daemon=True).start()
    else:
        print("  KNX Field Tool (HA Add-on) a ouvir em " + host + ":" + str(port), flush=True)

    try:
        socketio.run(app, host=host, port=port, debug=False, use_reloader=False,
                     allow_unsafe_werkzeug=(_ASYNC_MODE == 'threading'))
    except Exception as e:
        import traceback
        print("ERRO ao iniciar servidor: " + str(e), flush=True)
        traceback.print_exc()
        raise
