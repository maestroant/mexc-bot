"""Замер задержки до MEXC с текущей машины (для выбора сервера).

Что меряет (по --samples замеров, печатает min / медиану / p90 / max):
  rest_new       новый HTTPS-запрос с нуля: DNS + TCP + TLS + ответ   GET /api/v3/time
  rest_keepalive запрос по уже открытому соединению (так работает бот) GET /api/v3/time
  depth          стакан по открытому соединению                        GET /api/v3/depth
  signed         подписанный запрос (если есть ключи)                  GET /api/v3/selfSymbols
  ws_connect     установка WebSocket                                   wss://wbs-api.mexc.com/ws
  ws_ping        PING -> PONG по открытому WebSocket

Только стандартная библиотека: на чистом сервере нужен лишь python3.
Ключи (необязательно): MEXC_API_KEY / MEXC_API_SECRET или .env рядом / в корне проекта.

  python3 latency_bench.py
  python3 latency_bench.py --samples 50
"""

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import os
import socket
import ssl
import statistics
import struct
import time
import urllib.parse
import urllib.request
from pathlib import Path

API_HOST = "api.mexc.com"
WS_HOST = "wbs-api.mexc.com"


def load_dotenv() -> None:
    here = Path(__file__).resolve().parent
    for path in (here / ".env", here.parent / ".env"):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def ms(t0: float) -> float:
    return (time.perf_counter() - t0) * 1000


def report(name: str, values: list[float]) -> None:
    if not values:
        print(f"{name:<15} нет данных")
        return
    v = sorted(values)
    p90 = v[min(len(v) - 1, int(len(v) * 0.9))]
    print(f"{name:<15} min {v[0]:7.1f}  med {statistics.median(v):7.1f}  p90 {p90:7.1f}  max {v[-1]:7.1f}  ms  (n={len(v)})")


def get_json(conn: http.client.HTTPSConnection, path: str, headers: dict | None = None) -> tuple[int, object, dict]:
    conn.request("GET", path, headers=headers or {})
    r = conn.getresponse()
    body = r.read()
    try:
        data = json.loads(body)
    except ValueError:
        data = body[:200]
    return r.status, data, dict(r.getheaders())


def bench_rest(samples: int, key: str | None, secret: str | None) -> None:
    new, keepalive, depth, signed = [], [], [], []

    for _ in range(samples):
        t0 = time.perf_counter()
        c = http.client.HTTPSConnection(API_HOST, timeout=10)
        get_json(c, "/api/v3/time")
        new.append(ms(t0))
        c.close()

    conn = http.client.HTTPSConnection(API_HOST, timeout=10)
    _, data, headers = get_json(conn, "/api/v3/time")  # прогрев соединения
    offset = data["serverTime"] - int(time.time() * 1000) if isinstance(data, dict) else None
    edge = headers.get("Akamai-GRN") or headers.get("X-Cache")
    print(f"Смещение часов относительно биржи: {offset} ms;  Akamai: {edge}")

    for _ in range(samples):
        t0 = time.perf_counter()
        get_json(conn, "/api/v3/time")
        keepalive.append(ms(t0))
    for _ in range(samples):
        t0 = time.perf_counter()
        get_json(conn, "/api/v3/depth?symbol=BTCUSDT&limit=20")
        depth.append(ms(t0))

    if key and secret:
        for i in range(samples):
            query = urllib.parse.urlencode({"timestamp": int(time.time() * 1000) + (offset or 0), "recvWindow": 5000})
            sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            t0 = time.perf_counter()
            status, data, _ = get_json(conn, f"/api/v3/selfSymbols?{query}&signature={sig}", {"X-MEXC-APIKEY": key})
            elapsed = ms(t0)
            if status != 200:
                print(f"signed: HTTP {status} {data}")
                break
            signed.append(elapsed)
    conn.close()

    report("rest_new", new)
    report("rest_keepalive", keepalive)
    report("depth", depth)
    report("signed", signed) if key else print("signed          пропущено: нет ключей")


def ws_send_text(sock: ssl.SSLSocket, text: str) -> None:
    payload = text.encode()
    mask = os.urandom(4)
    header = bytes([0x81])
    n = len(payload)
    header += bytes([0x80 | n]) if n < 126 else bytes([0x80 | 126]) + struct.pack(">H", n)
    sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))


def ws_recv(sock: ssl.SSLSocket) -> bytes:
    def read(n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("WS закрыт")
            buf += chunk
        return buf

    b1, b2 = read(2)
    n = b2 & 0x7F
    if n == 126:
        n = struct.unpack(">H", read(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", read(8))[0]
    return read(n)


def ws_open() -> tuple[ssl.SSLSocket, str | None]:
    raw = socket.create_connection((WS_HOST, 443), timeout=10)
    sock = ssl.create_default_context().wrap_socket(raw, server_hostname=WS_HOST)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((f"GET /ws HTTP/1.1\r\nHost: {WS_HOST}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(4096)
    head = resp.split(b"\r\n\r\n")[0].decode(errors="replace")
    if " 101 " not in head.split("\r\n")[0]:
        raise ConnectionError(head.split("\r\n")[0])
    pop = next((l.split(":", 1)[1].strip() for l in head.split("\r\n") if l.lower().startswith("x-amz-cf-pop")), None)
    return sock, pop


def bench_ws(samples: int) -> None:
    connect, ping = [], []
    pop = None
    for _ in range(min(samples, 10)):  # установка WS дорогая, хватит 10
        t0 = time.perf_counter()
        sock, pop = ws_open()
        connect.append(ms(t0))
        sock.close()

    sock, _ = ws_open()
    for _ in range(samples):
        t0 = time.perf_counter()
        ws_send_text(sock, '{"method":"PING"}')
        while b"PONG" not in ws_recv(sock):
            pass
        ping.append(ms(t0))
    sock.close()

    print(f"CloudFront POP: {pop}")
    report("ws_connect", connect)
    report("ws_ping", ping)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--samples", type=int, default=30)
    args = p.parse_args()

    load_dotenv()
    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
    except Exception:
        ip = "?"
    print(f"Внешний IP этой машины: {ip}  (его нужно добавить в белый список API-ключа)\n")

    bench_rest(args.samples, os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"))
    print()
    bench_ws(args.samples)


if __name__ == "__main__":
    main()
