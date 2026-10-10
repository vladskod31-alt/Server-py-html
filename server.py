#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Нестабільний (chaos) HTTP-сервер на чистому Python.

Сервер спеціально поводиться ненадійно: випадково тормозить, повертає 500/503,
рве з'єднання, обриває тіло відповіді та влаштовує "аварії" (outage windows).
Рівень нестабільності перемикається на льоту через /admin/chaos.

Тільки стандартна бібліотека — жодних зовнішніх залежностей.

Приклад:
    python3 server.py --port 8000 --level medium
    curl http://127.0.0.1:8000/ping
    curl -X POST http://127.0.0.1:8000/admin/chaos -d '{"level":"heavy"}'
"""

from __future__ import annotations

import argparse
import json
import os
import random
import socket
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "1.0.0"
STARTED_AT = time.time()
ROOT = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(ROOT, "static")

# ---------------------------------------------------------------------------
# Налаштування хаосу
# ---------------------------------------------------------------------------
# outcomes — ваги результатів у відсотках:
#   ok      -> звичайна відповідь
#   slow    -> довга затримка, потім 200
#   err500  -> Internal Server Error
#   err503  -> Service Unavailable
#   drop    -> з'єднання рветься без жодного байта відповіді
#   partial -> заголовки + половина тіла, потім обрив
#   hang    -> підвисає довше за будь-який розумний таймаут клієнта
CHAOS_PRESETS = {
    "off": {
        "latency": (0.0, 0.02),
        "slow_seconds": (0.05, 0.3),
        "hang_seconds": (1.0, 2.0),
        "outage_chance": 0.0,
        "outage_seconds": (0, 0),
        "outage_cooldown": 9999,
        "outcomes": {"ok": 100},
    },
    "light": {
        "latency": (0.0, 0.25),
        "slow_seconds": (0.5, 2.0),
        "hang_seconds": (3.0, 8.0),
        "outage_chance": 0.002,
        "outage_seconds": (1, 4),
        "outage_cooldown": 40,
        "outcomes": {"ok": 88, "slow": 6, "err500": 4, "err503": 2},
    },
    "medium": {
        "latency": (0.0, 0.6),
        "slow_seconds": (1.0, 4.0),
        "hang_seconds": (6.0, 20.0),
        "outage_chance": 0.008,
        "outage_seconds": (1, 5),
        "outage_cooldown": 25,
        "outcomes": {"ok": 70, "slow": 10, "err500": 10, "err503": 4, "drop": 4, "hang": 2},
    },
    "heavy": {
        "latency": (0.0, 1.2),
        "slow_seconds": (2.0, 7.0),
        "hang_seconds": (10.0, 30.0),
        "outage_chance": 0.02,
        "outage_seconds": (2, 8),
        "outage_cooldown": 15,
        "outcomes": {
            "ok": 45, "slow": 15, "err500": 18, "err503": 8,
            "drop": 8, "partial": 3, "hang": 3,
        },
    },
    "insane": {
        "latency": (0.1, 2.0),
        "slow_seconds": (3.0, 12.0),
        "hang_seconds": (15.0, 45.0),
        "outage_chance": 0.08,
        "outage_seconds": (5, 20),
        "outage_cooldown": 8,
        "outcomes": {
            "ok": 18, "slow": 20, "err500": 25, "err503": 10,
            "drop": 10, "partial": 7, "hang": 10,
        },
    },
}


class Chaos:
    """Стан хаосу: поточний рівень, аварії, лічильники, статистика."""

    def __init__(self, level: str, seed: int | None = None) -> None:
        self.lock = threading.RLock()
        self.rng = random.Random(seed)
        self.level = level if level in CHAOS_PRESETS else "medium"
        self.outage_until = 0.0
        self.outage_triggered = 0
        self.outage_allowed_at = 0.0  # cooldown: коли можна влаштувати наступну аварію
        self.counts = {name: 0 for name in
                       ("ok", "slow", "err500", "err503", "drop", "partial", "hang")}
        self.requests = 0
        self.failures = 0
        self.bytes_out = 0
        self.static_served = 0
        self.latencies = deque(maxlen=5000)

    # -- конфіг -------------------------------------------------------------
    @property
    def cfg(self) -> dict:
        return CHAOS_PRESETS[self.level]

    def set_level(self, level: str) -> str:
        if level not in CHAOS_PRESETS:
            raise ValueError(
                "невідомий рівень '%s' (доступні: %s)"
                % (level, ", ".join(CHAOS_PRESETS))
            )
        with self.lock:
            self.level = level
        return level

    def force_outage(self, seconds: float, cooldown: float | None = None) -> float:
        with self.lock:
            duration = max(0.0, seconds)
            self.outage_until = time.time() + duration
            self.outage_triggered += 1
            gap = self.cfg["outage_cooldown"] if cooldown is None else cooldown
            self.outage_allowed_at = self.outage_until + max(0.0, gap)
            return self.outage_until

    def in_outage(self) -> float:
        with self.lock:
            left = self.outage_until - time.time()
        return max(0.0, left)

    def maybe_start_outage(self) -> float:
        """Випадково влаштовує аварію; повертає її тривалість у секундах (0 — якщо не випало)."""
        cfg = self.cfg
        with self.lock:
            if time.time() < self.outage_allowed_at:
                return 0.0  # ще триває cooldown після попередньої аварії
            if self.rng.random() >= cfg["outage_chance"]:
                return 0.0
            lo, hi = cfg["outage_seconds"]
            duration = self.rng.uniform(lo, hi)
        self.force_outage(duration)
        return duration

    def pick_outcome(self) -> str:
        outcomes = self.cfg["outcomes"]
        names = list(outcomes)
        weights = [outcomes[n] for n in names]
        return self.rng.choices(names, weights=weights, k=1)[0]

    def jitter(self) -> float:
        lo, hi = self.cfg["latency"]
        return self.rng.uniform(lo, hi)

    def delay(self, key: str) -> float:
        lo, hi = self.cfg[key]
        return self.rng.uniform(lo, hi)

    # -- статистика ---------------------------------------------------------
    def record(self, outcome: str, latency_ms: float, size: int = 0, failed: bool = False) -> None:
        with self.lock:
            self.requests += 1
            self.bytes_out += size
            self.latencies.append(latency_ms)
            if outcome in self.counts:
                self.counts[outcome] += 1
            if failed:
                self.failures += 1

    def record_static(self, size: int) -> None:
        """Статика не лізе в статистику затримок — лише байти й лічильник."""
        with self.lock:
            self.bytes_out += size
            self.static_served += 1

    def snapshot(self) -> dict:
        with self.lock:
            lat = sorted(self.latencies)
            counts = dict(self.counts)
            requests = self.requests
            failures = self.failures
            level = self.level
            bytes_out = self.bytes_out
            static_served = self.static_served
            outage_left = max(0.0, self.outage_until - time.time())
            outage_triggered = self.outage_triggered

        def pct(p: float) -> float:
            if not lat:
                return 0.0
            idx = min(len(lat) - 1, int(round(p / 100.0 * (len(lat) - 1))))
            return round(lat[idx], 2)

        uptime = time.time() - STARTED_AT
        success = requests - failures
        return {
            "version": VERSION,
            "uptime_s": round(uptime, 2),
            "level": level,
            "preset": CHAOS_PRESETS[level],
            "outage": {"active": outage_left > 0, "seconds_left": round(outage_left, 2),
                       "triggered_total": outage_triggered},
            "requests_total": requests,
            "success_total": success,
            "failures_total": failures,
            "success_rate": round(100.0 * success / requests, 2) if requests else 100.0,
            "bytes_out": bytes_out,
            "static_served": static_served,
            "outcomes": counts,
            "latency_ms": {
                "samples": len(lat),
                "min": round(lat[0], 2) if lat else 0.0,
                "avg": round(sum(lat) / len(lat), 2) if lat else 0.0,
                "p50": pct(50),
                "p95": pct(95),
                "max": round(lat[-1], 2) if lat else 0.0,
            },
        }


CHAOS = Chaos(level=os.environ.get("CHAOS_LEVEL", "medium"))
SERVE_LOG = os.environ.get("SERVE_LOG", "1") not in ("0", "false", "no")
_LOG_LOCK = threading.Lock()


def log(msg: str) -> None:
    if not SERVE_LOG:
        return
    stamp = time.strftime("%H:%M:%S")
    with _LOG_LOCK:
        print("[%s] %s" % (stamp, msg), flush=True)


# ---------------------------------------------------------------------------
# HTTP-обробник
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "UnstableServer/" + VERSION
    protocol_version = "HTTP/1.1"
    chaos = CHAOS

    # --- службове ----------------------------------------------------------
    def log_message(self, fmt: str, *args) -> None:  # вимикаємо стандартний лог
        pass

    def _send(self, code: int, payload: bytes, ctype: str = "application/json; charset=utf-8") -> int:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()
        return len(payload)

    def _json(self, code: int, obj: dict) -> int:
        return self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    def _drop(self) -> None:
        """Рвемо з'єднання без відповіді."""
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def _partial(self, payload: bytes) -> None:
        """Відправляємо заголовки й половину тіла, потім обриваємо."""
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload[: max(1, len(payload) // 2)])
            self.wfile.flush()
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def _read_body(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except (ValueError, UnicodeDecodeError):
            return {}

    # --- хаос --------------------------------------------------------------
    def _apply_chaos(self, route: str) -> bool:
        """Повертає True, якщо запит уже оброблено (або вбито) хаосом."""
        chaos = self.chaos
        started = time.perf_counter()

        def elapsed_ms() -> float:
            return (time.perf_counter() - started) * 1000.0

        outage_left = chaos.in_outage()
        if outage_left <= 0:
            triggered = chaos.maybe_start_outage()
            if triggered > 0:
                outage_left = triggered
                log("⚠ АВАРІЯ! сервер лягає на %.1f с" % triggered)

        outcome = "ok" if outage_left <= 0 else "err503"
        if outcome == "ok":
            outcome = chaos.pick_outcome()

        time.sleep(chaos.jitter())

        if outcome == "ok":
            return False

        if outcome == "slow":
            delay = chaos.delay("slow_seconds")
            time.sleep(delay)
            size = self._json(200, {"ok": True, "route": route, "slow": True,
                                    "delay_s": round(delay, 2),
                                    "ts": time.time()})
            chaos.record(outcome, elapsed_ms(), size)
            log("🐌 %s — повільна відповідь %.2f с" % (route, delay))
            return True

        if outcome == "err500":
            size = self._json(500, {"ok": False, "route": route,
                                    "error": "Internal Server Error",
                                    "detail": "worker впав на обробці запиту"})
            chaos.record(outcome, elapsed_ms(), size, failed=True)
            log("💥 500 %s" % route)
            return True

        if outcome == "err503":
            reason = "outage window" if outage_left > 0 else "service unavailable"
            size = self._json(503, {"ok": False, "route": route,
                                    "error": "Service Unavailable",
                                    "detail": reason,
                                    "retry_after_s": round(outage_left, 2)})
            chaos.record(outcome, elapsed_ms(), size, failed=True)
            log("🚫 503 %s (%s)" % (route, reason))
            return True

        if outcome == "drop":
            chaos.record(outcome, elapsed_ms(), 0, failed=True)
            log("🔌 з'єднання розірвано на %s" % route)
            self._drop()
            return True

        if outcome == "partial":
            payload = json.dumps({"ok": True, "route": route, "data": "x" * 512}).encode("utf-8")
            chaos.record(outcome, elapsed_ms(), len(payload) // 2, failed=True)
            log("✂️ обірване тіло відповіді на %s" % route)
            self._partial(payload)
            return True

        if outcome == "hang":
            delay = chaos.delay("hang_seconds")
            log("⏳ запит %s підвис на %.1f с" % (route, delay))
            time.sleep(delay)
            try:
                size = self._json(200, {"ok": True, "route": route, "hang": True,
                                        "delay_s": round(delay, 2)})
                chaos.record(outcome, elapsed_ms(), size)
            except (BrokenPipeError, ConnectionResetError):
                chaos.record(outcome, elapsed_ms(), 0, failed=True)
                log("👻 клієнт не дочекався %s" % route)
            return True

        return False

    # --- маршрути ----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/") or "/"

        if path in ("/", "/index.html"):
            return self._static("index.html")
        if path.startswith("/static/"):
            return self._static(path[len("/static/"):])

        if path == "/ping":
            return self._ping()
        if path == "/health":
            return self._health()
        if path == "/stats":
            return self._json(200, self.chaos.snapshot())
        if path == "/admin/chaos":
            return self._json(200, {"level": self.chaos.level,
                                    "available": list(CHAOS_PRESETS),
                                    "preset": self.chaos.cfg})
        return self._json(404, {"ok": False, "error": "Not Found", "path": path})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        body = self._read_body()

        if path == "/admin/chaos":
            level = str(body.get("level", "")).strip().lower()
            if not level:
                return self._json(400, {"ok": False, "error": "потрібне поле level",
                                        "available": list(CHAOS_PRESETS)})
            try:
                self.chaos.set_level(level)
            except ValueError as exc:
                return self._json(400, {"ok": False, "error": str(exc)})
            log("🎛 рівень хаосу -> %s" % level)
            return self._json(200, {"ok": True, "level": level, "preset": self.chaos.cfg})

        if path == "/admin/outage":
            try:
                seconds = float(body.get("seconds", 5))
            except (TypeError, ValueError):
                seconds = 5.0
            until = self.chaos.force_outage(seconds)
            log("🎛 примусова аварія на %.1f с" % seconds)
            return self._json(200, {"ok": True, "outage_until": round(until, 2)})

        if path == "/admin/reset":
            with self.chaos.lock:
                self.chaos.latencies.clear()
                self.chaos.requests = 0
                self.chaos.failures = 0
                self.chaos.bytes_out = 0
                self.chaos.static_served = 0
                for key in self.chaos.counts:
                    self.chaos.counts[key] = 0
            log("🎛 статистику скинуто")
            return self._json(200, {"ok": True, "reset": True})

        return self._json(404, {"ok": False, "error": "Not Found", "path": path})

    def _ping(self) -> None:
        started = time.perf_counter()
        if self._apply_chaos("/ping"):
            return
        payload = {
            "pong": True,
            "server": self.server_version,
            "ts": round(time.time(), 3),
            "seq": self.chaos.requests + 1,
            "uptime_s": round(time.time() - STARTED_AT, 2),
            "level": self.chaos.level,
            "client": self.client_address[0],
        }
        size = self._json(200, payload)
        self.chaos.record("ok", (time.perf_counter() - started) * 1000.0, size)
        log("🏓 200 /ping %.1f мс" % ((time.perf_counter() - started) * 1000.0))

    def _health(self) -> None:
        """Чесний стан — завжди відповідає, навіть під час аварії."""
        snap = self.chaos.snapshot()
        state = "degraded" if snap["outage"]["active"] else "up"
        if snap["success_rate"] < 80 and snap["requests_total"] > 10:
            state = "unstable"
        return self._json(200, {"state": state,
                                "uptime_s": snap["uptime_s"],
                                "success_rate": snap["success_rate"],
                                "outage": snap["outage"],
                                "latency_ms": snap["latency_ms"]})

    def _static(self, name: str) -> None:
        name = os.path.basename(name)
        target = os.path.join(STATIC_DIR, name)
        if not os.path.isfile(target):
            return self._json(404, {"ok": False, "error": "Not Found", "path": name})
        ctype = "text/html; charset=utf-8" if name.endswith((".html", ".htm")) else \
                "text/css; charset=utf-8" if name.endswith(".css") else \
                "application/javascript; charset=utf-8" if name.endswith(".js") else \
                "application/octet-stream"
        with open(target, "rb") as fh:
            data = fh.read()
        self.chaos.record_static(len(data))
        self._send(200, data, ctype)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

    def handle_error(self, request, client_address):  # не роняємо процес через клієнта
        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, socket.timeout)):
            return
        log("❗ помилка з'єднання з %s: %s" % (client_address, exc))


def watchdog(stop: threading.Event) -> None:
    """Періодично звітує про стан сервера."""
    while not stop.wait(30):
        snap = CHAOS.snapshot()
        log("📊 requests=%d success_rate=%s%% p95=%s мс level=%s"
            % (snap["requests_total"], snap["success_rate"],
               snap["latency_ms"]["p95"], snap["level"]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Нестабільний HTTP-сервер для тренування клієнтів на витривалість")
    parser.add_argument("--host", default="0.0.0.0", help="адреса для прослуховування (0.0.0.0)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    parser.add_argument("--level", default=os.environ.get("CHAOS_LEVEL", "medium"),
                        choices=list(CHAOS_PRESETS), help="рівень нестабільності")
    parser.add_argument("--seed", type=int, default=None, help="фіксований seed випадковості")
    args = parser.parse_args(argv)

    CHAOS.set_level(args.level)
    if args.seed is not None:
        CHAOS.rng.seed(args.seed)

    try:
        httpd = Server((args.host, args.port), Handler)
    except OSError as exc:
        print("Не вдалося підняти сервер на %s:%s — %s" % (args.host, args.port, exc),
              file=sys.stderr)
        return 1

    stop = threading.Event()
    threading.Thread(target=watchdog, args=(stop,), daemon=True).start()

    log("🚀 нестабільний сервер v%s на http://%s:%d" % (VERSION, args.host, args.port))
    log("🎛 рівень хаосу: %s | результати: %s" % (CHAOS.level, ", ".join(CHAOS.cfg["outcomes"])))
    log("🏓 ping: /ping | стан: /health | метрики: /stats | керування: /admin/chaos")
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        log("🛑 зупинка за Ctrl+C")
    finally:
        stop.set()
        httpd.server_close()
        log("сервер вимкнено")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
