#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Пінг-тест для нестабільного сервера.

Б'є по /ping кілька разів (можна паралельно), рахує успішні/провальні запити,
розбивку помилок і перцентилі затримки.

Приклад:
    python3 ping_test.py --url http://127.0.0.1:8000 --count 40 --concurrency 5 --timeout 5
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def one_ping(url: str, timeout: float) -> dict:
    """Один пінг: повертає {'ok': bool, 'status': int|None, 'ms': float, 'reason': str}."""
    started = time.perf_counter()
    req = urllib.request.Request(url, headers={"User-Agent": "ping-test/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ms = (time.perf_counter() - started) * 1000.0
            try:
                json.loads(body.decode("utf-8"))
                valid = True
            except (ValueError, UnicodeDecodeError):
                valid = False  # обірване тіло
            return {"ok": resp.status == 200 and valid, "status": resp.status,
                    "ms": ms, "reason": "ok" if valid else "corrupted-body"}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": exc.code,
                "ms": (time.perf_counter() - started) * 1000.0,
                "reason": "http-%d" % exc.code}
    except TimeoutError:
        return {"ok": False, "status": None,
                "ms": (time.perf_counter() - started) * 1000.0, "reason": "timeout"}
    except Exception as exc:  # розірване з'єднання, DNS, refused тощо
        return {"ok": False, "status": None,
                "ms": (time.perf_counter() - started) * 1000.0,
                "reason": type(exc).__name__}


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    idx = min(len(data) - 1, int(round(p / 100.0 * (len(data) - 1))))
    return data[idx]


def run(url: str, count: int, concurrency: int, timeout: float,
        interval: float, verbose: bool) -> int:
    results: list[dict] = []
    lock = threading.Lock()
    sem = threading.Semaphore(max(1, concurrency))

    print("→ пінгую %s/ping  (count=%d, concurrency=%d, timeout=%.1fs)\n"
          % (url, count, concurrency, timeout))

    def worker(index: int) -> None:
        with sem:
            res = one_ping(url + "/ping", timeout)
        with lock:
            results.append(res)
            if verbose:
                mark = (GREEN + "✓" if res["ok"] else RED + "✗") + RESET
                status = res["status"] if res["status"] else res["reason"]
                print("  #%03d %s %-12s %8.1f мс" % (index, mark, status, res["ms"]))

    threads = []
    started = time.perf_counter()
    for i in range(1, count + 1):
        th = threading.Thread(target=worker, args=(i,))
        th.start()
        threads.append(th)
        if interval > 0:
            time.sleep(interval)
    for th in threads:
        th.join()
    wall = time.perf_counter() - started

    ok = [r for r in results if r["ok"]]
    bad = [r for r in results if not r["ok"]]
    reasons = Counter(r["reason"] for r in bad)
    lat_all = [r["ms"] for r in results]
    lat_ok = [r["ms"] for r in ok]
    rate = 100.0 * len(ok) / len(results) if results else 0.0

    print("\n─────────────── РЕЗУЛЬТАТ ───────────────")
    print("  усього запитів : %d" % len(results))
    print("  успішних       : %d" % len(ok))
    print("  провальних     : %d" % len(bad))
    color = GREEN if rate >= 99 else YELLOW if rate >= 60 else RED
    print("  success rate   : %s%.1f%%%s" % (color, rate, RESET))
    print("  час прогону    : %.2f с (%.1f req/s)"
          % (wall, len(results) / wall if wall else 0))
    if reasons:
        print("  помилки        : " + ", ".join("%s ×%d" % kv for kv in reasons.most_common()))
    print("  затримка (усі) : min %.1f | avg %.1f | p50 %.1f | p95 %.1f | max %.1f мс"
          % (min(lat_all) if lat_all else 0, sum(lat_all) / len(lat_all) if lat_all else 0,
             percentile(lat_all, 50), percentile(lat_all, 95),
             max(lat_all) if lat_all else 0))
    if lat_ok:
        print("  затримка (успіх): avg %.1f | p95 %.1f мс"
              % (sum(lat_ok) / len(lat_ok), percentile(lat_ok, 95)))

    # Стан з боку самого сервера
    try:
        with urllib.request.urlopen(url + "/stats", timeout=timeout) as resp:
            stats = json.loads(resp.read().decode("utf-8"))
        print("  ── сервер каже ──")
        print("  level          : %s" % stats["level"])
        print("  requests_total : %d" % stats["requests_total"])
        print("  outcomes       : %s"
              % ", ".join("%s=%d" % kv for kv in stats["outcomes"].items() if kv[1]))
        print("  server p95     : %s мс" % stats["latency_ms"]["p95"])
    except Exception as exc:  # навіть /stats може не відповісти — сервер же нестабільний
        print("  ⚠ /stats недоступний: %s" % exc)

    verdict = ("СТАБІЛЬНО" if rate >= 99 else
               "НЕСТАБІЛЬНО" if rate >= 60 else "ПОВНИЙ ХАОС")
    print("  висновок       : %s%s%s" % (color, verdict, RESET))
    print("─────────────────────────────────────────")
    return 0 if rate >= 99 else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Пінг-тест нестабільного сервера")
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--interval", type=float, default=0.0,
                        help="пауза між стартом пінгів, с")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    return run(args.url.rstrip("/"), args.count, args.concurrency,
               args.timeout, args.interval, verbose=not args.quiet)


if __name__ == "__main__":
    raise SystemExit(main())
