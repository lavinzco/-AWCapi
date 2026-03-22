#!/usr/bin/env python3
"""每分钟下载一次 AWC 全量 METAR 缓存，并可按机场 ICAO 过滤输出。"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import signal
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

DEFAULT_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
DEFAULT_INTERVAL = 60
DEFAULT_TIMEOUT = 30


@dataclass
class Config:
    airports: set[str]
    url: str
    interval: int
    timeout: int
    output: Path
    include_raw_cache: bool


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(
        description="每分钟获取一次全量 METAR 缓存文件，并可过滤指定机场数据。"
    )
    parser.add_argument(
        "--airport",
        "-a",
        action="append",
        default=[],
        help="机场 ICAO（可重复指定，如 -a ZBAA -a ZSPD）。未指定则保留全部机场。",
    )
    parser.add_argument(
        "--airports-file",
        type=Path,
        help="包含 ICAO 列表的文本文件（每行一个，可包含逗号分隔）。",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="AWC 缓存文件 URL。")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL,
        help="轮询间隔（秒），默认 60。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="HTTP 请求超时时间（秒），默认 30。",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("metar_filtered_latest.csv"),
        help="过滤后的 CSV 输出路径。",
    )
    parser.add_argument(
        "--include-raw-cache",
        action="store_true",
        help="同时保存本次下载的原始 gzip 缓存文件（文件名带 UTC 时间戳）。",
    )

    ns = parser.parse_args(argv)
    airports = set(normalize_airports(ns.airport))

    if ns.airports_file:
        airports.update(normalize_airports(load_airports_file(ns.airports_file)))

    if ns.interval <= 0:
        parser.error("--interval 必须 > 0")
    if ns.timeout <= 0:
        parser.error("--timeout 必须 > 0")

    return Config(
        airports=airports,
        url=ns.url,
        interval=ns.interval,
        timeout=ns.timeout,
        output=ns.output,
        include_raw_cache=ns.include_raw_cache,
    )


def normalize_airports(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        for token in value.replace(",", " ").split():
            code = token.strip().upper()
            if code:
                result.append(code)
    return result


def load_airports_file(path: Path) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"读取机场文件失败: {path} ({exc})") from exc
    return [line for line in content.splitlines() if line.strip() and not line.strip().startswith("#")]


def fetch_cache_bytes(url: str, timeout: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "metar-cache-poller/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_csv_rows(gzip_payload: bytes) -> tuple[list[str], list[dict[str, str]]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_payload), mode="rb") as gz:
        csv_bytes = gz.read()
    text = csv_bytes.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV 头为空，无法解析")

    rows = list(reader)
    return list(reader.fieldnames), rows


def filter_rows(rows: Iterable[dict[str, str]], airports: set[str]) -> list[dict[str, str]]:
    if not airports:
        return list(rows)

    filtered: list[dict[str, str]] = []
    for row in rows:
        station = (row.get("station_id") or row.get("station") or "").upper()
        if station in airports:
            filtered.append(row)
    return filtered


def write_filtered_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_metadata(path: Path, config: Config, matched_rows: int, total_rows: int) -> None:
    meta = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": config.url,
        "interval_seconds": config.interval,
        "airports": sorted(config.airports),
        "rows_total": total_rows,
        "rows_matched": matched_rows,
    }
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def maybe_write_raw_cache(output_path: Path, gzip_payload: bytes) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_name = f"metars.cache.{ts}.csv.gz"
    raw_path = output_path.parent / raw_name
    raw_path.write_bytes(gzip_payload)
    return raw_path


def run_loop(config: Config) -> int:
    stop = False

    def handle_stop(_signum: int, _frame: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print(
        f"[start] interval={config.interval}s output={config.output} airports={len(config.airports) or 'ALL'}"
    )

    while not stop:
        started = time.time()
        try:
            payload = fetch_cache_bytes(config.url, config.timeout)
            fieldnames, all_rows = parse_csv_rows(payload)
            matched = filter_rows(all_rows, config.airports)
            write_filtered_csv(config.output, fieldnames, matched)
            write_metadata(config.output, config, len(matched), len(all_rows))

            raw_cache_path = None
            if config.include_raw_cache:
                raw_cache_path = maybe_write_raw_cache(config.output, payload)

            raw_suffix = f" raw={raw_cache_path}" if raw_cache_path else ""
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] ok total={len(all_rows)} matched={len(matched)} file={config.output}{raw_suffix}"
            )
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            print(f"[{datetime.now(timezone.utc).isoformat()}] error: {exc}", file=sys.stderr)

        elapsed = time.time() - started
        sleep_seconds = max(config.interval - elapsed, 0)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    print("[stop] 收到停止信号，已退出。")
    return 0


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv or sys.argv[1:])
    return run_loop(config)


if __name__ == "__main__":
    raise SystemExit(main())
