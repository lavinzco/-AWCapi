#!/usr/bin/env python3
"""本地网页版 METAR 缓存抓取器。"""

from __future__ import annotations

import csv
import gzip
import io
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DEFAULT_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
DEFAULT_INTERVAL = 60
DEFAULT_TIMEOUT = 30


@dataclass
class JobConfig:
    airports: set[str]
    url: str
    interval: int
    timeout: int
    output: Path
    include_raw_cache: bool


class AppState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.running = False
        self.stop_event = threading.Event()
        self.worker: threading.Thread | None = None
        self.config = JobConfig(set(), DEFAULT_URL, DEFAULT_INTERVAL, DEFAULT_TIMEOUT, Path("metar_filtered_latest.csv"), False)
        self.logs: list[str] = []

    def append_log(self, message: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"[{ts}] {message}"
        with self.lock:
            self.logs.append(line)
            if len(self.logs) > 300:
                self.logs = self.logs[-300:]


STATE = AppState()


def normalize_airports(raw: str) -> set[str]:
    out: set[str] = set()
    for token in raw.replace(",", " ").split():
        code = token.strip().upper()
        if code:
            out.add(code)
    return out


def fetch_cache_bytes(url: str, timeout: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "metar-web-app/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_csv_rows(gzip_payload: bytes) -> tuple[list[str], list[dict[str, str]]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_payload), mode="rb") as gz:
        csv_bytes = gz.read()
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8", errors="replace")))
    if not reader.fieldnames:
        raise ValueError("CSV 头为空")
    rows = list(reader)
    return list(reader.fieldnames), rows


def filter_rows(rows: list[dict[str, str]], airports: set[str]) -> list[dict[str, str]]:
    if not airports:
        return rows
    return [r for r in rows if (r.get("station_id") or r.get("station") or "").upper() in airports]


def write_filtered_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_metadata(path: Path, cfg: JobConfig, total_rows: int, matched_rows: int) -> None:
    meta = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": cfg.url,
        "interval_seconds": cfg.interval,
        "airports": sorted(cfg.airports),
        "rows_total": total_rows,
        "rows_matched": matched_rows,
    }
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def maybe_write_raw_cache(path: Path, payload: bytes) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw = path.parent / f"metars.cache.{ts}.csv.gz"
    raw.write_bytes(payload)
    return raw


def polling_loop() -> None:
    STATE.append_log("后台任务启动")
    while not STATE.stop_event.is_set():
        with STATE.lock:
            cfg = STATE.config
        started = time.time()
        try:
            payload = fetch_cache_bytes(cfg.url, cfg.timeout)
            fieldnames, rows = parse_csv_rows(payload)
            matched = filter_rows(rows, cfg.airports)
            write_filtered_csv(cfg.output, fieldnames, matched)
            write_metadata(cfg.output, cfg, len(rows), len(matched))
            extra = ""
            if cfg.include_raw_cache:
                raw = maybe_write_raw_cache(cfg.output, payload)
                extra = f" raw={raw}"
            STATE.append_log(f"更新成功 total={len(rows)} matched={len(matched)} output={cfg.output}{extra}")
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            STATE.append_log(f"更新失败: {exc}")

        elapsed = time.time() - started
        wait_seconds = max(cfg.interval - elapsed, 0)
        if STATE.stop_event.wait(wait_seconds):
            break

    with STATE.lock:
        STATE.running = False
        STATE.worker = None
    STATE.append_log("后台任务已停止")


HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>METAR 网页抓取器</title>
<style>body{font-family:Arial,sans-serif;max-width:900px;margin:20px auto;padding:0 12px;}label{display:block;margin:8px 0 4px;}input[type=text],input[type=number]{width:100%;padding:8px;}button{padding:8px 14px;margin-right:8px;}#logs{background:#111;color:#ddd;height:300px;overflow:auto;padding:10px;white-space:pre-wrap;}</style>
</head><body>
<h2>METAR 缓存网页版（本地运行）</h2>
<form id="cfgForm">
<label>机场 ICAO（空格或逗号分隔）</label><input type="text" name="airports" value="ZBAA ZSPD KLAX">
<label>轮询间隔（秒）</label><input type="number" name="interval" value="60" min="1">
<label>超时（秒）</label><input type="number" name="timeout" value="30" min="1">
<label>数据源 URL</label><input type="text" name="url" value="https://aviationweather.gov/data/cache/metars.cache.csv.gz">
<label>输出 CSV 路径</label><input type="text" name="output" value="metar_filtered_latest.csv">
<label><input type="checkbox" name="include_raw_cache"> 保存原始 gzip 快照</label><br>
<button type="button" onclick="startJob()">开始</button><button type="button" onclick="stopJob()">停止</button>
</form>
<h3 id="status">状态: -</h3><div id="logs"></div>
<script>
async function startJob(){const form=new FormData(document.getElementById('cfgForm'));await fetch('/start',{method:'POST',body:new URLSearchParams(form)});refresh();}
async function stopJob(){await fetch('/stop',{method:'POST'});refresh();}
async function refresh(){const r=await fetch('/status');const j=await r.json();document.getElementById('status').innerText='状态: '+(j.running?'运行中':'已停止');document.getElementById('logs').textContent=j.logs.join('\n');}
setInterval(refresh,3000);refresh();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            body = HTML.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/status":
            with STATE.lock:
                payload = json.dumps({"running": STATE.running, "logs": STATE.logs[-120:]}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def do_POST(self) -> None:
        if self.path == "/start":
            self.handle_start()
            return
        if self.path == "/stop":
            self.handle_stop()
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def handle_start(self) -> None:
        content_len = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_len).decode("utf-8", errors="ignore")
        form = urllib.parse.parse_qs(body)

        try:
            cfg = JobConfig(
                airports=normalize_airports(form.get("airports", [""])[0]),
                url=(form.get("url", [DEFAULT_URL])[0] or DEFAULT_URL).strip(),
                interval=max(int(form.get("interval", [str(DEFAULT_INTERVAL)])[0]), 1),
                timeout=max(int(form.get("timeout", [str(DEFAULT_TIMEOUT)])[0]), 1),
                output=Path((form.get("output", ["metar_filtered_latest.csv"])[0] or "metar_filtered_latest.csv").strip()),
                include_raw_cache=(form.get("include_raw_cache", [""])[0] == "on"),
            )
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "参数错误")
            return

        with STATE.lock:
            STATE.config = cfg
            if not STATE.running:
                STATE.stop_event.clear()
                STATE.running = True
                STATE.worker = threading.Thread(target=polling_loop, daemon=True)
                STATE.worker.start()
                STATE.append_log(
                    f"收到启动请求 interval={cfg.interval}s airports={len(cfg.airports) or 'ALL'} output={cfg.output}"
                )
            else:
                STATE.append_log("任务运行中，配置已更新并将在下一轮生效")

        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def handle_stop(self) -> None:
        STATE.stop_event.set()
        STATE.append_log("收到停止请求")
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    host, port = "127.0.0.1", 8080
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"METAR 网页版已启动: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        STATE.stop_event.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
