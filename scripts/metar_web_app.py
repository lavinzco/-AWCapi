#!/usr/bin/env python3
"""本地网页版 METAR 实时解码查看器。"""

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

DEFAULT_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
DEFAULT_INTERVAL = 60
DEFAULT_TIMEOUT = 30
MAX_ROWS_IN_MEMORY = 500


@dataclass
class JobConfig:
    airports: set[str]
    url: str
    interval: int
    timeout: int


class AppState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.running = False
        self.stop_event = threading.Event()
        self.worker: threading.Thread | None = None
        self.config = JobConfig(set(), DEFAULT_URL, DEFAULT_INTERVAL, DEFAULT_TIMEOUT)
        self.logs: list[str] = []
        self.latest_rows: list[dict[str, str]] = []
        self.last_updated_utc = "-"
        self.last_total = 0
        self.last_matched = 0

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
    req = urllib.request.Request(url, headers={"User-Agent": "metar-web-app/1.1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_csv_rows(gzip_payload: bytes) -> list[dict[str, str]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_payload), mode="rb") as gz:
        csv_bytes = gz.read()
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8", errors="replace")))
    if not reader.fieldnames:
        raise ValueError("CSV 头为空")
    return list(reader)


def filter_rows(rows: list[dict[str, str]], airports: set[str]) -> list[dict[str, str]]:
    if not airports:
        return rows
    return [r for r in rows if (r.get("station_id") or r.get("station") or "").upper() in airports]


def compact_row(row: dict[str, str]) -> dict[str, str]:
    return {
        "station_id": row.get("station_id", ""),
        "observation_time": row.get("observation_time", ""),
        "flight_category": row.get("flight_category", ""),
        "temp_c": row.get("temp_c", ""),
        "dewpoint_c": row.get("dewpoint_c", ""),
        "wind_dir_degrees": row.get("wind_dir_degrees", ""),
        "wind_speed_kt": row.get("wind_speed_kt", ""),
        "visibility_statute_mi": row.get("visibility_statute_mi", ""),
        "altim_in_hg": row.get("altim_in_hg", ""),
        "raw_text": row.get("raw_text", ""),
    }


def polling_loop() -> None:
    STATE.append_log("后台任务启动（实时解码模式，不写 CSV）")
    while not STATE.stop_event.is_set():
        with STATE.lock:
            cfg = STATE.config

        started = time.time()
        try:
            payload = fetch_cache_bytes(cfg.url, cfg.timeout)
            rows = parse_csv_rows(payload)
            matched = filter_rows(rows, cfg.airports)
            decoded = [compact_row(row) for row in matched[:MAX_ROWS_IN_MEMORY]]

            with STATE.lock:
                STATE.latest_rows = decoded
                STATE.last_updated_utc = datetime.now(timezone.utc).isoformat()
                STATE.last_total = len(rows)
                STATE.last_matched = len(matched)

            STATE.append_log(
                f"更新成功 total={len(rows)} matched={len(matched)} displayed={len(decoded)}"
            )
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
<html lang="zh-CN"><head><meta charset="utf-8"><title>METAR 实时解码</title>
<style>
body{font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;padding:0 12px;}
label{display:block;margin:8px 0 4px;}input[type=text],input[type=number]{width:100%;padding:8px;}
button{padding:8px 14px;margin-right:8px;}#logs{background:#111;color:#ddd;height:180px;overflow:auto;padding:10px;white-space:pre-wrap;}
.small{color:#666;font-size:13px;margin-top:4px;}
table{width:100%;border-collapse:collapse;margin-top:12px;font-size:13px;}th,td{border:1px solid #ccc;padding:6px;vertical-align:top;}th{background:#f2f2f2;}
.raw{font-family:monospace;white-space:pre-wrap;}
</style>
</head><body>
<h2>METAR 实时解码查看器（本地）</h2>
<form id="cfgForm">
<label>机场 ICAO（空格或逗号分隔；留空=全部机场）</label><input type="text" name="airports" value="ZBAA ZSPD KLAX">
<label>轮询间隔（秒）</label><input type="number" name="interval" value="60" min="1">
<label>超时（秒）</label><input type="number" name="timeout" value="30" min="1">
<label>数据源 URL</label><input type="text" name="url" value="https://aviationweather.gov/data/cache/metars.cache.csv.gz">
<div class="small">说明：当前模式会实时解码并展示，不再输出 CSV 文件。</div><br>
<button type="button" onclick="startJob()">开始</button><button type="button" onclick="stopJob()">停止</button>
</form>
<h3 id="status">状态: -</h3>
<div id="summary" class="small"></div>
<div id="logs"></div>
<table id="metarTable"><thead><tr>
<th>ICAO</th><th>观测时间</th><th>飞行类别</th><th>温度</th><th>露点</th><th>风向</th><th>风速kt</th><th>能见度mi</th><th>气压inHg</th><th>原始 METAR</th>
</tr></thead><tbody></tbody></table>
<script>
function esc(v){return (v??'').toString().replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');}
async function startJob(){const form=new FormData(document.getElementById('cfgForm'));await fetch('/start',{method:'POST',body:new URLSearchParams(form)});refresh();}
async function stopJob(){await fetch('/stop',{method:'POST'});refresh();}
function renderRows(rows){const tb=document.querySelector('#metarTable tbody');tb.innerHTML='';for(const r of rows){const tr=document.createElement('tr');tr.innerHTML=`<td>${esc(r.station_id)}</td><td>${esc(r.observation_time)}</td><td>${esc(r.flight_category)}</td><td>${esc(r.temp_c)}</td><td>${esc(r.dewpoint_c)}</td><td>${esc(r.wind_dir_degrees)}</td><td>${esc(r.wind_speed_kt)}</td><td>${esc(r.visibility_statute_mi)}</td><td>${esc(r.altim_in_hg)}</td><td class="raw">${esc(r.raw_text)}</td>`;tb.appendChild(tr);}}
async function refresh(){const r=await fetch('/status');const j=await r.json();document.getElementById('status').innerText='状态: '+(j.running?'运行中':'已停止');document.getElementById('summary').innerText=`上次更新: ${j.last_updated_utc} | 全量: ${j.last_total} | 匹配: ${j.last_matched} | 页面展示: ${j.rows.length}`;document.getElementById('logs').textContent=j.logs.join('\n');renderRows(j.rows);}
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
                payload_obj = {
                    "running": STATE.running,
                    "logs": STATE.logs[-120:],
                    "rows": STATE.latest_rows,
                    "last_updated_utc": STATE.last_updated_utc,
                    "last_total": STATE.last_total,
                    "last_matched": STATE.last_matched,
                }
            payload = json.dumps(payload_obj, ensure_ascii=False).encode("utf-8")
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
            )
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "参数错误")
            return

        log_message = ""
        with STATE.lock:
            STATE.config = cfg
            if not STATE.running:
                STATE.stop_event.clear()
                STATE.running = True
                STATE.worker = threading.Thread(target=polling_loop, daemon=True)
                STATE.worker.start()
                log_message = f"收到启动请求 interval={cfg.interval}s airports={len(cfg.airports) or 'ALL'}"
            else:
                log_message = "任务运行中，配置已更新并将在下一轮生效"

        if log_message:
            STATE.append_log(log_message)

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
    print(f"METAR 实时解码网页已启动: http://{host}:{port}")
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
