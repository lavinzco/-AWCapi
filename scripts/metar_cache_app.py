#!/usr/bin/env python3
"""本地运行的 METAR 缓存抓取应用（Tkinter GUI）。"""

from __future__ import annotations

import csv
import gzip
import io
import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

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


def normalize_airports(raw: str) -> set[str]:
    airports: set[str] = set()
    for token in raw.replace(",", " ").split():
        code = token.strip().upper()
        if code:
            airports.add(code)
    return airports


def fetch_cache_bytes(url: str, timeout: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "metar-cache-gui/1.0"})
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


def filter_rows(rows: list[dict[str, str]], airports: set[str]) -> list[dict[str, str]]:
    if not airports:
        return rows
    return [row for row in rows if (row.get("station_id") or row.get("station") or "").upper() in airports]


def write_filtered_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_metadata(path: Path, config: JobConfig, total_rows: int, matched_rows: int) -> None:
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    meta = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_url": config.url,
        "interval_seconds": config.interval,
        "airports": sorted(config.airports),
        "rows_total": total_rows,
        "rows_matched": matched_rows,
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def maybe_write_raw_cache(output_path: Path, payload: bytes) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = output_path.parent / f"metars.cache.{ts}.csv.gz"
    raw_path.write_bytes(payload)
    return raw_path


class MetarCacheApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("METAR 缓存抓取器")
        self.stop_event = threading.Event()
        self.worker: threading.Thread | None = None

        self.icao_var = tk.StringVar(value="ZBAA ZSPD KLAX")
        self.interval_var = tk.StringVar(value=str(DEFAULT_INTERVAL))
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        self.url_var = tk.StringVar(value=DEFAULT_URL)
        self.output_var = tk.StringVar(value="metar_filtered_latest.csv")
        self.raw_cache_var = tk.BooleanVar(value=False)

        self._build_ui()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="机场 ICAO（空格或逗号分隔）").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.icao_var).grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="轮询间隔(秒)").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.interval_var).grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="超时(秒)").grid(row=2, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.timeout_var).grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="数据源 URL").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.url_var).grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="输出 CSV 路径").grid(row=4, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.output_var).grid(row=4, column=1, sticky="ew", pady=4)

        ttk.Checkbutton(frame, text="保存原始 gzip 缓存", variable=self.raw_cache_var).grid(
            row=5, column=1, sticky="w", pady=6
        )

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=6, column=1, sticky="w", pady=6)
        self.start_btn = ttk.Button(btn_frame, text="开始", command=self.start_job)
        self.start_btn.grid(row=0, column=0, padx=(0, 8))
        self.stop_btn = ttk.Button(btn_frame, text="停止", command=self.stop_job, state="disabled")
        self.stop_btn.grid(row=0, column=1)

        self.log_text = scrolledtext.ScrolledText(frame, width=100, height=18, state="disabled")
        self.log_text.grid(row=7, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        frame.rowconfigure(7, weight=1)

    def append_log(self, message: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"[{ts}] {message}\n"

        def _append() -> None:
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, line)
            self.log_text.see(tk.END)
            self.log_text.configure(state="disabled")

        self.root.after(0, _append)

    def build_config(self) -> JobConfig:
        airports = normalize_airports(self.icao_var.get())

        try:
            interval = int(self.interval_var.get().strip())
            timeout = int(self.timeout_var.get().strip())
        except ValueError as exc:
            raise ValueError("间隔和超时必须是整数") from exc

        if interval <= 0 or timeout <= 0:
            raise ValueError("间隔和超时必须大于 0")

        output = Path(self.output_var.get().strip() or "metar_filtered_latest.csv")
        url = self.url_var.get().strip() or DEFAULT_URL

        return JobConfig(
            airports=airports,
            url=url,
            interval=interval,
            timeout=timeout,
            output=output,
            include_raw_cache=self.raw_cache_var.get(),
        )

    def start_job(self) -> None:
        if self.worker and self.worker.is_alive():
            return

        try:
            config = self.build_config()
        except ValueError as exc:
            messagebox.showerror("参数错误", str(exc))
            return

        self.stop_event.clear()
        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")

        self.worker = threading.Thread(target=self.run_loop, args=(config,), daemon=True)
        self.worker.start()
        self.append_log(
            f"任务已启动 interval={config.interval}s airports={len(config.airports) or 'ALL'} output={config.output}"
        )

    def stop_job(self) -> None:
        self.stop_event.set()
        self.start_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.append_log("已请求停止任务。")

    def run_loop(self, config: JobConfig) -> None:
        while not self.stop_event.is_set():
            started = time.time()
            try:
                payload = fetch_cache_bytes(config.url, config.timeout)
                fieldnames, all_rows = parse_csv_rows(payload)
                matched_rows = filter_rows(all_rows, config.airports)
                write_filtered_csv(config.output, fieldnames, matched_rows)
                write_metadata(config.output, config, len(all_rows), len(matched_rows))

                raw_file = None
                if config.include_raw_cache:
                    raw_file = maybe_write_raw_cache(config.output, payload)

                suffix = f" raw={raw_file}" if raw_file else ""
                self.append_log(
                    f"更新成功 total={len(all_rows)} matched={len(matched_rows)} file={config.output}{suffix}"
                )
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                self.append_log(f"更新失败: {exc}")

            elapsed = time.time() - started
            wait_seconds = max(config.interval - elapsed, 0)
            if self.stop_event.wait(wait_seconds):
                break

        self.root.after(0, lambda: self.start_btn.configure(state="normal"))
        self.root.after(0, lambda: self.stop_btn.configure(state="disabled"))
        self.append_log("任务已停止。")


def main() -> int:
    root = tk.Tk()
    app = MetarCacheApp(root)
    root.protocol("WM_DELETE_WINDOW", lambda: (app.stop_job(), root.destroy()))
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
