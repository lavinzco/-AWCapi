#!/usr/bin/env python3
"""本地运行的 METAR 实时解码桌面应用（Tkinter GUI）。"""

from __future__ import annotations

import csv
import gzip
import io
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk

DEFAULT_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
DEFAULT_INTERVAL = 60
DEFAULT_TIMEOUT = 30
MAX_ROWS_IN_UI = 200


@dataclass
class JobConfig:
    airports: set[str]
    url: str
    interval: int
    timeout: int


def normalize_airports(raw: str) -> set[str]:
    airports: set[str] = set()
    for token in raw.replace(",", " ").split():
        code = token.strip().upper()
        if code:
            airports.add(code)
    return airports


def fetch_cache_bytes(url: str, timeout: int) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "metar-cache-gui/1.1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def parse_csv_rows(gzip_payload: bytes) -> list[dict[str, str]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gzip_payload), mode="rb") as gz:
        csv_bytes = gz.read()
    text = csv_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV 头为空，无法解析")
    return list(reader)


def filter_rows(rows: list[dict[str, str]], airports: set[str]) -> list[dict[str, str]]:
    if not airports:
        return rows
    return [row for row in rows if (row.get("station_id") or row.get("station") or "").upper() in airports]


def compact_row(row: dict[str, str]) -> tuple[str, str, str, str, str, str, str, str, str, str]:
    return (
        row.get("station_id", ""),
        row.get("observation_time", ""),
        row.get("flight_category", ""),
        row.get("temp_c", ""),
        row.get("dewpoint_c", ""),
        row.get("wind_dir_degrees", ""),
        row.get("wind_speed_kt", ""),
        row.get("visibility_statute_mi", ""),
        row.get("altim_in_hg", ""),
        row.get("raw_text", ""),
    )


class MetarCacheApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("METAR 实时解码桌面版")
        self.stop_event = threading.Event()
        self.worker: threading.Thread | None = None

        self.icao_var = tk.StringVar(value="ZBAA ZSPD KLAX")
        self.interval_var = tk.StringVar(value=str(DEFAULT_INTERVAL))
        self.timeout_var = tk.StringVar(value=str(DEFAULT_TIMEOUT))
        self.url_var = tk.StringVar(value=DEFAULT_URL)

        self._build_ui()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(8, weight=1)

        ttk.Label(frame, text="机场 ICAO（空格或逗号分隔）").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.icao_var).grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="轮询间隔(秒)").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.interval_var).grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="超时(秒)").grid(row=2, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.timeout_var).grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="数据源 URL").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.url_var).grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="说明：当前模式实时解码显示，不输出 CSV 文件。", foreground="#666").grid(
            row=4, column=1, sticky="w", pady=(2, 6)
        )

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=5, column=1, sticky="w", pady=6)
        self.start_btn = ttk.Button(btn_frame, text="开始", command=self.start_job)
        self.start_btn.grid(row=0, column=0, padx=(0, 8))
        self.stop_btn = ttk.Button(btn_frame, text="停止", command=self.stop_job, state="disabled")
        self.stop_btn.grid(row=0, column=1)

        self.summary_var = tk.StringVar(value="未开始")
        ttk.Label(frame, textvariable=self.summary_var).grid(row=6, column=1, sticky="w", pady=(0, 6))

        self.log_text = scrolledtext.ScrolledText(frame, width=120, height=7, state="disabled")
        self.log_text.grid(row=7, column=0, columnspan=2, sticky="nsew", pady=(6, 8))

        columns = (
            "station_id",
            "observation_time",
            "flight_category",
            "temp_c",
            "dewpoint_c",
            "wind_dir_degrees",
            "wind_speed_kt",
            "visibility_statute_mi",
            "altim_in_hg",
            "raw_text",
        )
        self.table = ttk.Treeview(frame, columns=columns, show="headings", height=12)
        headers = ["ICAO", "观测时间", "飞行类别", "温度", "露点", "风向", "风速kt", "能见度mi", "气压", "原始METAR"]
        widths = [70, 165, 90, 60, 60, 60, 60, 80, 70, 420]
        for col, header, width in zip(columns, headers, widths):
            self.table.heading(col, text=header)
            self.table.column(col, width=width, anchor="w")
        self.table.grid(row=8, column=0, columnspan=2, sticky="nsew")

        ysb = ttk.Scrollbar(frame, orient="vertical", command=self.table.yview)
        self.table.configure(yscrollcommand=ysb.set)
        ysb.grid(row=8, column=2, sticky="ns")

    def append_log(self, message: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"[{ts}] {message}\n"

        def _append() -> None:
            self.log_text.configure(state="normal")
            self.log_text.insert(tk.END, line)
            self.log_text.see(tk.END)
            self.log_text.configure(state="disabled")

        self.root.after(0, _append)

    def render_rows(self, rows: list[tuple[str, ...]]) -> None:
        def _render() -> None:
            self.table.delete(*self.table.get_children())
            for row in rows:
                self.table.insert("", tk.END, values=row)

        self.root.after(0, _render)

    def set_summary(self, text: str) -> None:
        self.root.after(0, lambda: self.summary_var.set(text))

    def build_config(self) -> JobConfig:
        airports = normalize_airports(self.icao_var.get())

        try:
            interval = int(self.interval_var.get().strip())
            timeout = int(self.timeout_var.get().strip())
        except ValueError as exc:
            raise ValueError("间隔和超时必须是整数") from exc

        if interval <= 0 or timeout <= 0:
            raise ValueError("间隔和超时必须大于 0")

        url = self.url_var.get().strip() or DEFAULT_URL

        return JobConfig(airports=airports, url=url, interval=interval, timeout=timeout)

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
        self.append_log(f"任务已启动 interval={config.interval}s airports={len(config.airports) or 'ALL'}")

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
                all_rows = parse_csv_rows(payload)
                matched_rows = filter_rows(all_rows, config.airports)
                display_rows = [compact_row(r) for r in matched_rows[:MAX_ROWS_IN_UI]]
                self.render_rows(display_rows)

                updated = datetime.now(timezone.utc).isoformat()
                self.set_summary(
                    f"上次更新: {updated} | 全量: {len(all_rows)} | 匹配: {len(matched_rows)} | 展示: {len(display_rows)}"
                )
                self.append_log(f"更新成功 total={len(all_rows)} matched={len(matched_rows)}")
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
