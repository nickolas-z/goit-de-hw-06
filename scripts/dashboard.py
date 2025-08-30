#!/usr/bin/env python
from __future__ import annotations
import argparse, json, threading, signal, time
from collections import deque
from typing import Optional, Iterable, Any, Deque, Dict, Any, Set
from kafka import KafkaConsumer
from configs import kafka_config
from logging_config import setup_logging

# when running dashboard logs written to file only so console
setup_logging(suppress_console=True)
from sensor_producer import run_producer
from sensor_processor import run_processor
from alert_consumer import run_alert_consumer
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text

_stop = threading.Event()

def build_consumer(topics: Iterable[str]) -> Any:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=None,
        consumer_timeout_ms=1000,
    )

def layout_template() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body")
    )
    layout["body"].split_row(
        Layout(name="sensors"),
        Layout(name="temp"),
        Layout(name="humidity"),
    )
    return layout

def make_table(title: str, rows: Deque[Dict[str, Any]], columns: Iterable[str], highlight_map: Optional[Any] = None) -> Table:
    tbl = Table(title=title, box=box.SIMPLE_HEAVY, expand=True, show_edge=True)
    for col in columns:
        if col == 'ts':
            tbl.add_column(col, overflow="fold", no_wrap=True, justify="left", width=19)
        elif col in ('temperature','humidity','value'):
            tbl.add_column(col, overflow="fold", no_wrap=True, justify="right", width=8)
        else:
            tbl.add_column(col, overflow="fold", no_wrap=True, justify="left")
    # render newest first
    for item in reversed(list(rows)):
        row_cells = []
        for c in columns:
            val = item.get(c)
            # format timestamps
            if c == 'ts' and isinstance(val, str):
                try:
                    val = val.replace('T', ' ')[:19]
                except Exception:
                    pass
            txt = Text(str(val))
            if highlight_map:
                for fn, style in highlight_map:
                    try:
                        if fn(c, val, item):
                            txt.stylize(style)
                    except Exception:
                        pass
            row_cells.append(txt)
        tbl.add_row(*row_cells)
    return tbl

def dashboard(prefix: str, max_rows: int) -> None:
    sensors_topic = f"{prefix}_building_sensors"
    temp_topic = f"{prefix}_temperature_alerts"
    hum_topic = f"{prefix}_humidity_alerts"

    raw_rows: Deque[Dict[str, Any]] = deque(maxlen=max_rows)
    temp_rows: Deque[Dict[str, Any]] = deque(maxlen=max_rows)
    hum_rows: Deque[Dict[str, Any]] = deque(maxlen=max_rows)

    counts = {
        "raw": 0,
        "temp_alerts": 0,
        "hum_alerts": 0
    }
    sensors_set: Set[int] = set()

    cons_raw: Any = build_consumer([sensors_topic])
    cons_alerts: Any = build_consumer([temp_topic, hum_topic])

    def consume_raw() -> None:
        while not _stop.is_set():
            try:
                for msg in cons_raw:
                    if _stop.is_set(): break
                    data = msg.value
                    counts["raw"] += 1
                    sid = data.get("sensor_id")
                    if sid is not None:
                        sensors_set.add(sid)
                    raw_rows.append({
                        "sensor_id": data.get("sensor_id"),
                        "ts": data.get("ts"),
                        "temperature": data.get("temperature"),
                        "humidity": data.get("humidity")
                    })
            except Exception:
                time.sleep(0.5)

    def consume_alerts() -> None:
        while not _stop.is_set():
            try:
                for msg in cons_alerts:
                    if _stop.is_set(): break
                    data = msg.value
                    base = {
                        "sensor_id": data.get("sensor_id"),
                        "ts": data.get("ts"),
                        "metric": data.get("metric"),
                        "value": data.get("value"),
                        "threshold": data.get("threshold"),
                        "message": data.get("message"),
                    }
                    if msg.topic.endswith("temperature_alerts"):
                        counts["temp_alerts"] += 1
                        temp_rows.append(base)
                    else:
                        counts["hum_alerts"] += 1
                        hum_rows.append(base)
            except Exception:
                time.sleep(0.5)

    t1 = threading.Thread(target=consume_raw, daemon=True)
    t2 = threading.Thread(target=consume_alerts, daemon=True)
    t1.start()
    t2.start()

    lay = layout_template()

    # start a key-watcher thread that sets _stop when user presses "q"
    def key_watcher() -> None:
        import sys, termios, tty, select
        fd = sys.stdin.fileno()
        try:
            old = termios.tcgetattr(fd)
        except Exception:
            old = None
        try:
            tty.setcbreak(fd)
            while not _stop.is_set():
                if select.select([sys.stdin], [], [], 0.1)[0]:
                    ch = sys.stdin.read(1)
                    if ch and ch.lower() == 'q':
                        _stop.set()
                        break
        finally:
            if old is not None:
                try:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old)
                except Exception:
                    pass

    tk = threading.Thread(target=key_watcher, daemon=True)
    tk.start()

    def render() -> Layout:
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        header_text = Text.assemble(
            (f"prefix={prefix}  ", "bold white"),
            (f"updated={now}  ", "dim"),
            (f"RAW={counts['raw']}  ", "cyan bold"),
            (f"T={counts['temp_alerts']}  ", "red bold"),
            (f"H={counts['hum_alerts']}  ", "magenta bold"),
            (f"SENSORS={len(sensors_set)}", "green bold"),
        )
        lay["header"].update(Panel(header_text, title="Kafka Dashboard", border_style="blue"))

        lay["sensors"].update(
            make_table(
                    "Sensors (latest)",
                    raw_rows,
                    ["sensor_id", "ts", "temperature", "humidity"],
                    highlight_map=[
                        (lambda c,v,row: c=="temperature" and isinstance(v,(int,float)) and float(v)>40, "bold red"),
                        (lambda c,v,row: c=="humidity" and isinstance(v,(int,float)) and (float(v)>80 or float(v)<20), "bold magenta"),
                    ],
                )
        )
        lay["temp"].update(
            make_table(
                "Temperature Alerts",
                temp_rows,
                ["sensor_id","ts","value","threshold","message"],
                highlight_map=[(lambda c,v,row: True, "red")]
            )
        )
        lay["humidity"].update(
            make_table(
                "Humidity Alerts",
                hum_rows,
                ["sensor_id","ts","value","threshold","message"],
                highlight_map=[(lambda c,v,row: True, "magenta")]
            )
        )
        return lay

    with Live(render(), refresh_per_second=4, screen=False):
        while not _stop.is_set():
            time.sleep(0.25)
            lay = render()

    cons_raw.close()
    cons_alerts.close()

def main() -> None:
    """
    Rich dashboard:
        - Left column: last N (default 15) raw sensor readings
        - Center: temperature alerts
        - Right: humidity alerts
        - Header: counters + unique sensors

    Run:
        python scripts/dashboard.py --prefix myname
    """    
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--max-rows", type=int, default=15)
    ap.add_argument("--start-producer", action="store_true", help="Start a local producer thread")
    ap.add_argument("--start-processor", action="store_true", help="Start a local processor thread")
    ap.add_argument("--start-alerts", action="store_true", help="Start a local alert consumer thread")
    args = ap.parse_args()

    def stop_sig(*_: Any) -> None:
        _stop.set()
    signal.signal(signal.SIGINT, stop_sig)
    signal.signal(signal.SIGTERM, stop_sig)

    threads = []
    if args.start_producer:
        t = threading.Thread(target=run_producer, args=(args.prefix,), kwargs={"stop_event": _stop}, daemon=True)
        threads.append(t)
        t.start()
    if args.start_processor:
        t = threading.Thread(target=run_processor, args=(args.prefix,), kwargs={"stop_event": _stop}, daemon=True)
        threads.append(t)
        t.start()
    if args.start_alerts:
        t = threading.Thread(target=run_alert_consumer, args=(args.prefix,), kwargs={"stop_event": _stop}, daemon=True)
        threads.append(t)
        t.start()

    dashboard(args.prefix, args.max_rows)

if __name__ == "__main__":
    main()