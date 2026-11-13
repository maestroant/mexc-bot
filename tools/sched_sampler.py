"""Профиль CPU и планировщика вокруг открытия листинга (решение клиента 13.11, п. 6) — отдельный процесс, без вмешательства
в цикл событий PAPER. Ни одного ордера; один публичный запрос exchangeInfo раз в 30 с (время открытия, перенос).

Что снимается (Linux /proc и cgroup v2), каждые FAST_MS мс в окне T0 − PRE_S … T0 + POST_S и раз в секунду до него:
  по процессам листинга (paper.py, listing_recorder.py, probe_listing.py этой пары, listing_watcher.py) и самому себе:
    utime/stime (/proc/pid/stat), run_ns/wait_ns/slices (/proc/pid/schedstat: время на CPU и ожидание в очереди
    планировщика — «scheduler delay»), voluntary/nonvoluntary context switches (/proc/pid/status);
  по системе: /proc/stat (user, nice, system, idle, iowait, irq, softirq, steal по каждому ядру), procs_running,
    PSI CPU some/full (/proc/pressure/cpu), запись на корневой диск (/proc/diskstats: секторов записано, мс записи,
    в полёте), cgroup службы наблюдателя (nr_throttled, throttled_usec, cpu.pressure).
Каждая строка несёт m = CLOCK_MONOTONIC, мс — та же шкала, что поле m в журнале PAPER (LOOP_LAG, PROC), поэтому
задержки цикла сопоставляются с замерами точно. Данные копятся в памяти и пишутся одним файлом в конце окна
(logs/sched_<SYM>_<время>.jsonl.gz) — запись на диск во время окна не создаётся. Собственная цена — в строке SELF.
  python3 tools/sched_sampler.py SYMBOL [--open-ms MS] [--fast-ms 100]
Анализ: python3 tools/sched_report.py SYMBOL
"""

import argparse
import gzip
import json
import os
import signal
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
PRE_S, POST_S = 150, 60
TICK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
CGROUP = Path("/sys/fs/cgroup/system.slice/mexc-listing-watcher.service")
SCRIPTS = ("paper.py", "listing_recorder.py", "probe_listing.py", "listing_watcher.py")


def mono_ms() -> float:
    return time.monotonic() * 1000


def read(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return f.read().decode("ascii", "replace")
    except OSError:
        return ""


def find_pids(symbol: str) -> dict[int, str]:
    out = {os.getpid(): "self"}
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        cmd = read(f"/proc/{d}/cmdline").split("\0")
        for s in SCRIPTS:
            if any(c.endswith(s) for c in cmd) and (s == "listing_watcher.py" or symbol in cmd):
                out[int(d)] = s.replace(".py", "")
    return out


def proc_sample(pid: int) -> list | None:
    st = read(f"/proc/{pid}/stat")
    if not st:
        return None
    f = st[st.rfind(")") + 2:].split()
    ss = read(f"/proc/{pid}/schedstat").split()
    vol = nvol = 0
    for line in read(f"/proc/{pid}/status").splitlines():
        if line.startswith("voluntary_ctxt_switches"):
            vol = int(line.split()[1])
        elif line.startswith("nonvoluntary_ctxt_switches"):
            nvol = int(line.split()[1])
    # utime, stime (такты), run_ns, wait_ns, slices, voluntary, nonvoluntary, processor
    return [int(f[11]), int(f[12]), int(ss[0]) if ss else 0, int(ss[1]) if ss else 0, int(ss[2]) if ss else 0,
            vol, nvol, int(f[36])]


def socket_inodes(pid: int) -> set[str]:
    out = set()
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                link = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if link.startswith("socket:["):
                out.add(link[8:-1])
    except OSError:
        pass
    return out


def rx_queues(inodes: set[str]) -> list[int]:
    """Байт в очереди приёма ОС у TCP-сокетов процесса (/proc/net/tcp*, rx_queue) — то, что ещё не прочитал Python."""
    out = []
    for fn in ("/proc/net/tcp", "/proc/net/tcp6"):
        for line in read(fn).splitlines()[1:]:
            p = line.split()
            if len(p) > 9 and p[9] in inodes:
                out.append(int(p[4].split(":")[1], 16))
    return out


def sys_sample(disk: str | None) -> dict:
    cpus, running = {}, None
    for line in read("/proc/stat").splitlines():
        if line.startswith("cpu"):
            p = line.split()
            cpus[p[0]] = [int(x) for x in p[1:9]]  # user nice system idle iowait irq softirq steal
        elif line.startswith("procs_running"):
            running = int(line.split()[1])
    psi = {}
    for line in read("/proc/pressure/cpu").splitlines():
        p = line.split()
        psi[p[0]] = int(p[-1].split("=")[1])
    d = None
    if disk:
        for line in read("/proc/diskstats").splitlines():
            p = line.split()
            if len(p) > 10 and p[2] == disk:
                d = [int(p[9]), int(p[10]), int(p[11])]  # секторов записано, мс записи, в полёте
    cg = {}
    for line in read(str(CGROUP / "cpu.stat")).splitlines():
        k, v = line.split()
        if k in ("nr_throttled", "throttled_usec", "usage_usec"):
            cg[k] = int(v)
    for line in read(str(CGROUP / "cpu.pressure")).splitlines():
        p = line.split()
        cg["psi_" + p[0]] = int(p[-1].split("=")[1])
    return {"cpu": cpus, "run": running, "psi": psi, "disk": d, "cg": cg}


def root_disk() -> str | None:
    for line in read("/proc/mounts").splitlines():
        dev, mnt = line.split()[:2]
        if mnt == "/" and dev.startswith("/dev/"):
            name = os.path.basename(os.path.realpath(dev))
            for cand in (name, name.rstrip("0123456789").rstrip("p")):
                if f" {cand} " in read("/proc/diskstats"):
                    return name
    return None


def open_time(symbol: str) -> int | None:
    try:
        url = f"https://api.mexc.com/api/v3/exchangeInfo?symbol={symbol}"
        with urllib.request.urlopen(url, timeout=5) as r:
            s = (json.loads(r.read()).get("symbols") or [{}])[0]
            return s.get("firstOpenTime") or None
    except Exception:  # noqa: BLE001 — время открытия не обязательно: тогда ждём аргумент/предел
        return None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("symbol")
    p.add_argument("--open-ms", type=int, default=None)
    p.add_argument("--fast-ms", type=float, default=100.0)
    p.add_argument("--max-wait", type=float, default=3.0, help="часов ждать открытия (3)")
    a = p.parse_args()
    sym = a.symbol.upper()
    os.nice(10)  # уступать CPU процессам листинга; счётчики ядра накопительные — точность не теряется
    open_ms = a.open_ms or open_time(sym)
    disk = root_disk()
    samples, header = [], {"symbol": sym, "fast_ms": a.fast_ms, "pre_s": PRE_S, "post_s": POST_S, "tick_hz": TICK,
                           "disk": disk, "ncpu": os.cpu_count(), "started_wall": int(time.time() * 1000),
                           "started_m": mono_ms(), "fields_proc": ["utime", "stime", "run_ns", "wait_ns", "slices",
                                                                   "vol_cs", "nvol_cs", "cpu"]}
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    pids, last_pids, last_info, started = {}, 0.0, time.monotonic(), time.monotonic()
    paper_socks: set[str] = set()
    own_ms = 0.0
    while not stop["flag"]:
        now_wall = time.time() * 1000
        if open_ms and now_wall > open_ms + POST_S * 1000:
            break
        if time.monotonic() - started > a.max_wait * 3600:
            break
        if not a.open_ms and time.monotonic() - last_info > 30 and (not open_ms or now_wall < open_ms - 300_000):
            open_ms, last_info = open_time(sym) or open_ms, time.monotonic()  # перенос старта; за 5 мин — заморожено
        fast = open_ms is not None and now_wall >= open_ms - PRE_S * 1000
        armed = open_ms is not None and now_wall >= open_ms - (PRE_S + 60) * 1000  # раньше — только ждём (память)
        if not armed:
            time.sleep(1.0)
            continue
        if fast or not samples or now_wall - samples[-1]["w"] >= 1000:
            c0 = time.perf_counter()
            if time.monotonic() - last_pids > 5:
                pids, last_pids = find_pids(sym), time.monotonic()
                paper_socks = set().union(*(socket_inodes(pid) for pid, n in pids.items() if n == "paper"))
            row = {"m": round(mono_ms(), 3), "w": int(now_wall), "sys": sys_sample(disk),
                   "p": {f"{name}:{pid}": v for pid, name in pids.items() if (v := proc_sample(pid))}}
            if paper_socks:
                row["sock"] = rx_queues(paper_socks)
            samples.append(row)
            own_ms += (time.perf_counter() - c0) * 1000
        time.sleep((a.fast_ms if fast else 250) / 1000)
    header.update(open_ms=open_ms, samples=len(samples), own_sample_ms_total=round(own_ms, 1),
                  own_sample_ms_avg=round(own_ms / max(1, len(samples)), 3), ended_m=mono_ms())
    out = LOGS / f"sched_{sym}_{time.strftime('%Y%m%d_%H%M%S')}.jsonl.gz"
    with gzip.open(out, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"SELF": header}) + "\n")
        for s in samples:
            f.write(json.dumps(s, separators=(",", ":")) + "\n")
    print(f"{sym}: замеров {len(samples)}, своя цена {header['own_sample_ms_avg']} мс на замер → {out}", flush=True)


if __name__ == "__main__":
    if sys.platform != "linux":
        sys.exit("только Linux (/proc)")
    main()
