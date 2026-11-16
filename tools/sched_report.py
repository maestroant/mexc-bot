"""Разбор профиля CPU/планировщика (tools/sched_sampler.py) вместе с журналом PAPER A (решение клиента 13.11, п. 6).

Для каждой задержки цикла событий (LOOP_LAG журнала PAPER, шкала m = CLOCK_MONOTONIC) берутся замеры, охватывающие
интервал [m − lag − шаг, m + шаг], и считаются приращения:
  PAPER: время на CPU (run), ожидание в очереди планировщика (wait = scheduler delay), вынужденные переключения;
  прочие процессы листинга: кто был на CPU; система: занятость ядер, steal (гипервизор), iowait, PSI CPU, запись на
  диск, троттлинг cgroup.
Признак (не доказательство): RUNQUEUE — PAPER ждал в очереди ≥ половины задержки; OWN_CPU — PAPER сам был на CPU ≥
половины задержки (тогда смотреть PROC/StallSampler); STEAL — гипервизор забирал CPU; DISK — запись на диск в
интервале; иначе UNDETERMINED. Для доказательства нужен воспроизводимый тест — признак только сужает поиск.
Плюс профиль по секундам T0 − 10 … T0 + 10 с и steal гипервизора по окну.
  python3 tools/sched_report.py SYMBOL [--sched FILE] [--journal FILE]
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"


def latest(pattern: str) -> Path | None:
    c = sorted(LOGS.glob(pattern))
    return c[-1] if c else None


def load_sched(path: Path) -> tuple[dict, list[dict]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        head = json.loads(f.readline())["SELF"]
        return head, [json.loads(x) for x in f]


def load_journal(path: Path) -> tuple[list[dict], list[tuple[float, str, float]]]:
    """LOOP_LAG и все прочие строки журнала с m: (m, вид, мс обработки для PROC) — для окна каждой задержки."""
    lags, marks = [], []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if e.get("m") is None:
                continue
            ch = e.get("ch") if e.get("src") == "event" else e.get("src")
            if ch == "LOOP_LAG":
                lags.append({"m": e["m"], "t": e["t"], **e["data"]})
            else:
                marks.append((e["m"], str(ch), float((e.get("data") or {}).get("ms") or 0) if ch == "PROC" else 0.0))
    return lags, marks


def window_events(marks: list, lo: float, hi: float) -> str:
    """Что PAPER записал в журнал в окне задержки: кадры (PROC и их сумма мс), REST, WS, прочие события."""
    got = [(c, ms) for m, c, ms in marks if lo <= m <= hi]
    if not got:
        return "в журнале за окно ничего"
    cnt: dict[str, int] = {}
    for c, _ in got:
        cnt[c] = cnt.get(c, 0) + 1
    proc = sum(ms for c, ms in got if c == "PROC")
    return ", ".join(f"{k} ×{v}" for k, v in sorted(cnt.items(), key=lambda kv: -kv[1])) +         (f"; обработка кадров {proc:.1f} мс" if proc else "")


def proc(sample: dict, name: str) -> list | None:
    return next((v for k, v in sample["p"].items() if k.startswith(name + ":")), None)


def delta(a: dict, b: dict, tick: int) -> dict:
    """Приращения между двумя замерами."""
    d = {"ms": b["m"] - a["m"]}
    for name in ("paper", "listing_recorder", "probe_listing", "listing_watcher", "self"):
        x, y = proc(a, name), proc(b, name)
        if x and y:
            d[name] = {"run_ms": (y[2] - x[2]) / 1e6, "wait_ms": (y[3] - x[3]) / 1e6, "nvol": y[6] - x[6],
                       "vol": y[5] - x[5], "cpu_ms": (y[0] + y[1] - x[0] - x[1]) * 1000 / tick}
    ca, cb = a["sys"]["cpu"].get("cpu"), b["sys"]["cpu"].get("cpu")
    if ca and cb:
        diff = [y - x for x, y in zip(ca, cb)]
        total = sum(diff) or 1
        d["ncpu"] = len([k for k in b["sys"]["cpu"] if k != "cpu"])
        d["busy_cores"] = round((total - diff[3] - diff[4]) / total * len([k for k in b["sys"]["cpu"] if k != "cpu"]), 2)
        d["steal_ms"] = diff[7] * 1000 / tick
        d["iowait_ms"] = diff[4] * 1000 / tick
    pa, pb = a["sys"].get("psi") or {}, b["sys"].get("psi") or {}
    if "some" in pa and "some" in pb:
        d["psi_some_ms"] = (pb["some"] - pa["some"]) / 1000
    da, db = a["sys"].get("disk"), b["sys"].get("disk")
    if da and db:
        d["disk_kb"] = (db[0] - da[0]) / 2
        d["disk_ms"] = db[1] - da[1]
    ga, gb = a["sys"].get("cg") or {}, b["sys"].get("cg") or {}
    if "nr_throttled" in ga and "nr_throttled" in gb:
        d["throttled"] = gb["nr_throttled"] - ga["nr_throttled"]
    d["procs_running"] = b["sys"].get("run")
    return d


def verdict(lag: float, d: dict) -> str:
    pp = d.get("paper") or {}
    tags = []
    if pp.get("wait_ms", 0) >= lag / 2:
        tags.append("RUNQUEUE")
    if pp.get("run_ms", 0) >= lag / 2:
        tags.append("OWN_CPU")
    if d.get("busy_cores") is not None and d.get("ncpu") and d["busy_cores"] >= 0.9 * d["ncpu"]:
        tags.append("CPU_SATURATED")
    if d.get("steal_ms", 0) > 0:
        tags.append("STEAL")
    if d.get("throttled", 0) > 0:
        tags.append("CGROUP_THROTTLE")
    if d.get("disk_ms", 0) >= lag / 2:
        tags.append("DISK")
    return "+".join(tags) or "UNDETERMINED"


def bracket(samples: list[dict], lo: float, hi: float) -> tuple[dict, dict] | None:
    a = next((s for s in reversed(samples) if s["m"] <= lo), None)
    b = next((s for s in samples if s["m"] >= hi), None)
    return (a, b) if a and b else None


def fmt(d: dict) -> str:
    pp, parts = d.get("paper") or {}, []
    parts.append(f"PAPER на CPU {pp.get('run_ms', 0):.1f} / в очереди {pp.get('wait_ms', 0):.1f} мс, "
                 f"вынужд. перекл. {pp.get('nvol', 0)}")
    for name in ("listing_recorder", "probe_listing", "listing_watcher"):
        if (o := d.get(name)) and o["run_ms"] >= 1:
            label = {"listing_recorder": "запись", "probe_listing": "проба", "listing_watcher": "наблюдатель"}[name]
            parts.append(f"{label} на CPU {o['run_ms']:.1f} мс")
    parts.append(f"ядер занято {d.get('busy_cores', '?')}, steal {d.get('steal_ms', 0):.0f} мс, PSI {d.get('psi_some_ms', 0):.1f} мс, "
                 f"диск {d.get('disk_kb', 0):.0f} КБ / {d.get('disk_ms', 0)} мс, в очереди готовых {d.get('procs_running')}")
    return "; ".join(parts)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("symbol")
    p.add_argument("--sched")
    p.add_argument("--journal")
    a = p.parse_args()
    sym = a.symbol.upper()
    sp = Path(a.sched) if a.sched else latest(f"sched_{sym}_*.jsonl.gz")
    jp = Path(a.journal) if a.journal else latest(f"paperrec_{sym}_*.jsonl.gz")
    if not sp:
        sys.exit(f"нет профиля sched_{sym}_*")
    head, samples = load_sched(sp)
    tick, open_ms = head["tick_hz"], head.get("open_ms")
    print(f"профиль {sp.name}: замеров {len(samples)}, шаг {head['fast_ms']} мс, ядер {head['ncpu']}, своя цена "
          f"{head['own_sample_ms_avg']} мс на замер (всего {head['own_sample_ms_total']} мс)")
    if jp:
        lags, marks = load_journal(jp)
        print(f"\nзадержки цикла событий PAPER A ({jp.name}): {len(lags)}")
        for lag in lags:
            rel = f"T0{(lag['t'] - open_ms) / 1000:+.1f} с" if open_ms else str(lag["t"])
            br = bracket(samples, lag["m"] - lag["lag_ms"] - 1, lag["m"] + 1)
            if not br:
                print(f"  {rel}: {lag['lag_ms']} мс — вне окна профиля")
                continue
            d = delta(*br, tick)
            print(f"  {rel}: {lag['lag_ms']} мс [{verdict(lag['lag_ms'], d)}] интервал замеров {d['ms']:.0f} мс — {fmt(d)}")
            extra = {k: v for k, v in lag.items() if k in ("gc", "gc_ms", "threads") and v}
            print(f"      журнал PAPER в окне: {window_events(marks, lag['m'] - lag['lag_ms'] - 20, lag['m'] + 2)}"
                  + (f"; {extra}" if extra else "") + f"; стек: {(lag.get('where') or '?')[:120]}")
            sk = [s.get("sock") for s in br if s.get("sock")]
            if sk:
                print(f"      сокеты PAPER, байт в очереди приёма ОС (до Python): {sk[0]} → {sk[-1]}")
    total, spikes = 0.0, []
    for a, b in zip(samples, samples[1:]):  # steal гипервизора по всему окну: шаг замеров — 100 мс (1 с до окна)
        ca, cb = a["sys"]["cpu"].get("cpu"), b["sys"]["cpu"].get("cpu")
        if ca and cb:
            st = (cb[7] - ca[7]) * 1000 / tick
            total += st
            if st >= 30:
                rel = f"T0{(b['w'] - open_ms) / 1000:+.1f} с" if open_ms else str(b["w"])
                spikes.append(f"{rel}: {st:.0f} мс за {b['m'] - a['m']:.0f} мс")
    print(f"\nsteal гипервизора за окно профиля: {total:.0f} мс (сумма по ядрам); интервалов ≥ 30 мс: {len(spikes)}"
          + (" — " + "; ".join(spikes[:20]) if spikes else ""))
    if open_ms:
        print("\nпо секундам, T0 − 10 … T0 + 10 с:")
        w0 = next((s for s in samples if s["w"] >= open_ms - 10_000), None)
        i = samples.index(w0) if w0 else len(samples)
        while i < len(samples) - 1 and samples[i]["w"] <= open_ms + 10_000:
            j = next((k for k in range(i + 1, len(samples)) if samples[k]["m"] - samples[i]["m"] >= 1000), None)
            if j is None:
                break
            d = delta(samples[i], samples[j], tick)
            print(f"  T0{(samples[i]['w'] - open_ms) / 1000:+5.1f} с: {fmt(d)}")
            i = j


if __name__ == "__main__":
    main()
