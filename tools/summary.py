"""Собирает все csv из results/ в одну сводку results/summary.md и рисует графики results/charts/*.png.

    docker compose run --rm -T toolbox python tools/summary.py
"""
from __future__ import annotations

import glob
import math
from collections import defaultdict
from pathlib import Path

from common import QTY_LIMIT, RESULTS, info, median, read_csv

FORMATS = ["PARQUET", "ORC"]
CODECS = ["NONE", "SNAPPY", "GZIP", "ZSTD"]
QUERIES = ["Q18", "LIKE_SCAN"]
CHARTS = RESULTS / "charts"
CHARTS.mkdir(exist_ok=True)
COLORS = {"PARQUET": "#1b7f79", "ORC": "#d98c1f"}
PAPER, TEXT, MUTED, GRID = "#ffffff", "#1d1d1f", "#5b5b60", "#e6e6e9"


def fmt(x, digits=2):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    return f"{x:,.{digits}f}".replace(",", " ").replace(".", ",")


def md_table(head, rows):
    align = ["---"] + ["---:"] * (len(head) - 1)
    return "\n".join(["| " + " | ".join(head) + " |", "|" + "|".join(align) + "|"] +
                     ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def bench_csv(pattern):
    rows = []
    for p in sorted(glob.glob(str(RESULTS / "bench" / pattern))):
        rows += read_csv("bench/" + Path(p).name)
    return rows


def truthy(v):
    return str(v).lower() in ("true", "1")


# ============================================================================ слои
def layers_section():
    rows = read_csv("layers.csv")
    if not rows:
        return "_results/layers.csv нет, сначала запусти tools/pipeline.py_\n"
    out = ["## Слои медальона\n"]
    for layer in ["bronze", "silver", "gold"]:
        out.append(f"### {layer}\n")
        out.append(md_table(["Таблица", "Строк", "Колонок", "Файлов", "МБ", "Формат", "Кодек в футере",
                             "Row group в макс. файле", "Партиции", "Сортировка"],
                            [[f"`{r['table']}`", fmt(int(r["rows"]), 0), r["columns"], r["files"],
                              fmt(float(r["size_mb"]), 3), r["format"], r["codec_in_file"],
                              r["row_groups_biggest_file"], r["partitioning"], r["sorted_by"]]
                             for r in rows if r["layer"] == layer]) + "\n")
    steps = read_csv("pipeline_steps.csv")
    if steps:
        out.append("### Время построения\n")
        out.append(md_table(["Слой", "Таблица", "Строк", "Время, с", "CPU, с", "Пик памяти, МБ"],
                            [[r["layer"], f"`{r['table']}`", fmt(int(r["rows_written"]), 0),
                              fmt(float(r["server_s"])), fmt(float(r["cpu_s"]), 1), fmt(float(r["peak_mem_mb"]), 0)]
                             for r in steps]) + "\n")
    dq = read_csv("dq_checks.csv")
    if dq:
        out.append("### Проверки качества\n")
        out.append(md_table(["Проверка", "Значение"], [[d["check"], d["value"]] for d in dq]) + "\n")
    return "\n".join(out)


# ============================================================================ бенчмарк
def collect():
    sizes, writes, reads = bench_csv("bench_*_sizes.csv"), bench_csv("bench_*_writes.csv"), bench_csv("bench_*_reads.csv")
    gen_rep = defaultdict(float)
    for g in read_csv("bench/generator.csv"):
        gen_rep[g["repeat"]] += float(g["server_s"])
    gen = median(gen_rep.values()) if gen_rep else float("nan")
    res = {}
    for f in FORMATS:
        for c in CODECS:
            v = f"{f}+{c}"
            sz = [r for r in sizes if r["variant"] == v]
            if not sz:
                continue
            per_rep, cpu_rep = defaultdict(float), defaultdict(float)
            for r in writes:
                if r["variant"] == v:
                    per_rep[r["repeat"]] += float(r["server_s"])
                    cpu_rep[r["repeat"]] += float(r["cpu_s"])
            w = list(per_rep.values())

            def pick(q, key="server_s", warm=False):
                return [float(r[key]) for r in reads
                        if r["variant"] == v and r["query"] == q and truthy(r["warmup"]) == warm]
            li = next((r for r in sz if r["table"] == "lineitem"), {})
            d = {"format": f, "codec": c, "mb": sum(float(r["size_mb"]) for r in sz),
                 "li_mb": float(li.get("size_mb", "nan")), "codec_file": li.get("codec_in_file", ""),
                 "w": median(w), "w_lo": min(w) if w else float("nan"), "w_hi": max(w) if w else float("nan"),
                 "w_cpu": median(cpu_rep.values()), "w_net": median(w) - gen, "reps": len(w)}
            for q in QUERIES:
                xs = pick(q)
                d[q], d[q + "_lo"], d[q + "_hi"] = median(xs), min(xs), max(xs)
                d[q + "_mb"] = median(pick(q, "read_mb"))
                d[q + "_cpu"] = median(pick(q, "cpu_s"))
                first = pick(q, warm=True)
                d[q + "_first"] = first[0] if first else float("nan")
                d["runs"] = len(xs)
            res[v] = d
    for d in res.values():
        base = res.get(f"{d['format']}+NONE")
        d["ratio"] = base["mb"] / d["mb"] if base else float("nan")
    return res, gen


def bench_section(S, gen):
    if not S:
        return "_замеров бенчмарка нет, запусти ./bench.sh_\n"
    some = next(iter(S.values()))
    best = {k: min(S.values(), key=lambda d: d[k]) for k in ["mb", "w", "Q18", "LIKE_SCAN"]}

    def mark(d, k, txt):
        return f"**{txt}**" if d is best[k] else txt

    rows = [[f"{d['format']} + {d['codec']}", d["codec_file"], mark(d, "mb", fmt(d["mb"], 1)), fmt(d["li_mb"], 1),
             fmt(d["ratio"], 2) + "×", mark(d, "w", f"{fmt(d['w'], 1)} ({fmt(d['w_lo'], 1)}–{fmt(d['w_hi'], 1)})"),
             fmt(d["w_net"], 1), mark(d, "Q18", f"{fmt(d['Q18'])} ({fmt(d['Q18_lo'])}–{fmt(d['Q18_hi'])})"),
             fmt(d["Q18_mb"], 1),
             mark(d, "LIKE_SCAN", f"{fmt(d['LIKE_SCAN'])} ({fmt(d['LIKE_SCAN_lo'])}–{fmt(d['LIKE_SCAN_hi'])})"),
             fmt(d["LIKE_SCAN_mb"], 1)] for d in S.values()]
    out = ["## Бенчмарк Bronze (customer + orders + lineitem)\n",
           f"Записей на вариант: {some['reps']}, кругов чтения после прогрева: {some['runs']}. "
           "Везде медиана, в скобках min–max, жирным лучшая медиана в столбце.\n",
           md_table(["Вариант", "Кодек в футере", "Bronze, МБ", "lineitem, МБ", "Сжатие к NONE", "Запись, с",
                     "Запись без генерации, с", "Q18, с", "Q18 прочитано, МБ", "LIKE_SCAN, с",
                     "LIKE_SCAN прочитано, МБ"], rows), "",
           f"Генерация тех же таблиц коннектором tpch без записи: **{fmt(gen, 1)} с** (медиана).\n",
           "### Первый запуск после записи и CPU\n",
           md_table(["Вариант", "Q18 первый, с", "Q18 CPU, с", "LIKE_SCAN первый, с", "LIKE_SCAN CPU, с",
                     "Запись CPU, с"],
                    [[f"{d['format']} + {d['codec']}", fmt(d["Q18_first"]), fmt(d["Q18_cpu"], 1),
                      fmt(d["LIKE_SCAN_first"]), fmt(d["LIKE_SCAN_cpu"], 1), fmt(d["w_cpu"], 0)]
                     for d in S.values()]), ""]
    gz_zs = [[c, f"{fmt(S[f'PARQUET+{c}']['Q18'])} с", fmt(S[f"PARQUET+{c}"]["mb"], 1), ""]
             for c in ["GZIP", "ZSTD"] if f"PARQUET+{c}" in S]
    if gz_zs:
        out += ["### GZIP и ZSTD (Parquet), как в форме отчета\n",
                md_table(["Сжатие на Bronze", "Время запроса", "Размер Bronze, МБ", "Вывод"], gz_zs), ""]
    return "\n".join(out)


def layout_section():
    rows = read_csv("bench/layout.csv")
    if not rows:
        return ""
    tab = []
    for case in dict.fromkeys(r["case"] for r in rows):
        rr = [r for r in rows if r["case"] == case and not truthy(r["warmup"])]
        xs = [float(r["server_s"]) for r in rr]
        tab.append([case, f"`{rr[0]['table']}`", rr[0]["table_files"], fmt(float(rr[0]["table_mb"]), 2),
                    f"{fmt(median(xs))} ({fmt(min(xs))}–{fmt(max(xs))})",
                    fmt(median([r["cpu_s"] for r in rr]), 2), fmt(median([r["read_mb"] for r in rr]), 2),
                    fmt(median([r["rows_processed"] for r in rr]), 0)])
    out = ["## Раскладка Silver и слои медальона (Q18)\n",
           md_table(["Источник", "Таблица", "Файлов", "МБ", "Время, с", "CPU, с", "Прочитано, МБ",
                     "Строк обработано"], tab), ""]
    for name, title in [("fact_orders_rowgroups.csv", "silver.fact_orders (sorted_by)"),
                        ("bench/unsorted_rowgroups.csv", "fact_orders без сортировки")]:
        rg = read_csv(name)
        if not rg:
            continue
        hit = [r for r in rg if r["max_total_quantity"] not in ("", "None")
               and float(r["max_total_quantity"]) > QTY_LIMIT]
        out.append(f"- {title}: файлов {len({r['file'] for r in rg})}, row group {len(rg)} "
                   f"({fmt(sum(int(r['rows']) for r in rg), 0)} строк). У **{len(hit)}** из них "
                   f"max(total_quantity) > {QTY_LIMIT} ({fmt(sum(int(r['rows']) for r in hit), 0)} строк), "
                   f"пропустить их Trino не может.")
    log = read_csv("bench/storage_log.csv")
    if log:
        out += ["", "### Ожидание очистки бакета между записями\n",
                md_table(["Момент", "Бакет до, МБ", "Бакет после, МБ", "Ждали, с"],
                         [[r["moment"], r["bucket_mb_before"], r["bucket_mb_after"], r["waited_s"]] for r in log])]
    return "\n".join(out) + "\n"


# ============================================================================ графики
def draw(S):
    if not S:
        return []
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "text.color": TEXT,
                         "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
                         "axes.edgecolor": GRID})

    def bars(ax, key, title, digits, spread=None):
        width = 0.38
        for i, f in enumerate(FORMATS):
            xs, ys, lo, hi = [], [], [], []
            for j, c in enumerate(CODECS):
                d = S.get(f"{f}+{c}")
                if not d or math.isnan(d[key]):
                    continue
                xs.append(j + (i - 0.5) * (width + 0.02))
                ys.append(d[key])
                if spread:
                    lo.append(d[key] - d[spread[0]])
                    hi.append(d[spread[1]] - d[key])
            drawn = ax.bar(xs, ys, width=width, color=COLORS[f], label=f, zorder=3)
            if spread:
                ax.errorbar(xs, ys, yerr=[lo, hi], fmt="none", ecolor=MUTED, elinewidth=1, capsize=3, zorder=4)
            tops = [y + h for y, h in zip(ys, hi)] if spread else ys
            for b, y, top in zip(drawn, ys, tops):
                ax.annotate(fmt(y, digits), (b.get_x() + b.get_width() / 2, top), xytext=(0, 3),
                            textcoords="offset points", ha="center", va="bottom", fontsize=8, color=MUTED)
        ax.set_xticks(range(len(CODECS)), CODECS)
        ax.set_title(title, loc="left", fontsize=11, pad=8)
        ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="y", length=0)
        ax.margins(y=0.14)

    files = []
    for fname, title, panels in [
        ("storage_write.png", "Bronze sf4: сколько места занимает и как долго пишется",
         [("mb", "Размер трех таблиц, МБ", 0, None), ("w", "Запись, с: медиана и min–max", 1, ("w_lo", "w_hi"))]),
        ("read_speed.png", "Bronze sf4: скорость чтения",
         [("Q18", "Q18, с: медиана и min–max", 2, ("Q18_lo", "Q18_hi")),
          ("LIKE_SCAN", "LIKE по lineitem.comment, с", 2, ("LIKE_SCAN_lo", "LIKE_SCAN_hi"))]),
    ]:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), facecolor=PAPER)
        for ax, (k, t, dg, sp) in zip(axes, panels):
            bars(ax, k, t, dg, sp)
        h, lab = axes[0].get_legend_handles_labels()
        fig.legend(h, lab, loc="upper right", ncol=2, frameon=False)
        fig.suptitle(title, x=0.01, ha="left", fontsize=13)
        fig.tight_layout(rect=(0, 0, 1, 0.93))
        path = CHARTS / fname
        fig.savefig(path, dpi=150, facecolor=PAPER)
        plt.close(fig)
        files.append(path)
        info(f"график {path.relative_to(RESULTS.parent)}")
    return files


def main():
    S, gen = collect()
    parts = ["# Сводка по всем замерам (собрана tools/summary.py)\n"]
    env = RESULTS / "environment.md"
    if env.exists():
        parts += ["## Окружение\n", env.read_text(encoding="utf-8")]
    parts += [layers_section(), bench_section(S, gen), layout_section()]
    if draw(S):
        parts += ["![Размер и запись](charts/storage_write.png)\n", "![Чтение](charts/read_speed.png)\n"]
    for extra in ["validation.md", "iceberg_demo.md"]:
        p = RESULTS / extra
        if p.exists():
            parts.append(p.read_text(encoding="utf-8"))
    (RESULTS / "summary.md").write_text("\n".join(parts), encoding="utf-8")
    info("results/summary.md собран")


if __name__ == "__main__":
    main()
