#!/usr/bin/env python3
"""
make_index_scatter.py

Usage:
  python3 make_index_scatter.py --analogues path/to/analogues.csv \
    --nina path/to/nina34.anom.data --pdo path/to/pdo.timeseries.sstens.data \
    --glb path/to/GLB.Ts+dSST.txt --outdir path/to/output_dir \
    [--original-date YYYY-MM-DD]

This script:
 - reads analogues.csv (CSV with rows like: date,timer,year,month,day,rank,period)
 - parses monthly index files (robust to leading headers; finds lines starting with 4-digit year)
 - for each analogue (past/present/original) extracts the index values at that year/month
 - makes two scatter figures:
     Figure1: nina34 (x) vs PDO (y)
     Figure2: GLB.Ts (x) vs nina34 (y)
 - saves PNGs into outdir:
     fig1_nina34_vs_pdo.png
     fig2_glb_vs_nina34.png
"""

import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime
import math

import matplotlib.pyplot as plt

# ---------------------
# Parsers for index tables
# ---------------------
def parse_year_month_table(path, months_per_line=12):
    """
    Generic year-month table parser.
    Returns dict[(year,month)] = float_value (monthly values).
    It expects lines that start with 4-digit year, followed by months (12 values).
    """
    vals = {}
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # look for lines that start with a year
            m = re.match(r'^(\d{4})\s+(.+)$', line)
            if not m:
                continue
            year = int(m.group(1))
            rest = m.group(2)
            # split by whitespace, or by multiple spaces
            tokens = re.split(r'\s+', rest.strip())
            # take the first 12 tokens that look numeric
            month_vals = []
            for tok in tokens:
                # remove possible asterisks or 'NA' placeholders
                tok_clean = tok.replace('*', '').replace('NA', 'nan')
                try:
                    val = float(tok_clean)
                    month_vals.append(val)
                except:
                    # skip tokens that don't parse
                    continue
                if len(month_vals) >= months_per_line:
                    break
            if len(month_vals) >= months_per_line:
                for m_idx, v in enumerate(month_vals[:months_per_line], start=1):
                    vals[(year, m_idx)] = v
    return vals

def parse_nina34_psl(path):
    return parse_year_month_table(path, months_per_line=12)

def parse_pdo_psl(path):
    return parse_year_month_table(path, months_per_line=12)

def parse_gistemp_table(path):
    return parse_year_month_table(path, months_per_line=12)

# ---------------------
# Read analogues.csv
# ---------------------
def read_analogues(path):
    """
    Expect CSV with at least columns:
    date_str, someval, year, month, day, rank, period
    Returns list of dicts: {'date': 'YYYY-MM-DD', 'year':int, 'month':int, 'day':int, 'period': 'past'/'present'/'original'}
    """
    rows = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
            # skip header heuristically
            if r[0].lower().startswith('date') or r[0].lower().startswith('#'):
                continue
            try:
                date_str = r[0].strip()
                if len(r) >= 7:
                    year = int(r[2])
                    month = int(r[3])
                    day = int(r[4])
                    period = r[6].strip()
                else:
                    # fallback parse date
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    year, month, day = dt.year, dt.month, dt.day
                    period = r[-1].strip() if len(r) > 1 else 'past'
                rows.append({'date': date_str, 'year': year, 'month': month, 'day': day, 'period': period})
            except Exception:
                # skip problematic line
                continue
    return rows

# ---------------------
# Helpers
# ---------------------
def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def add_original_to_analogues(analogues, original_date_str):
    """
    If original_date_str is provided and there is no row with period 'original',
    add a single row derived from original_date_str.
    Returns extended list (a new list).
    """
    if not original_date_str:
        return analogues
    # detect if original already present
    for r in analogues:
        if r.get('period') == 'original':
            return analogues  # already present
    try:
        dt = datetime.fromisoformat(original_date_str)
    except Exception:
        # try common formats
        try:
            dt = datetime.strptime(original_date_str, "%Y-%m-%d")
        except Exception:
            return analogues
    # insert at front so plotting highlights it early (not essential)
    new_row = {'date': dt.strftime("%Y-%m-%d"), 'year': dt.year, 'month': dt.month, 'day': dt.day, 'period': 'original'}
    return [new_row] + analogues

# ---------------------
# Main
# ---------------------
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--analogues", required=True)
    p.add_argument("--nina", required=True)
    p.add_argument("--pdo", required=True)
    p.add_argument("--glb", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--original-date", default=None, help="YYYY-MM-DD snapshot/original date to include if not already in analogues")
    args = p.parse_args()

    analogues = read_analogues(args.analogues)
    if analogues is None:
        analogues = []
    # ensure original is included (from provided original-date)
    analogues = add_original_to_analogues(analogues, args.original_date)

    if len(analogues) == 0:
        print("No analogues found in", args.analogues)
        return

    nina = parse_nina34_psl(args.nina)
    pdo = parse_pdo_psl(args.pdo)
    glb = parse_gistemp_table(args.glb)

    # Prepare arrays for plotting
    groups = defaultdict(list)  # period -> list of dicts with keys year, month, nina, pdo, glb, date

    for a in analogues:
        y = a['year']; m = a['month']; period = a['period']
        key = (y, m)
        nina_val = nina.get(key, float('nan'))
        pdo_val = pdo.get(key, float('nan'))
        glb_val = glb.get(key, float('nan'))
        groups[period].append({'year': y, 'month': m, 'nina': nina_val, 'pdo': pdo_val, 'glb': glb_val, 'date': a['date']})

    ensure_dir(args.outdir)

    # Colour mapping
    color_map = {'past': 'tab:blue', 'present': 'tab:orange', 'original': 'tab:red'}
    marker_map = {'past': 'o', 'present': 's', 'original': '*'}
    size_map = {'past': 40, 'present': 50, 'original': 200}

    # Figure 1: nina34 vs PDO
    fig1, ax1 = plt.subplots(figsize=(8,6))
    for period in ['past', 'present', 'original']:
        items = groups.get(period, [])
        xs, ys, labels = [], [], []
        for it in items:
            x = it['nina']; y = it['pdo']
            if x is None or y is None or math.isnan(x) or math.isnan(y):
                continue
            xs.append(x); ys.append(y); labels.append(it['date'])
        if xs:
            edgecol = 'k' if period == 'original' else None
            zord = 5 if period == 'original' else 3
            ax1.scatter(xs, ys, label=period, c=color_map.get(period,'gray'),
                        marker=marker_map.get(period,'o'), s=size_map.get(period,40),
                        edgecolors=edgecol, alpha=0.95, zorder=zord)
    ax1.set_xlabel("NINO3.4 anomaly (monthly)")
    ax1.set_ylabel("PDO index (monthly)")
    ax1.set_title("Figure 1 — NINO3.4 vs PDO (analogues)")
    ax1.grid(True, linestyle=':', alpha=0.4)
    ax1.legend(title='period')
    fig1_fn = os.path.join(args.outdir, "fig1_nina34_vs_pdo.png")
    fig1.savefig(fig1_fn, dpi=200, bbox_inches='tight')
    print("Saved:", fig1_fn)

    # Figure 2: GLB.Ts vs nina34
    fig2, ax2 = plt.subplots(figsize=(8,6))
    for period in ['past', 'present', 'original']:
        items = groups.get(period, [])
        xs, ys = [], []
        for it in items:
            x = it['glb']; y = it['nina']
            if x is None or y is None or math.isnan(x) or math.isnan(y):
                continue
            xs.append(x); ys.append(y)
        if xs:
            edgecol = 'k' if period == 'original' else None
            zord = 5 if period == 'original' else 3
            ax2.scatter(xs, ys, label=period, c=color_map.get(period,'gray'),
                        marker=marker_map.get(period,'o'), s=size_map.get(period,40),
                        edgecolors=edgecol, alpha=0.95, zorder=zord)
    ax2.set_xlabel("GISTEMP GLB.Ts+dSST (monthly, °C anomaly)")
    ax2.set_ylabel("NINO3.4 anomaly (monthly)")
    ax2.set_title("Figure 2 — GLB.Ts vs NINO3.4 (analogues)")
    ax2.grid(True, linestyle=':', alpha=0.4)
    ax2.legend(title='period')
    fig2_fn = os.path.join(args.outdir, "fig2_glb_vs_nina34.png")
    fig2.savefig(fig2_fn, dpi=200, bbox_inches='tight')
    print("Saved:", fig2_fn)

    # Also write a small CSV with extracted values for inspection
    csv_out = os.path.join(args.outdir, "index_values_extracted.csv")
    with open(csv_out, 'w', encoding='utf-8') as cf:
        cf.write("date,year,month,period,nina34,pdo,glb\n")
        # iterate through periods in stable order
        for period in ['original', 'present', 'past']:
            for it in groups.get(period, []):
                nina_v = "" if math.isnan(it['nina']) else f"{it['nina']}"
                pdo_v = "" if math.isnan(it['pdo']) else f"{it['pdo']}"
                glb_v = "" if math.isnan(it['glb']) else f"{it['glb']}"
                cf.write(f"{it['date']},{it['year']},{it['month']},{period},{nina_v},{pdo_v},{glb_v}\n")
    print("Saved extracted values to:", csv_out)

if __name__ == "__main__":
    main()
