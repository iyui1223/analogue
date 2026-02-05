#!/usr/bin/env python3
"""
make_index_scatter.py

Usage:
  python3 make_index_scatter.py --analogues path/to/analogues.csv \
    --nina path/to/nina34.anom.data --pdo path/to/pdo.timeseries.sstens.data \
    --glb path/to/GLB.Ts+dSST.txt --outdir path/to/output_dir

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
                # remove possible asterisks or non-numeric chars
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
    # same format as generic year-month table
    return parse_year_month_table(path, months_per_line=12)

def parse_pdo_psl(path):
    # The PDO file may have additional leading columns; generic parser should work
    return parse_year_month_table(path, months_per_line=12)

def parse_gistemp_table(path):
    # GISTEMP GLB.Ts+dSST.txt uses Year followed by monthly anomalies (12 cols),
    # possibly additional summary columns at end. Generic parser should work.
    return parse_year_month_table(path, months_per_line=12)

# ---------------------
# Read analogues.csv
# ---------------------
def read_analogues(path):
    """
    Expect CSV with at least columns:
    date_str, someval, year, month, day, rank, period
    We'll try to parse columns by header presence, else assume the example format.
    Returns list of dicts: {'date': 'YYYY-MM-DD', 'year':int, 'month':int, 'day':int, 'period': 'past'/'present'/'original'}
    """
    rows = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        for r in reader:
            if not r or r[0].lower().startswith('date'):
                continue
            # tolerate different column counts
            try:
                date_str = r[0].strip()
                # try to use the explicit year/month columns if present
                if len(r) >= 7:
                    year = int(r[2])
                    month = int(r[3])
                    day = int(r[4])
                    period = r[6].strip()
                else:
                    # fallback: parse date
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    year, month, day = dt.year, dt.month, dt.day
                    period = r[-1].strip() if len(r) > 1 else 'past'
                rows.append({'date': date_str, 'year': year, 'month': month, 'day': day, 'period': period})
            except Exception as e:
                # skip problematic line
                continue
    return rows

# ---------------------
# Plotting helpers
# ---------------------
def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--analogues", required=True)
    p.add_argument("--nina", required=True)
    p.add_argument("--pdo", required=True)
    p.add_argument("--glb", required=True)
    p.add_argument("--outdir", required=True)
    args = p.parse_args()

    analogues = read_analogues(args.analogues)
    if len(analogues) == 0:
        print("No analogues found in", args.analogues)
        return

    nina = parse_nina34_psl(args.nina)
    pdo = parse_pdo_psl(args.pdo)
    glb = parse_gistemp_table(args.glb)

    # Prepare arrays for plotting
    groups = defaultdict(list)  # period -> list of (x,y,label)
    # For fig1: x=nina34, y=pdo
    # For fig2: x=glb, y=nina34

    for a in analogues:
        y = a['year']; m = a['month']; period = a['period']
        key = (y, m)
        nina_val = nina.get(key, float('nan'))
        pdo_val = pdo.get(key, float('nan'))
        glb_val = glb.get(key, float('nan'))
        # keep only if we have at least one non-nan for intended plot
        groups[period].append({'year': y, 'month': m, 'nina': nina_val, 'pdo': pdo_val, 'glb': glb_val, 'date': a['date']})

    ensure_dir(args.outdir)

    # Colour mapping
    color_map = {'past': 'tab:blue', 'present': 'tab:orange', 'original': 'tab:red'}
    marker_map = {'past': 'o', 'present': 's', 'original': '*'}
    size_map = {'past': 40, 'present': 50, 'original': 140}

    # Figure 1: nina34 vs PDO
    fig1, ax1 = plt.subplots(figsize=(8,6))
    plotted = 0
    for period, items in groups.items():
        xs = []
        ys = []
        labels = []
        for it in items:
            x = it['nina']
            y = it['pdo']
            if x is None or y is None or (math.isnan(x) and math.isnan(y)):
                continue
            if math.isnan(x) or math.isnan(y):
                # skip pairs where either is nan
                continue
            xs.append(x); ys.append(y); labels.append(it['date'])
        if xs:
            ax1.scatter(xs, ys, label=period, c=color_map.get(period,'gray'),
                        marker=marker_map.get(period,'o'), s=size_map.get(period,40), edgecolors='k' if period=='original' else 'none', alpha=0.9)
            plotted += len(xs)
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
    plotted2 = 0
    for period, items in groups.items():
        xs = []
        ys = []
        for it in items:
            x = it['glb']
            y = it['nina']
            if x is None or y is None or (math.isnan(x) and math.isnan(y)):
                continue
            if math.isnan(x) or math.isnan(y):
                continue
            xs.append(x); ys.append(y)
        if xs:
            ax2.scatter(xs, ys, label=period, c=color_map.get(period,'gray'),
                        marker=marker_map.get(period,'o'), s=size_map.get(period,40), edgecolors='k' if period=='original' else 'none', alpha=0.9)
            plotted2 += len(xs)
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
        for period, items in groups.items():
            for it in items:
                nina_v = "" if math.isnan(it['nina']) else f"{it['nina']}"
                pdo_v = "" if math.isnan(it['pdo']) else f"{it['pdo']}"
                glb_v = "" if math.isnan(it['glb']) else f"{it['glb']}"
                cf.write(f"{it['date']},{it['year']},{it['month']},{period},{nina_v},{pdo_v},{glb_v}\n")
    print("Saved extracted values to:", csv_out)

if __name__ == "__main__":
    main()
