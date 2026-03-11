#!/usr/bin/env python3
"""
make_index_scatter.py

Usage:
  python3 make_index_scatter.py --analogues path/to/analogues.csv \
    --nina path/to/nina34.anom.data --pdo path/to/pdo.timeseries.sstens.data \
    --glb path/to/GLB.Ts+dSST.txt --sam path/to/sam_indices_era5.csv \
    --outdir path/to/output_dir [--original-date YYYY-MM-DD]

This script:
 - reads analogues.csv (CSV with rows like: date,timer,year,month,day,rank,period)
 - parses monthly index files (robust to leading headers; finds lines starting with 4-digit year)
 - linearly interpolates monthly values to daily resolution using the 15th of
   each month as the anchor point, so that analogue snapshots (which are daily)
   get a smoothly interpolated index value rather than a step-function
 - for each analogue (past/present/original) extracts the interpolated index values
 - makes three scatter figures:
     Figure1: NINO3.4 (x) vs PDO (y)
     Figure2: GLB.Ts  (x) vs NINO3.4 (y)
     Figure3: SAM     (x) vs NINO3.4 (y)
 - saves PNGs into outdir
"""

import argparse
import csv
import os
import re
from collections import defaultdict
from datetime import datetime, date, timedelta
import math

import matplotlib.pyplot as plt

MISSING_SENTINEL = -999.0

# ---------------------
# Parsers for index tables
# ---------------------
def parse_year_month_table(path, months_per_line=12):
    """
    Generic year-month table parser.
    Returns dict[(year,month)] = float_value (monthly values).
    Lines starting with a 4-digit year followed by 12 numeric tokens are parsed.
    Values equal to the missing sentinel (-999) are stored as NaN.
    """
    vals = {}
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = re.match(r'^(\d{4})\s+(.+)$', line)
            if not m:
                continue
            year = int(m.group(1))
            rest = m.group(2)
            tokens = re.split(r'\s+', rest.strip())
            month_vals = []
            for tok in tokens:
                tok_clean = tok.replace('*', '').replace('NA', 'nan')
                try:
                    val = float(tok_clean)
                    month_vals.append(val)
                except Exception:
                    continue
                if len(month_vals) >= months_per_line:
                    break
            if len(month_vals) >= months_per_line:
                for m_idx, v in enumerate(month_vals[:months_per_line], start=1):
                    if v <= MISSING_SENTINEL:
                        v = float('nan')
                    vals[(year, m_idx)] = v
    return vals

def parse_nina34_psl(path):
    return parse_year_month_table(path, months_per_line=12)

def parse_pdo_psl(path):
    return parse_year_month_table(path, months_per_line=12)

def parse_gistemp_table(path):
    return parse_year_month_table(path, months_per_line=12)

def parse_sam_psl(path):
    return parse_year_month_table(path, months_per_line=12)


def parse_sam_era5_csv(path):
    """
    Parse sam_indices_era5.csv (date,sam_eof,sam_gong).
    Uses sam_eof column only. Returns dict[(year, month)] = float.
    """
    vals = {}
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row.get('date', '').strip()
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            try:
                v = float(row.get('sam_eof', ''))
            except (ValueError, TypeError):
                v = float('nan')
            vals[(dt.year, dt.month)] = v
    return vals

# ---------------------
# Monthly → daily interpolation
# ---------------------
def _mid_month_date(year, month):
    """Return the 15th of the given year/month as a date object."""
    return date(year, month, 15)


def _prev_month(year, month):
    if month == 1:
        return (year - 1, 12)
    return (year, month - 1)


def _next_month(year, month):
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)


def interpolate_daily(monthly_vals, year, month, day):
    """
    Linearly interpolate a monthly index value to a specific day.

    Each monthly value is anchored at the 15th of its month.  For a target
    day, we interpolate between the two nearest mid-month anchors:
      - day <= 15  → between previous month's 15th and this month's 15th
      - day >  15  → between this month's 15th and next month's 15th

    Returns float (may be NaN if either bounding month is missing).
    """
    target = date(year, month, day)

    if day <= 15:
        y0, m0 = _prev_month(year, month)
        y1, m1 = year, month
    else:
        y0, m0 = year, month
        y1, m1 = _next_month(year, month)

    v0 = monthly_vals.get((y0, m0), float('nan'))
    v1 = monthly_vals.get((y1, m1), float('nan'))

    if math.isnan(v0) or math.isnan(v1):
        return monthly_vals.get((year, month), float('nan'))

    d0 = _mid_month_date(y0, m0)
    d1 = _mid_month_date(y1, m1)
    span = (d1 - d0).days
    if span == 0:
        return v0
    frac = (target - d0).days / span
    return v0 + frac * (v1 - v0)


# ---------------------
# Read analogues.csv
# ---------------------
def read_analogues(path):
    """
    Expect CSV with at least columns:
    date_str, someval, year, month, day, rank, period
    Returns list of dicts with 'date', 'year', 'month', 'day', 'period'.
    """
    rows = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        for r in reader:
            if not r:
                continue
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
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    year, month, day = dt.year, dt.month, dt.day
                    period = r[-1].strip() if len(r) > 1 else 'past'
                rows.append({'date': date_str, 'year': year, 'month': month, 'day': day, 'period': period})
            except Exception:
                continue
    return rows

# ---------------------
# Helpers
# ---------------------
def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def add_original_to_analogues(analogues, original_date_str):
    """
    If original_date_str is provided and there is no row with period 'original',
    add a single row derived from original_date_str.
    """
    if not original_date_str:
        return analogues
    for r in analogues:
        if r.get('period') == 'original':
            return analogues
    try:
        dt = datetime.fromisoformat(original_date_str)
    except Exception:
        try:
            dt = datetime.strptime(original_date_str, "%Y-%m-%d")
        except Exception:
            return analogues
    new_row = {'date': dt.strftime("%Y-%m-%d"), 'year': dt.year, 'month': dt.month, 'day': dt.day, 'period': 'original'}
    return [new_row] + analogues


def _scatter_panel(ax, groups, xkey, ykey, xlabel, ylabel, title, color_map, marker_map, size_map):
    """Draw a single scatter panel for two index keys."""
    for period in ['past', 'present', 'original']:
        items = groups.get(period, [])
        xs, ys = [], []
        for it in items:
            x, y = it[xkey], it[ykey]
            if x is None or y is None or math.isnan(x) or math.isnan(y):
                continue
            xs.append(x)
            ys.append(y)
        if xs:
            edgecol = 'k' if period == 'original' else None
            zord = 5 if period == 'original' else 3
            ax.scatter(xs, ys, label=period, c=color_map.get(period, 'gray'),
                       marker=marker_map.get(period, 'o'), s=size_map.get(period, 40),
                       edgecolors=edgecol, alpha=0.95, zorder=zord)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, linestyle=':', alpha=0.4)
    ax.legend(title='period')


# ---------------------
# Main
# ---------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--analogues", required=True)
    p.add_argument("--nina", required=True)
    p.add_argument("--pdo", required=True)
    p.add_argument("--glb", required=True)
    p.add_argument("--sam", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--original-date", default=None,
                   help="YYYY-MM-DD snapshot/original date to highlight")
    args = p.parse_args()

    analogues = read_analogues(args.analogues)
    if analogues is None:
        analogues = []
    analogues = add_original_to_analogues(analogues, args.original_date)

    if len(analogues) == 0:
        print("No analogues found in", args.analogues)
        return

    nina = parse_nina34_psl(args.nina)
    pdo = parse_pdo_psl(args.pdo)
    glb = parse_gistemp_table(args.glb)
    if args.sam.lower().endswith('.csv'):
        sam = parse_sam_era5_csv(args.sam)
    else:
        sam = parse_sam_psl(args.sam)

    groups = defaultdict(list)

    for a in analogues:
        y, m, d, period = a['year'], a['month'], a['day'], a['period']
        nina_val = interpolate_daily(nina, y, m, d)
        pdo_val = interpolate_daily(pdo, y, m, d)
        glb_val = interpolate_daily(glb, y, m, d)
        sam_val = interpolate_daily(sam, y, m, d)
        groups[period].append({
            'year': y, 'month': m, 'day': d,
            'nina': nina_val, 'pdo': pdo_val, 'glb': glb_val, 'sam': sam_val,
            'date': a['date'],
        })

    ensure_dir(args.outdir)

    color_map = {'past': 'tab:blue', 'present': 'tab:orange', 'original': 'tab:red'}
    marker_map = {'past': 'o', 'present': 's', 'original': '*'}
    size_map = {'past': 40, 'present': 50, 'original': 200}

    # Figure 1: NINO3.4 vs PDO
    fig1, ax1 = plt.subplots(figsize=(8, 6))
    _scatter_panel(ax1, groups, 'nina', 'pdo',
                   "NINO3.4 anomaly",
                   "PDO index",
                   "Figure 1 — NINO3.4 vs PDO (analogues)",
                   color_map, marker_map, size_map)
    fig1_fn = os.path.join(args.outdir, "fig1_nina34_vs_pdo.png")
    fig1.savefig(fig1_fn, dpi=200, bbox_inches='tight')
    print("Saved:", fig1_fn)

    # Figure 2: GLB.Ts vs NINO3.4
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    _scatter_panel(ax2, groups, 'glb', 'nina',
                   "GISTEMP GLB.Ts+dSST (°C anomaly)",
                   "NINO3.4 anomaly",
                   "Figure 2 — GLB.Ts vs NINO3.4 (analogues)",
                   color_map, marker_map, size_map)
    fig2_fn = os.path.join(args.outdir, "fig2_glb_vs_nina34.png")
    fig2.savefig(fig2_fn, dpi=200, bbox_inches='tight')
    print("Saved:", fig2_fn)

    # Figure 3: SAM vs NINO3.4
    fig3, ax3 = plt.subplots(figsize=(8, 6))
    _scatter_panel(ax3, groups, 'sam', 'nina',
                   "SAM index (ERA5 EOF)",
                   "NINO3.4 anomaly",
                   "Figure 3 — SAM vs NINO3.4 (analogues)",
                   color_map, marker_map, size_map)
    fig3_fn = os.path.join(args.outdir, "fig3_sam_vs_nina34.png")
    fig3.savefig(fig3_fn, dpi=200, bbox_inches='tight')
    print("Saved:", fig3_fn)

    csv_out = os.path.join(args.outdir, "index_values_extracted.csv")
    with open(csv_out, 'w', encoding='utf-8') as cf:
        cf.write("date,year,month,day,period,nina34,pdo,glb,sam\n")
        for period in ['original', 'present', 'past']:
            for it in groups.get(period, []):
                def _fmt(v):
                    return "" if math.isnan(v) else f"{v:.4f}"
                cf.write(f"{it['date']},{it['year']},{it['month']},{it['day']},"
                         f"{period},{_fmt(it['nina'])},{_fmt(it['pdo'])},"
                         f"{_fmt(it['glb'])},{_fmt(it['sam'])}\n")
    print("Saved extracted values to:", csv_out)

if __name__ == "__main__":
    main()
