"""
Temporary script to select analogues from pre-calculated all_distances.csv

Usage:
    python select_analogues_from_distances.py
    
This reads the existing all_distances.csv and applies the new period ranges
(past: 1948-1987, present: 1988-2026) to select analogues.
"""

import pandas as pd
from pathlib import Path

# =============================================================================
# Configuration - edit as needed
# =============================================================================
DISTANCES_FILE = Path("/home/yi260/rds/hpc-work/analogue/Data/F02_analogue_search/era5/antarctica_peninsula_2020/all_distances.csv")
OUTPUT_DIR = DISTANCES_FILE.parent

# Period ranges
PAST_START = 1948
PAST_END = 1987
PRESENT_START = 1988
PRESENT_END = 2026

# Analogue selection parameters
N_ANALOGUES = 15
MIN_SEPARATION_DAYS = 5

# Snapshot date to exclude (and nearby dates)
SNAPSHOT_DATE = pd.Timestamp("2020-02-08")

# =============================================================================
# Functions
# =============================================================================

def select_time_separated_analogues(
    df: pd.DataFrame,
    n_analogues: int,
    time_col: str = 'date',
    distance_col: str = 'distance',
    min_separation: pd.Timedelta = pd.Timedelta('5D')
) -> pd.DataFrame:
    """
    Select top N analogues with minimum time separation.
    """
    df_sorted = df.sort_values(distance_col).reset_index(drop=True)
    
    chosen_indices = []
    chosen_dates = []
    
    for idx, row in df_sorted.iterrows():
        candidate_date = row[time_col]
        
        is_separated = True
        for chosen_date in chosen_dates:
            if abs(candidate_date - chosen_date) < min_separation:
                is_separated = False
                break
        
        if is_separated:
            chosen_indices.append(idx)
            chosen_dates.append(candidate_date)
            
            if len(chosen_indices) >= n_analogues:
                break
    
    result = df_sorted.loc[chosen_indices].copy()
    result['rank'] = range(1, len(result) + 1)
    
    return result


def main():
    print("=" * 60)
    print("Select Analogues from Pre-calculated Distances")
    print("=" * 60)
    print(f"Input: {DISTANCES_FILE}")
    print(f"Past period: {PAST_START}-{PAST_END}")
    print(f"Present period: {PRESENT_START}-{PRESENT_END}")
    print(f"N analogues: {N_ANALOGUES}")
    print(f"Min separation: {MIN_SEPARATION_DAYS} days")
    print()
    
    # Load distances
    print("Loading distances...")
    df = pd.read_csv(DISTANCES_FILE)
    df['date'] = pd.to_datetime(df['date'])
    print(f"  Total rows: {len(df)}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Distance range: {df['distance'].min():.2e} to {df['distance'].max():.2e}")
    print()
    
    # Define period masks
    min_separation = pd.Timedelta(days=MIN_SEPARATION_DAYS)
    
    past_mask = (df['year'] >= PAST_START) & (df['year'] <= PAST_END)
    present_mask = (df['year'] >= PRESENT_START) & (df['year'] <= PRESENT_END)
    
    # Exclude snapshot date
    snapshot_exclusion = (
        (df['date'] >= SNAPSHOT_DATE - min_separation) & 
        (df['date'] <= SNAPSHOT_DATE + min_separation)
    )
    past_mask = past_mask & ~snapshot_exclusion
    present_mask = present_mask & ~snapshot_exclusion
    
    print(f"Past candidates: {past_mask.sum()}")
    print(f"Present candidates: {present_mask.sum()}")
    print()
    
    # Select past analogues
    print("Selecting past analogues...")
    past_candidates = df[past_mask].copy()
    past_df = select_time_separated_analogues(
        past_candidates,
        n_analogues=N_ANALOGUES,
        min_separation=min_separation
    )
    past_df['period'] = 'past'
    
    print(f"\n--- Top {len(past_df)} Past Analogues ---")
    print(past_df[['rank', 'date', 'distance']].to_string(index=False))
    
    # Select present analogues
    print("\nSelecting present analogues...")
    present_candidates = df[present_mask].copy()
    present_df = select_time_separated_analogues(
        present_candidates,
        n_analogues=N_ANALOGUES,
        min_separation=min_separation
    )
    present_df['period'] = 'present'
    
    print(f"\n--- Top {len(present_df)} Present Analogues ---")
    print(present_df[['rank', 'date', 'distance']].to_string(index=False))
    
    # Save results
    past_file = OUTPUT_DIR / 'past_analogues.csv'
    present_file = OUTPUT_DIR / 'present_analogues.csv'
    combined_file = OUTPUT_DIR / 'analogues.csv'
    
    past_df.to_csv(past_file, index=False)
    present_df.to_csv(present_file, index=False)
    
    combined = pd.concat([past_df, present_df], ignore_index=True)
    combined.to_csv(combined_file, index=False)
    
    print(f"\n--- Results saved ---")
    print(f"  Past: {past_file}")
    print(f"  Present: {present_file}")
    print(f"  Combined: {combined_file}")
    print("=" * 60)


if __name__ == '__main__':
    main()
