"""
Battery Discharge Analysis Script
==================================
Ижил бүтэцтэй (discharge data) CSV файл дээр ажилладаг.

Ашиглах:
    python battery_analysis.py <csv_file_path>

Шүүлтүүр:
    start_cause = SCHEDULED
    stop_cause  ∈ {MAX_TIME, BATTERY_VOLTAGE, BATTERY_CAPACITY}

Батерей солилт илрүүлэх:
    stop_cause холилдолт: өмнөх 10 row дотор BATTERY_VOLTAGE давамгай байсан бол
    дараагийн 5 row дотор BATTERY_CAPACITY давамгай болвол → солигдсон

Threshold логик:
    Good   : duration > 180 min
    Medium : 30 <= duration <= 180 min
    Bad    : duration < 30 min
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
GOOD_THRESHOLD   = 180
BAD_THRESHOLD    = 30

VALID_START_CAUSE = ['SCHEDULED']
VALID_STOP_CAUSE  = ['MAX_TIME', 'BATTERY_VOLTAGE', 'BATTERY_CAPACITY']

# Батерей солилт илрүүлэх параметр
PREV_WINDOW      = 10   # өмнөх N row
NEXT_WINDOW      = 5    # дараагийн N row
PREV_CAP_MAX     = 0.2  # өмнөх window дотор CAPACITY-н харьцаа < 20%
NEXT_CAP_MIN     = 0.6  # дараагийн window дотор CAPACITY-н харьцаа > 60%
MIN_GAP          = 5    # дараалсан changepoint-уудын хоорондын min зай


def classify(d):
    if d > GOOD_THRESHOLD:
        return 'good'
    elif d >= BAD_THRESHOLD:
        return 'medium'
    else:
        return 'bad'


def load_data(filepath):
    df = pd.read_csv(filepath)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time']   = pd.to_datetime(df['end_time'])

    # ── Алхам 1: Он сараар эрэмблэх ──
    df = df.sort_values('start_time').reset_index(drop=True)

    total = len(df)

    # ── Алхам 2: Шүүлт ──
    mask     = (df['start_cause'].isin(VALID_START_CAUSE)) & \
               (df['stop_cause'].isin(VALID_STOP_CAUSE))
    filtered = df[mask].reset_index(drop=True)
    kept     = len(filtered)

    print(f"\n🔍 Filter результат:")
    print(f"   Нийт мөр       : {total}")
    print(f"   Шүүлтээр үлдсэн: {kept}  (хасагдсан {total - kept} мөр)")
    if kept > 0:
        print(f"   stop_cause тархалт:")
        for val, cnt in filtered['stop_cause'].value_counts().items():
            print(f"     {val}: {cnt}")

    return filtered


def detect_replacements(df):
    """
    Батерей солилт = stop_cause-н холилдолт өөрчлөгдөх үе:
    өмнөх PREV_WINDOW row дотор BATTERY_CAPACITY < PREV_CAP_MAX  (голдуу VOLTAGE)
    дараагийн NEXT_WINDOW row дотор BATTERY_CAPACITY > NEXT_CAP_MIN (голдуу CAPACITY)
    """
    df['is_capacity'] = df['stop_cause'].eq('BATTERY_CAPACITY').astype(int)

    candidates = []
    for i in range(PREV_WINDOW, len(df) - NEXT_WINDOW):
        prev_ratio = df['is_capacity'].iloc[i - PREV_WINDOW:i].mean()
        next_ratio = df['is_capacity'].iloc[i:i + NEXT_WINDOW].mean()
        if prev_ratio < PREV_CAP_MAX and next_ratio >= NEXT_CAP_MIN:
            candidates.append(i)

    # Дараалсан индексүүдийг нэгтгэх — бүлгийн эхний индексийг авна
    changepoints = []
    for idx in candidates:
        if not changepoints or idx - changepoints[-1] > MIN_GAP:
            changepoints.append(idx)

    return df.iloc[changepoints].copy() if changepoints else pd.DataFrame()


def plot_analysis(df, replacements, output_path, title):
    fig, ax = plt.subplots(figsize=(14, 5))

    color_map = {'good': '#2ecc71', 'medium': '#f39c12', 'bad': '#e74c3c'}
    for status, group in df.groupby('status'):
        ax.scatter(group['start_time'], group['duration_min'],
                   c=color_map[status], alpha=0.7, s=20)

    # Battery replacement lines
    for _, row in replacements.iterrows():
        ax.axvline(row['start_time'], color='blue', linestyle='--',
                   alpha=0.8, linewidth=1.5)

    # Threshold lines
    ax.axhline(GOOD_THRESHOLD, color='green', linestyle=':', linewidth=1)
    ax.axhline(BAD_THRESHOLD,  color='red',   linestyle=':', linewidth=1)

    # Legend
    patches = [mpatches.Patch(color=c, label=s.capitalize()) for s, c in color_map.items()]
    patches += [
        mpatches.Patch(color='blue', label='Battery replacement'),
        plt.Line2D([0],[0], color='green', linestyle=':', label=f'{GOOD_THRESHOLD} min (good)'),
        plt.Line2D([0],[0], color='red',   linestyle=':', label=f'{BAD_THRESHOLD} min (bad)'),
    ]
    ax.legend(handles=patches, loc='upper right', fontsize=8)

    ax.set_xlabel('Date')
    ax.set_ylabel('Duration (min)')
    ax.set_title(title)
    ax.set_ylim(0, max(df['duration_min'].max() * 1.15, 50))

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n📊 Chart saved → {output_path}")


def print_report(df, replacements, filepath):
    site = df['site_location'].iloc[0] if 'site_location' in df.columns else 'Unknown'
    ip   = df['IP'].iloc[0] if 'IP' in df.columns else os.path.basename(filepath)

    print("\n" + "=" * 55)
    print(f"  Battery Analysis Report")
    print(f"  IP   : {ip}")
    print(f"  Site : {site}")
    print(f"  Data : {df['start_time'].iloc[0].date()} → {df['start_time'].iloc[-1].date()}")
    print(f"  Нийт discharge (шүүлтийн дараа): {len(df)}")
    print("=" * 55)

    counts = df['status'].value_counts()
    total  = len(df)
    print("\n📋 Status Summary:")
    for status in ['good', 'medium', 'bad']:
        n   = counts.get(status, 0)
        pct = n / total * 100
        icon  = {'good': '✅', 'medium': '🟡', 'bad': '🔴'}[status]
        label = {'good': f'> {GOOD_THRESHOLD} min',
                 'medium': f'{BAD_THRESHOLD}–{GOOD_THRESHOLD} min',
                 'bad': f'< {BAD_THRESHOLD} min'}[status]
        print(f"  {icon} {status.upper():8s} ({label:15s}): {n:4d}  ({pct:.1f}%)")

    print("\n🔧 Battery Replacement Detected:")
    if replacements.empty:
        print("  None detected")
    else:
        for _, row in replacements.iterrows():
            print(f"  📅 {row['start_time'].date()}  "
                  f"(discharge #{row['discharge_id']}, "
                  f"stop_cause: {row['stop_cause']}, "
                  f"duration: {row['duration_min']} min)")

    recent         = df.tail(10)
    current_status = recent['status'].mode()[0]
    current_avg    = recent['duration_min'].mean()
    icon = {'good': '✅', 'medium': '🟡', 'bad': '🔴'}[current_status]
    print(f"\n{icon} Current Status  : {current_status.upper()}")
    print(f"   Avg (last 10)  : {current_avg:.1f} min")
    print(f"   Last discharge : {df.iloc[-1]['start_time'].date()} — {df.iloc[-1]['duration_min']} min")

    print("\n💡 Recommendation:")
    if current_status == 'bad':
        print("  ⚠️  Battery is in BAD condition. Replacement recommended!")
    elif current_status == 'medium':
        print("  🔍 Battery is in MEDIUM condition. Monitor closely.")
    else:
        print("  👍 Battery is in GOOD condition.")
    print("=" * 55)


def main():
    if len(sys.argv) < 2:
        print("Usage: python battery_analysis.py <csv_file_path>")
        sys.exit(1)

    filepath = sys.argv[1]
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        sys.exit(1)

    print(f"📂 Loading: {filepath}")

    # 1. Load + Sort + Filter
    df = load_data(filepath)
    if df.empty:
        print("❌ Шүүлтийн дараа өгөгдөл хоосон байна.")
        sys.exit(1)

    # 2. Classify
    df['status'] = df['duration_min'].apply(classify)

    # 3. Detect battery replacements (stop_cause холилдолт)
    replacements = detect_replacements(df)

    # 4. Report
    print_report(df, replacements, filepath)

    # 5. Plot
    base_name   = os.path.splitext(os.path.basename(filepath))[0]
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, f"{base_name}_battery_analysis.png")

    ip_label = df['IP'].iloc[0] if 'IP' in df.columns else base_name
    plot_analysis(df, replacements, output_path,
                  title=f"{ip_label} — Battery Discharge Duration & Status")


if __name__ == '__main__':
    main()