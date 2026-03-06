import pandas as pd
import numpy as np
import os
from datetime import timedelta
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────
# ТОХИРГОО
# ─────────────────────────────────────────
data_folder = 'data'
OUTPUT_FILE  = 'output_analysis.xlsx'

CAP_THRESHOLD   = 65    # BATTERY_CAPACITY weak threshold %
MIN_DURATION    = 13    # Хүчингүй cycle хасах босго (мин)
BASELINE_TOP_N  = 10
SLOPE_DAYS      = 30
MIN_CYCLES_PRED = 5     # Predict хийхэд хамгийн бага cycle тоо
MAX_PRED_DAYS   = 1825  # 5 жилээс хол бол "Stable long term"

# Duration level threshold
LEVEL_CRITICAL = 30
LEVEL_AVERAGE  = 60
LEVEL_GOOD     = 120
LEVEL_VERYGOOD = 180

# ─────────────────────────────────────────
# ТУСЛАХ ФУНКЦҮҮД
# ─────────────────────────────────────────

def duration_level(dur):
    if dur <= LEVEL_CRITICAL:
        return 'CRITICAL'
    elif dur <= LEVEL_AVERAGE:
        return 'AVERAGE'
    elif dur <= LEVEL_GOOD:
        return 'GOOD'
    else:
        return 'VERY GOOD'

def is_valid_cycle(row):
    if row['duration_min'] < MIN_DURATION and row['stop_cause'] != 'BATTERY_VOLTAGE':
        return False
    return True

def weighted_regression(recent_df):
    """
    Weighted Linear Regression:
    x = cycle index, y = duration_min
    w = [1, 2, 3, ... n] — сүүлийнх нь их жинтэй
    Буцаах: slope (мин/cycle), intercept, avg_days_per_cycle
    """
    if len(recent_df) < MIN_CYCLES_PRED:
        return None, None, None

    x = np.arange(len(recent_df), dtype=float)
    y = recent_df['duration_min'].values.astype(float)
    w = x + 1

    slope, intercept = np.polyfit(x, y, 1, w=w)

    # Cycle хоорондын дундаж хоног
    dates = recent_df['start_time'].values
    if len(dates) >= 2:
        total_days = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days
        avg_days_per_cycle = total_days / (len(dates) - 1) if total_days > 0 else 2
    else:
        avg_days_per_cycle = 2

    return round(slope, 4), round(intercept, 4), round(avg_days_per_cycle, 2)

def predict_days_to_threshold(current_idx, slope, intercept, threshold, avg_days_per_cycle, today):
    """
    Хэдэн хоногийн дараа threshold-д хүрэх тооцоо.
    current_pred = slope * current_idx + intercept
    cycles_needed = (current_pred - threshold) / abs(slope)
    """
    if slope is None or slope >= 0:
        return 'Improving/Stable'

    current_pred = slope * current_idx + intercept
    if current_pred <= threshold:
        return 'Already below'

    cycles_needed = (current_pred - threshold) / abs(slope)
    days_needed = cycles_needed * avg_days_per_cycle

    if days_needed > MAX_PRED_DAYS:
        return 'Stable long term'
    if days_needed <= 0:
        return 'Already below'

    pred_date = today + timedelta(days=int(days_needed))
    return pred_date.strftime('%Y-%m-%d')

def get_trend(slope):
    if slope is None:
        return 'Insufficient data'
    if slope > 0.3:
        return 'IMPROVING'
    elif slope < -0.3:
        return 'DECLINING'
    else:
        return 'STABLE'

def get_group_cap(row, group):
    if group == 1:
        vals = [row['final_cap_rate1'], row['final_cap_rate2']]
    else:
        vals = [row['final_cap_rate3'], row['final_cap_rate4']]
    valid = [v for v in vals if v > 0]
    return round(np.mean(valid), 2) if valid else np.nan

def group_exists(row, group):
    if group == 1:
        return row['init_cap_rate1'] > 0 or row['init_cap_rate2'] > 0
    else:
        return row['init_cap_rate3'] > 0 or row['init_cap_rate4'] > 0

def determine_weak_group(row):
    g1_exists = group_exists(row, 1)
    g2_exists = group_exists(row, 2)
    g1_cap = row['group1_cap']
    g2_cap = row['group2_cap']
    cause = row['stop_cause']

    if g1_exists and not g2_exists:
        return 'single_group1'
    if g2_exists and not g1_exists:
        return 'single_group2'

    if cause in ['BATTERY_VOLTAGE', 'MAX_TIME']:
        if pd.isna(g1_cap) or pd.isna(g2_cap):
            return 'unknown'
        if g1_cap < g2_cap:
            return 'group1'
        elif g2_cap < g1_cap:
            return 'group2'
        else:
            return 'equal'
    elif cause == 'BATTERY_CAPACITY':
        g1_weak = (not pd.isna(g1_cap)) and (g1_cap <= CAP_THRESHOLD)
        g2_weak = (not pd.isna(g2_cap)) and (g2_cap <= CAP_THRESHOLD)
        if g1_weak and not g2_weak:
            return 'group1'
        elif g2_weak and not g1_weak:
            return 'group2'
        elif g1_weak and g2_weak:
            return 'equal'
        else:
            if g1_cap < g2_cap:
                return 'group1'
            elif g2_cap < g1_cap:
                return 'group2'
            else:
                return 'equal'
    return 'unknown'

# ─────────────────────────────────────────
# ҮНДСЭН БОЛОВСРУУЛАЛТ
# ─────────────────────────────────────────
all_files = [f for f in os.listdir(data_folder) if f.lower().endswith('.csv')]
print(f"{len(all_files)} ширхэг csv")

results = []
today = pd.Timestamp.today().normalize()

for filename in all_files:
    filepath = os.path.join(data_folder, filename)
    try:
        df = pd.read_csv(filepath)
        df['start_time'] = pd.to_datetime(df['start_time'])
        df = df.sort_values('start_time').reset_index(drop=True)

        valid_causes = ['BATTERY_VOLTAGE', 'BATTERY_CAPACITY', 'MAX_TIME']
        df = df[df['stop_cause'].isin(valid_causes)].reset_index(drop=True)
        if len(df) < 1:
            print(f" → {filename}: хоосон, алгасав")
            continue

        # Хүчингүй cycle хасах
        df = df[df.apply(is_valid_cycle, axis=1)].reset_index(drop=True)
        if len(df) < 1:
            print(f" → {filename}: шүүлтийн дараа хоосон")
            continue

        # Group cap
        df['group1_cap'] = df.apply(lambda r: get_group_cap(r, 1), axis=1)
        df['group2_cap'] = df.apply(lambda r: get_group_cap(r, 2), axis=1)
        df['weak_group'] = df.apply(determine_weak_group, axis=1)

        # Battery change илрүүлэх
        battery_change_indices = [0]
        durations = df['duration_min']
        for i in range(1, len(df)):
            if i >= 5 and i + 5 <= len(df):
                pre_med = durations.iloc[i-5:i].median()
                post_med = durations.iloc[i:i+5].median()
                if pre_med > 0 and post_med / pre_med >= 1.5 and post_med >= 60:
                    battery_change_indices.append(i)

        last_change_idx = battery_change_indices[-1]
        battery_change_date = df.loc[last_change_idx, 'start_time']
        recent_df = df.iloc[last_change_idx:].copy().reset_index(drop=True)

        # Baseline & slope (30 хоног)
        top_10 = recent_df['duration_min'].sort_values(ascending=False).head(BASELINE_TOP_N)
        baseline = top_10.median()

        cutoff_date = df['start_time'].max() - pd.Timedelta(days=SLOPE_DAYS)
        last_30_df = df[df['start_time'] >= cutoff_date]
        slope_30 = last_30_df['duration_min'].median() if len(last_30_df) > 0 else recent_df['duration_min'].median()

        last_row = df.iloc[-1]
        last_duration    = last_row['duration_min']
        last_stop_cause  = last_row['stop_cause']
        last_start_time  = last_row['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        ip               = df['IP'].iloc[0]

        drop_percent = round(((baseline - slope_30) / baseline * 100), 2) if baseline > 0 else 0.0
        if drop_percent < 25:
            drop_level = 'NORMAL'
        elif drop_percent <= 55:
            drop_level = 'STRONG'
        else:
            drop_level = 'CRITICAL'

        if last_duration >= 59:
            status = 'stable'
        elif last_duration < 30:
            status = 'critical'
        else:
            status = 'degrading'

        # Хамгийн бага duration ба түүний stop_cause
        min_idx      = recent_df['duration_min'].idxmin()
        min_duration = recent_df.loc[min_idx, 'duration_min']
        min_stop     = recent_df.loc[min_idx, 'stop_cause']
        min_date     = recent_df.loc[min_idx, 'start_time'].strftime('%Y-%m-%d')

        # Duration level
        current_level = duration_level(last_duration)

        # Weighted Linear Regression
        wlr_slope, wlr_intercept, avg_days_cycle = weighted_regression(recent_df)
        trend = get_trend(wlr_slope)
        current_idx = len(recent_df)  # Дараагийн cycle-ийн байрлал

        # Predict
        has_enough = wlr_slope is not None and len(recent_df) >= MIN_CYCLES_PRED

        if current_level == 'VERY GOOD' and has_enough:
            pred_to_good    = predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_GOOD,    avg_days_cycle, today)
            pred_to_average = predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_AVERAGE, avg_days_cycle, today)
            pred_to_critical= predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_CRITICAL,avg_days_cycle, today)
        elif current_level == 'GOOD' and has_enough:
            pred_to_good    = 'Current level'
            pred_to_average = predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_AVERAGE, avg_days_cycle, today)
            pred_to_critical= predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_CRITICAL,avg_days_cycle, today)
        elif current_level == 'AVERAGE' and has_enough:
            pred_to_good    = 'Already passed'
            pred_to_average = 'Current level'
            pred_to_critical= predict_days_to_threshold(current_idx, wlr_slope, wlr_intercept, LEVEL_CRITICAL,avg_days_cycle, today)
        elif current_level == 'CRITICAL':
            pred_to_good    = 'Already critical'
            pred_to_average = 'Already critical'
            pred_to_critical= 'Current level'
        else:
            pred_to_good    = 'Insufficient data'
            pred_to_average = 'Insufficient data'
            pred_to_critical= 'Insufficient data'

        # Weak group (сүүлийн 30 хоног давамгайлсан)
        last_30_weak = df[df['start_time'] >= cutoff_date]['weak_group'].value_counts()
        dominant_weak = last_30_weak.index[0] if len(last_30_weak) > 0 else 'unknown'

        results.append({
            'ip address'              : ip,
            'battery change date'     : battery_change_date.strftime('%Y-%m-%d %H:%M:%S'),
            'baseline'                : round(baseline, 2),
            'slope(30d median)'       : round(slope_30, 2),
            'drop percent'            : drop_percent,
            'drop_level'              : drop_level,
            'last duration_min'       : last_duration,
            'last_start_time'         : last_start_time,
            'last_stop_cause'         : last_stop_cause,
            'critical degrading stable': status,
            # Duration level & prediction
            'current_level'           : current_level,
            'trend'                   : trend,
            'wlr_slope(min/cycle)'    : wlr_slope if wlr_slope is not None else 'N/A',
            'avg_days/cycle'          : avg_days_cycle if avg_days_cycle is not None else 'N/A',
            'pred_date→GOOD'          : pred_to_good,
            'pred_date→AVERAGE'       : pred_to_average,
            'pred_date→CRITICAL'      : pred_to_critical,
            # Min duration
            'min_duration_min'        : min_duration,
            'min_duration_date'       : min_date,
            'min_duration_stop_cause' : min_stop,
            # Group
            'last_weak_group'         : last_row['weak_group'],
            'dominant_weak_30d'       : dominant_weak,
        })

        print(f" ✓ {filename} → {ip} | {current_level} | {trend} | drop: {drop_percent}%")

    except Exception as e:
        print(f" ✗ {filename} алдаа: {e}")

# ─────────────────────────────────────────
# EXCEL ГАРГАХ
# ─────────────────────────────────────────
if not results:
    print("Ямар ч файл боловсруулж чадсангүй!")
else:
    output_df = pd.DataFrame(results)

    # critical → degrading → stable эрэмбэлэх
    status_order = {'critical': 0, 'degrading': 1, 'stable': 2}
    output_df['_sort'] = output_df['critical degrading stable'].map(status_order)
    output_df = output_df.sort_values('_sort').drop(columns='_sort').reset_index(drop=True)

    output_df.to_excel(OUTPUT_FILE, index=False)

    wb = load_workbook(OUTPUT_FILE)
    ws = wb.active

    # Баганын map
    col_map = {}
    for c in range(1, ws.max_column + 1):
        col_map[ws.cell(1, c).value] = c

    # Өнгөний тодорхойлолт
    red    = PatternFill(start_color='FF4444', end_color='FF4444', fill_type='solid')
    orange = PatternFill(start_color='FFA500', end_color='FFA500', fill_type='solid')
    yellow = PatternFill(start_color='FFD700', end_color='FFD700', fill_type='solid')
    green  = PatternFill(start_color='00CC66', end_color='00CC66', fill_type='solid')
    blue   = PatternFill(start_color='4FC3F7', end_color='4FC3F7', fill_type='solid')
    gray   = PatternFill(start_color='CCCCCC', end_color='CCCCCC', fill_type='solid')

    for row in range(2, ws.max_row + 1):

        # critical degrading stable
        if 'critical degrading stable' in col_map:
            val = ws.cell(row, col_map['critical degrading stable']).value
            if val == 'critical':   ws.cell(row, col_map['critical degrading stable']).fill = red
            elif val == 'degrading':ws.cell(row, col_map['critical degrading stable']).fill = orange
            elif val == 'stable':   ws.cell(row, col_map['critical degrading stable']).fill = green

        # drop_level
        if 'drop_level' in col_map:
            val = ws.cell(row, col_map['drop_level']).value
            if val == 'STRONG':   ws.cell(row, col_map['drop_level']).fill = orange
            elif val == 'CRITICAL':ws.cell(row, col_map['drop_level']).fill = red
            elif val == 'NORMAL': ws.cell(row, col_map['drop_level']).fill = green

        # current_level
        if 'current_level' in col_map:
            val = ws.cell(row, col_map['current_level']).value
            if val == 'CRITICAL':  ws.cell(row, col_map['current_level']).fill = red
            elif val == 'AVERAGE': ws.cell(row, col_map['current_level']).fill = orange
            elif val == 'GOOD':    ws.cell(row, col_map['current_level']).fill = yellow
            elif val == 'VERY GOOD':ws.cell(row, col_map['current_level']).fill = green

        # trend
        if 'trend' in col_map:
            val = ws.cell(row, col_map['trend']).value
            if val == 'DECLINING':   ws.cell(row, col_map['trend']).fill = red
            elif val == 'STABLE':    ws.cell(row, col_map['trend']).fill = yellow
            elif val == 'IMPROVING': ws.cell(row, col_map['trend']).fill = green

        # weak_group өнгө
        for col_name in ['last_weak_group', 'dominant_weak_30d']:
            if col_name in col_map:
                val = ws.cell(row, col_map[col_name]).value
                if val == 'group1':          ws.cell(row, col_map[col_name]).fill = orange
                elif val == 'group2':        ws.cell(row, col_map[col_name]).fill = yellow
                elif val == 'equal':         ws.cell(row, col_map[col_name]).fill = red
                elif val and val.startswith('single'): ws.cell(row, col_map[col_name]).fill = gray

    # Header
    header_fill = PatternFill(start_color='1F3864', end_color='1F3864', fill_type='solid')
    header_font = Font(color='FFFFFF', bold=True)
    for c in range(1, ws.max_column + 1):
        ws.cell(1, c).fill = header_fill
        ws.cell(1, c).font = header_font
        ws.cell(1, c).alignment = Alignment(horizontal='center', wrap_text=True)
        ws.column_dimensions[get_column_letter(c)].width = 22

    ws.row_dimensions[1].height = 35
    wb.save(OUTPUT_FILE)

    print(f"\n{'='*55}")
    print(f"ДУУСЛАА: {len(results)} IP → {OUTPUT_FILE}")
    print(f"\nНэмэгдсэн баганууд:")
    print(f"  current_level          → CRITICAL / AVERAGE / GOOD / VERY GOOD")
    print(f"  trend                  → IMPROVING / STABLE / DECLINING")
    print(f"  wlr_slope(min/cycle)   → Weighted regression slope")
    print(f"  pred_date→GOOD/AVERAGE/CRITICAL → Хэзээ level буурах")
    print(f"  min_duration_min/date/stop_cause → Хамгийн бага cycle")
    print(f"  last_weak_group / dominant_weak_30d → Battery group анализ")