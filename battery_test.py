import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

data_folder = 'data'
all_files = [f for f in os.listdir(data_folder) if f.lower().endswith('.csv')]
print(f"{len(all_files)} ширхэг csv")
results = []
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
        battery_change_indices = [0]
        durations = df['duration_min']
        # stops = df['stop_cause']
        for i in range(1, len(df)):
            # if stops.iloc[i] != stops.iloc[i-1]:
            #     battery_change_indices.append(i)
            # else:
                if i >= 5 and i + 5 <= len(df):
                    pre_med = durations.iloc[i-5:i].median()
                    post_med = durations.iloc[i:i+5].median()
                    if pre_med > 0 and post_med / pre_med >= 1.5 and post_med >= 60:
                        battery_change_indices.append(i)
        last_change_idx = battery_change_indices[-1]
        battery_change_date = df.loc[last_change_idx, 'start_time']
        recent_df = df.iloc[last_change_idx:].copy()
        # Battery_change hiisnees hois top10
        top_10 = recent_df['duration_min'].sort_values(ascending=False).head(10)
        baseline = top_10.median()
        # Suuliin 30 honog dahi median 
        cutoff_date = df['start_time'].max() - pd.Timedelta(days=30)
        last_30_df = df[df['start_time'] >= cutoff_date]
        slope = last_30_df['duration_min'].median() if len(last_30_df) > 0 else recent_df['duration_min'].median()

        last_duration = df.iloc[-1]['duration_min']
        drop_percent = round(((baseline - slope) / baseline * 100), 2) if baseline > 0 else 0.0
        if drop_percent < 25:
            drop_level = "NORMAL"
        elif drop_percent <= 55:
            drop_level = "STRONG"
        else:
            drop_level = "CRITICAL"
        last_stop_cause = df.iloc[-1]['stop_cause']
        if last_duration >= 59:
            status = "stable"
        elif last_duration < 30:
            status = "critical"
        else:
            status = "degrading"
        ip = df['IP'].iloc[0]
        last_start_time = df.iloc[-1]['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        results.append({
            'ip address': ip,
            'battery change date': battery_change_date.strftime('%Y-%m-%d %H:%M:%S'),
            'baseline': round(baseline, 2),
            'slope': round(slope, 2),
            'drop percent': drop_percent,
            'last duration_min': last_duration,
            'last_start_time': last_start_time,
            'critical degrading stable': status,
            'drop_level': drop_level
        })
        print(f" ✓ {filename} → {ip} (last: {last_duration} мин | {last_stop_cause} | drop: {drop_percent}%)")
    except Exception as e:
        print(f" ✗ {filename} алдаа: {e}")
if not results:
    print("Ямар ч файл боловсруулж чадсангүй!")
else:
    output_df = pd.DataFrame(results)
    output_df = output_df[['ip address', 'battery change date', 'baseline', 'slope',
                           'drop percent', 'last duration_min', 'last_start_time',
                            'critical degrading stable', 'drop_level']]
    output_df.to_excel('output_analysis.xlsx', index=False)
    wb = load_workbook('output_analysis.xlsx')
    ws = wb.active
    status_col = None
    drop_col = None
    for col in range(1, ws.max_column + 1):
        if ws.cell(1, col).value == 'critical degrading stable':
            status_col = col
        if ws.cell(1, col).value == 'drop_level':
            drop_col = col
    if status_col:
        red_fill = PatternFill(start_color='FF0000', end_color='FF0000', fill_type='solid')
        orange_fill = PatternFill(start_color='FFA500', end_color='FFA500', fill_type='solid')
        green_fill = PatternFill(start_color='00FF00', end_color='00FF00', fill_type='solid')
        for row in range(2, ws.max_row + 1):
            val = ws.cell(row, status_col).value
            if val == 'critical':
                ws.cell(row, status_col).fill = red_fill
            elif val == 'degrading':
                ws.cell(row, status_col).fill = orange_fill
            elif val == 'stable':
                ws.cell(row, status_col).fill = green_fill
    if drop_col:
        for row in range(2, ws.max_row + 1):
            val = ws.cell(row, drop_col).value
            if val == "STRONG":
                ws.cell(row, drop_col).fill = orange_fill
            elif val == "CRITICAL":
                ws.cell(row, drop_col).fill = red_fill
    wb.save('output_analysis.xlsx')
    print("\n=== БҮХ АНАЛИЗ ДУУСЛАА ===")
    print(f"Нийт {len(results)} IP-ийн анализ хийгдэж, output_analysis.xlsx файл үүсгэгдлээ!")
    print("• Baseline: сүүлийн баттерийн TOP 10 утгын median")
    print("• Slope: сүүлийн 30 хоногийн median")
    print("• drop_level: NORMAL / STRONG (25-65%) / CRITICAL (>65%)")
    print("\nХамгийн сүүлийн 5 IP:")
    print(output_df[['ip address', 'last_start_time',
                     'last duration_min', 'drop percent', 'drop_level',
                     'critical degrading stable']].tail(5))