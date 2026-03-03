import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

def detect_battery_change(df, soh_col='soh', threshold=95):
    """
    SOH багана ашиглан батерей солигдсон цэгийг илрүүлэх
    threshold-оос дээш гарсан анхны цэгийг шинэ батерейны эхлэл гэж үзнэ.
    Олдохгүй бол 0-г буцаана (бүх өгөгдлийг ашиглана).
    """
    above = df[df[soh_col] > threshold]
    if len(above) > 0:
        return above.index[0]
    return 0

def estimate_soh(df):
    """duration_min ба final_cap_rate ашиглан SOH тооцоолох"""
    # Алдагдсан хувь
    lost = 100 - df['final_cap_rate']
    lost = lost.clip(lower=0.1)  # 0-д хуваахаас сэргийлэх
    
    df['capacity_ratio'] = df['duration_min'] / lost
    
    # Жишиг харьцааг MAX_TIME бичлэгүүдээс авна
    healthy = df[df['stop_cause'] == 'MAX_TIME']
    if len(healthy) >= 3:
        ref_ratio = healthy['capacity_ratio'].mean()
    else:
        # Хамгийн сайн 10%-ийн дундаж
        top_thresh = df['capacity_ratio'].quantile(0.9)
        good = df[df['capacity_ratio'] >= top_thresh]
        if len(good) > 0:
            ref_ratio = good['capacity_ratio'].mean()
        else:
            ref_ratio = df['capacity_ratio'].mean()
    
    if pd.isna(ref_ratio) or ref_ratio <= 0:
        ref_ratio = df['capacity_ratio'].mean()
    
    df['soh'] = (df['capacity_ratio'] / ref_ratio) * 100
    df['soh'] = df['soh'].clip(0, 100)
    return df

def analyze_battery(df_batt, batt_name, site_name, interval_days=2):
    """Нэг батерейны өгөгдлийг шинжлэх (солигдсоноос хойшхи)"""
    if len(df_batt) < 3:
        print(f"  {batt_name}: хэт цөөн өгөгдөл, алгасах")
        return None
    
    # Эхлээд SOH тооцоол
    df_batt = estimate_soh(df_batt)
    
    # Солигдсон цэгийг илрүүлэх
    change_idx = detect_battery_change(df_batt, soh_col='soh', threshold=95)
    df_new = df_batt.iloc[change_idx:].copy()
    
    if len(df_new) == 0:
        print(f"  {batt_name}: шинэ батерейны өгөгдөл олдсонгүй")
        return None
    
    df_new = df_new.reset_index(drop=True)
    df_new['cycle_number'] = range(1, len(df_new)+1)
    
    print(f"\n  {batt_name}: сүүлийн батерей {len(df_new)} цикл (эхлэл: {df_new['start_time'].iloc[0].date()})")
    
    if len(df_new) < 3:
        print("    Хэт цөөн өгөгдөл, таамаглал хийх боломжгүй")
        return None
    
    # Шугаман регресс
    X = df_new['cycle_number'].values.reshape(-1, 1)
    y = df_new['soh'].values
    model = LinearRegression()
    model.fit(X, y)
    
    last_cycle = df_new['cycle_number'].iloc[-1]
    last_date = df_new['start_time'].iloc[-1]
    last_soh = df_new['soh'].iloc[-1]
    
    # Дараагийн 5 циклийн таамаглал (10 хоног)
    future_cycles = 5
    future_cycle_nums = np.array([last_cycle + i + 1 for i in range(future_cycles)]).reshape(-1,1)
    future_soh = model.predict(future_cycle_nums)
    future_soh = np.clip(future_soh, 0, 100)
    future_dates = [last_date + timedelta(days=interval_days*(i+1)) for i in range(future_cycles)]
    
    # 80% босго
    slope = model.coef_[0]
    if slope < 0:
        cycles_to_80 = (80 - last_soh) / slope
        if cycles_to_80 > 0:
            days_to_80 = cycles_to_80 * interval_days
            date_to_80 = last_date + timedelta(days=days_to_80)
        else:
            date_to_80 = None
    else:
        date_to_80 = None
    
    # Хэвлэх
    print(f"    Одоогийн SOH: {last_soh:.2f}% ({last_date.date()})")
    for i in range(future_cycles):
        print(f"      {future_dates[i].date()}: {future_soh[i]:.2f}%")
    if date_to_80:
        print(f"    ⚠️ Ашиглах боломжгүй болох: {date_to_80.date()} (≈{days_to_80:.0f} хоног)")
    
    return {
        'battery': batt_name,
        'current_soh': last_soh,
        'current_date': last_date,
        'future_dates': future_dates,
        'future_soh': future_soh,
        'eol_date': date_to_80
    }

def process_file(file_path, interval_days=2):
    print(f"\n--- {os.path.basename(file_path)} ---")
    df = pd.read_csv(file_path)
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df = df.dropna(subset=['start_time'])
    df = df.sort_values('start_time').reset_index(drop=True)
    
    valid_causes = ['BATTERY_VOLTAGE', 'BATTERY_CAPACITY', 'MAX_TIME']
    if 'stop_cause' in df.columns:
        df = df[df['stop_cause'].isin(valid_causes)].copy()
    
    if len(df) == 0:
        print("  Шүүлтийн дараа өгөгдөл хоосон")
        return []
    
    # Боломжит батерейнууд
    batt_indices = []
    for i in range(1,5):
        col = f'final_cap_rate{i}'
        if col in df.columns and (df[col] > 0).any():
            batt_indices.append(i)
    
    if not batt_indices:
        print("  Идэвхтэй батерей олдсонгүй")
        return []
    
    results = []
    for i in batt_indices:
        batt_name = f'String{i}'
        # Тухайн батерейны өгөгдлийг цуглуулах
        batt_df = df[df[f'final_cap_rate{i}'] > 0].copy()
        batt_df = batt_df.rename(columns={
            f'final_cap_rate{i}': 'final_cap_rate',
            f'init_cap_rate{i}': 'init_cap_rate'
        })
        # stop_cause, duration_min, start_time-г шилжүүлэх
        batt_df['stop_cause'] = df['stop_cause']
        batt_df['duration_min'] = df['duration_min']
        batt_df['start_time'] = df['start_time']
        
        res = analyze_battery(batt_df, batt_name, os.path.basename(file_path), interval_days)
        if res:
            res['site'] = os.path.basename(file_path).replace('.csv','')
            results.append(res)
    
    return results

def main(data_folder='../data', interval_days=2):
    if not os.path.exists(data_folder):
        print(f"Хавтас олдсонгүй: {data_folder}")
        return
    
    files = glob.glob(os.path.join(data_folder, '*.csv'))
    if not files:
        print(f"'{data_folder}' хавтаснаас .csv файл олдсонгүй")
        return
    
    all_results = []
    for f in files:
        try:
            res = process_file(f, interval_days)
            all_results.extend(res)
        except Exception as e:
            print(f"Алдаа {f}: {e}")
            import traceback
            traceback.print_exc()
    
    if all_results:
        df_out = pd.DataFrame([{
            'site': r['site'],
            'battery': r['battery'],
            'current_soh': round(r['current_soh'],2),
            'current_date': r['current_date'].strftime('%Y-%m-%d'),
            'eol_date': r['eol_date'].strftime('%Y-%m-%d') if r['eol_date'] else None
        } for r in all_results])
        df_out.to_csv('battery_analysis_with_change_detection.csv', index=False, encoding='utf-8-sig')
        print("\n✅ Хадгалагдсан: battery_analysis_with_change_detection.csv")
    else:
        print("Үр дүн байхгүй")

if __name__ == "__main__":
    main()