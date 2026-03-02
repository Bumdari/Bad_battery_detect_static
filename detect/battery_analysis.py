import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

# ========================= ФУНКЦУУД =========================

def estimate_soh_and_forecast(df_batt, battery_name, site_name, interval_days=2, output_folder='results'):
    """
    Нэг батерейны өгөгдлөөр SOH тооцоолж, шугаман регрессээр таамаглал хийнэ
    """
    if len(df_batt) < 3:
        print(f" {battery_name}: Хэт цөөн өгөгдөл, алгасах")
        return None
    
    df = df_batt.copy()
    df = df.sort_values('start_time').reset_index(drop=True)
    df['cycle_number'] = range(1, len(df)+1)
    
    # init_cap_rate байгаа эсэх, ихэвчлэн 100 орчим
    if 'init_cap_rate1' in df.columns:
        init_col = 'init_cap_rate1'
    else:
        # байхгүй бол бүгдийг 100 гэж үзье
        df['init_cap_rate'] = 100.0
        init_col = 'init_cap_rate'
    
    # Алдагдсан хувь (0-с их байлгах)
    lost = df[init_col] - df['final_cap_rate']
    lost = lost.clip(lower=0.1)   # 0-д хуваахаас сэргийлэх
    
    # Capacity ratio: хугацаа / алдагдсан хувь
    df['capacity_ratio'] = df['duration_min'] / lost
    
    # Жишиг харьцааг MAX_TIME бичлэгүүдээс авах
    healthy = df[df['stop_cause'] == 'MAX_TIME']
    if len(healthy) >= 3:
        reference_ratio = healthy['capacity_ratio'].mean()
    else:
        # Хамгийн сайн 10% -ийн дундаж
        top_thresh = df['capacity_ratio'].quantile(0.9)
        reference_ratio = df[df['capacity_ratio'] >= top_thresh]['capacity_ratio'].mean()
    
    if pd.isna(reference_ratio) or reference_ratio <= 0:
        # Бүр боломжгүй бол бүх өгөгдлийн дундаж
        reference_ratio = df['capacity_ratio'].mean()
    
    # SOH тооцоолох
    df['soh'] = (df['capacity_ratio'] / reference_ratio) * 100
    df['soh'] = df['soh'].clip(0, 100)
    
    # --- Шугаман регресс (х цикл, у SOH) ---
    X = df['cycle_number'].values.reshape(-1, 1)
    y = df['soh'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    # Одоогийн мэдээлэл
    last_cycle = df['cycle_number'].iloc[-1]
    last_date = df['start_time'].iloc[-1]
    last_soh = df['soh'].iloc[-1]
    
    # Дараагийн 10 өдөр (5 цикл, 2 хоног тутам)
    future_cycles = 5
    future_cycle_nums = np.array([last_cycle + i + 1 for i in range(future_cycles)]).reshape(-1, 1)
    future_soh = model.predict(future_cycle_nums)
    future_soh = np.clip(future_soh, 0, 100)
    
    future_dates = [last_date + timedelta(days=interval_days*(i+1)) for i in range(future_cycles)]
    
    # 80% босгод хүрэх хугацаа
    slope = model.coef_[0]
    if slope < 0:  # буурч байгаа тохиолдолд
        cycles_to_80 = (80 - last_soh) / slope
        if cycles_to_80 > 0:
            days_to_80 = cycles_to_80 * interval_days
            date_to_80 = last_date + timedelta(days=days_to_80)
        else:
            cycles_to_80 = days_to_80 = date_to_80 = None
    else:
        cycles_to_80 = days_to_80 = date_to_80 = None
    
    # Хэвлэх
    print(f"\n  {battery_name}:")
    print(f"    Одоогийн SOH: {last_soh:.2f}% ({last_date.strftime('%Y-%m-%d')})")
    print(f"    Таамаглал (5 цикл):")
    for i in range(future_cycles):
        print(f"      {future_dates[i].strftime('%Y-%m-%d')}: {future_soh[i]:.2f}%")
    if date_to_80:
        print(f"    ⚠️  Ашиглах боломжгүй болох огноо (80%): {date_to_80.strftime('%Y-%m-%d')} (≈{days_to_80:.0f} хоног)")
    else:
        print(f"    ✅ 80% -д хүрэхгүй (эсвэл удахгүй)")
    
    # График зурах
    plt.figure(figsize=(10,5))
    plt.plot(df['start_time'], df['soh'], 'b-o', markersize=3, label='SOH (тооцоолсон)')
    # Таамаглал
    plt.plot(future_dates, future_soh, 'r--s', markersize=4, label='Таамаглал (LR)')
    plt.axhline(y=80, color='gray', linestyle='--', label='80% босго')
    plt.xlabel('Огноо')
    plt.ylabel('SOH (%)')
    plt.title(f'{site_name} - {battery_name} SOH таамаглал')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    graph_file = os.path.join(output_folder, f"{site_name}_{battery_name}_forecast.png")
    plt.savefig(graph_file, dpi=100)
    plt.close()
    print(f"    График хадгалагдсан: {graph_file}")
    
    # Үр дүнг буцаах
    result = {
        'battery': battery_name,
        'current_soh': last_soh,
        'current_date': last_date,
        'future_dates': future_dates,
        'future_soh': future_soh,
        'eol_date': date_to_80,
        'eol_days': days_to_80 if date_to_80 else None,
        'slope': slope,
        'reference_ratio': reference_ratio
    }
    return result


def process_single_file(file_path, interval_days=2, output_folder='results'):
    """
    Нэг CSV файлыг боловсруулах
    """
    print(f"\n{'='*60}")
    print(f"Файл: {file_path}")
    print('='*60)
    
    # Файл унших
    df = pd.read_csv(file_path)
    print(f"Анхны мөр: {len(df)}")
    
    # start_time -> datetime
    if 'start_time' not in df.columns:
        print("start_time багана байхгүй, алгасах")
        return None
    df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    df = df.dropna(subset=['start_time'])
    
    # Огноогоор эрэмбэлэх
    df = df.sort_values('start_time').reset_index(drop=True)
    
    # stop_cause-аар шүүх
    valid_causes = ['BATTERY_VOLTAGE', 'BATTERY_CAPACITY', 'MAX_TIME']
    if 'stop_cause' in df.columns:
        df = df[df['stop_cause'].isin(valid_causes)].copy()
        print(f"Шүүлтийн дараа: {len(df)} мөр")
    else:
        print("stop_cause багана байхгүй, бүх өгөгдлийг ашиглана")
    
    if len(df) == 0:
        print("Өгөгдөл хоосон")
        return None
    
    # Боломжит батерейнуудыг илрүүлэх (final_cap_rate1,2,...)
    batt_indices = []
    for i in range(1,5):
        col = f'final_cap_rate{i}'
        if col in df.columns and (df[col] > 0).any():
            batt_indices.append(i)
    
    print(f"Идэвхтэй батерейнууд: {batt_indices}")
    
    site_name = os.path.basename(file_path).replace('.csv', '')
    
    # Батерей тус бүрээр шинжилгээ
    results = []
    for idx in batt_indices:
        batt_name = f'String{idx}'
        cap_col = f'final_cap_rate{idx}'
        init_col = f'init_cap_rate{idx}'
        # Тухайн батерейны өгөгдлийг цуглуулах (final_cap_rate != 0)
        batt_df = df[df[cap_col] > 0].copy()
        if len(batt_df) == 0:
            continue
        
        # duration_min байгаа эсэх
        if 'duration_min' not in batt_df.columns:
            print(f"{batt_name}: duration_min багана байхгүй")
            continue
        
        batt_df = batt_df[['start_time', 'stop_cause', 'duration_min', cap_col, init_col]].rename(
            columns={cap_col: 'final_cap_rate', init_col: 'init_cap_rate'})
        
        res = estimate_soh_and_forecast(batt_df, batt_name, site_name, interval_days, output_folder)
        if res:
            res['site'] = site_name
            results.append(res)
    
    # Бүх батерейны үр дүнг хадгалах
    if results:
        summary_df = pd.DataFrame([{
            'site': r['site'],
            'battery': r['battery'],
            'current_soh': r['current_soh'],
            'current_date': r['current_date'].strftime('%Y-%m-%d'),
            'eol_date': r['eol_date'].strftime('%Y-%m-%d') if r['eol_date'] else None,
            'eol_days': r['eol_days'],
            'slope': r['slope']
        } for r in results])
        
        summary_file = os.path.join(output_folder, f"{site_name}_summary.csv")
        summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
        print(f"\nТайлан хадгалагдсан: {summary_file}")
    
    return results


def process_all_files(data_folder='data', interval_days=2, output_folder='results'):
    """
    data хавтас дахь бүх csv файлыг боловсруулах
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    csv_files = glob.glob(os.path.join(data_folder, '*.csv'))
    if not csv_files:
        print(f"'{data_folder}' хавтаснаас .csv файл олдсонгүй")
        return
    
    all_results = []
    for file in csv_files:
        try:
            res = process_single_file(file, interval_days, output_folder)
            if res:
                all_results.extend(res)
        except Exception as e:
            print(f"Алдаа {file}: {e}")
            import traceback
            traceback.print_exc()
    
    # Бүх файлын нэгдсэн дүн
    if all_results:
        master_df = pd.DataFrame([{
            'site': r['site'],
            'battery': r['battery'],
            'current_soh': r['current_soh'],
            'current_date': r['current_date'].strftime('%Y-%m-%d'),
            'eol_date': r['eol_date'].strftime('%Y-%m-%d') if r['eol_date'] else None,
            'eol_days': r['eol_days']
        } for r in all_results])
        master_file = os.path.join(output_folder, "all_batteries_forecast.csv")
        master_df.to_csv(master_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ Бүх батерейны нэгдсэн тайлан: {master_file}")
    
    print(f"\n{'='*60}\nБүх файл боловсруулагдсан. Үр дүн '{output_folder}' хавтасст хадгалагдлаа.\n{'='*60}")


# ========================= ҮНДСЭН ХЭСЭГ =========================
if __name__ == "__main__":
    # Тохиргоо
    DATA_FOLDER = "../data"            # csv файлууд байрлах хавтас
    INTERVAL_DAYS = 2               # 2 хоног тутамд хэмжилт
    OUTPUT_FOLDER = "results"       # гаргах хавтас
    
    process_all_files(DATA_FOLDER, INTERVAL_DAYS, OUTPUT_FOLDER)