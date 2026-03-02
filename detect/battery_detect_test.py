import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')
import os
from datetime import datetime, timedelta

def process_battery_file(file_path, output_folder='results', measurement_interval_days=2):
    """
    Нэг CSV файл боловсруулж, статистик таамаглал хийнэ
    Батерейны тоог (String1, String2) автоматаар илрүүлнэ
    
    Parameters:
    file_path: CSV файлын зам
    output_folder: Үр дүнг хадгалах хавтас
    measurement_interval_days: Хэмжилтийн интервал (хоногоор)
    """
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    file_name = os.path.basename(file_path)
    site_name = file_name.replace('.csv', '')
    
    print(f"\n" + "="*60)
    print(f"Боловсруулж байна: {site_name}")
    print(f"Файл: {file_path}")
    print(f"Хэмжилтийн интервал: {measurement_interval_days} хоног")
    print("="*60)
    
    # CSV файлыг унших
    try:
        df = pd.read_csv(file_path)
        print(f"Анхны өгөгдлийн хэмжээ: {len(df)} мөр")
    except Exception as e:
        print(f"Алдаа: Файлыг уншиж чадсангүй - {e}")
        return None
    
    # Он сар өдрөөр эрэмбэлэх
    if 'start_time' in df.columns:
        # start_time-г Timestamp болгон хөрвүүлэх
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        
        # Хөрвүүлэлт амжилтгүй болсон мөрүүдийг хаях
        invalid_dates = df['start_time'].isna().sum()
        if invalid_dates > 0:
            print(f"Анхааруулга: {invalid_dates} мөр буруу огноотой - хасагдана")
            df = df.dropna(subset=['start_time'])
        
        df = df.sort_values('start_time').reset_index(drop=True)
        print("Өгөгдлийг огноогоор эрэмбэлсэн.")
        
        # Хэмжилтийн интервалыг шалгах
        if len(df) > 1:
            time_diffs = df['start_time'].diff().dt.total_seconds() / (24*3600)  # хоногоор
            avg_interval = time_diffs.mean()
            print(f"Дундаж хэмжилтийн интервал: {avg_interval:.1f} хоног")
            
            if abs(avg_interval - measurement_interval_days) > 1:
                print(f"Анхааруулга: Дундаж интервал ({avg_interval:.1f} хоног) нь тохиргооноос ({measurement_interval_days} хоног) ялгаатай байна.")
    else:
        print("Анхааруулга: 'start_time' багана олдсонгүй.")
        return None
    
    # Зөвхөн шаардлагатай stop_cause-уудыг шүүх
    valid_stop_causes = ['MAX_TIME', 'BATTERY_CAPACITY', 'BATTERY_VOLTAGE']
    if 'stop_cause' in df.columns:
        initial_count = len(df)
        df_filtered = df[df['stop_cause'].isin(valid_stop_causes)].copy()
        print(f"Зөвшөөрөгдсөн stop_cause: {valid_stop_causes}")
        print(f"Шүүлтийн дараа: {len(df_filtered)} мөр үлдсэн ({initial_count - len(df_filtered)} мөр хасагдсан)")
        
        if len(df_filtered) == 0:
            print("Шүүлтийн дараа ямар ч өгөгдөл үлдсэнгүй.")
            return None
        df = df_filtered
    else:
        print("Анхааруулга: 'stop_cause' багана олдсонгүй.")
        return None
    
    # Батерейны тоог илрүүлэх
    battery_strings = []
    battery_cols = []
    
    for i in range(1, 5):  # 1-4 хүртэл
        col_name = f'final_cap_rate{i}'
        if col_name in df.columns:
            # 0-ээс их утгатай мөрүүд байгаа эсэхийг шалгах
            non_zero = df[col_name].notna() & (df[col_name] > 0)
            if non_zero.any():
                battery_strings.append(f'String{i}')
                battery_cols.append(col_name)
    
    print(f"\nИлрүүлсэн батерейны тоо: {len(battery_strings)}")
    print(f"Батерейнууд: {battery_strings}")
    print(f"Баганууд: {battery_cols}")
    
    # Батерей бүрийн өгөгдлийг тусад нь хадгалах
    battery_data = {}
    
    for idx, (battery_name, col_name) in enumerate(zip(battery_strings, battery_cols)):
        volt_col = f'final_batt_volt{idx+1}'
        init_volt_col = f'init_batt_volt{idx+1}'
        
        battery_df = pd.DataFrame()
        battery_df['start_time'] = df['start_time']
        battery_df['soh'] = df[col_name].values
        
        # Хэрэв 0 утгатай бол NaN болгох (хэрэглэгдээгүй батерей)
        battery_df.loc[battery_df['soh'] == 0, 'soh'] = np.nan
        battery_df.loc[battery_df['soh'] > 100, 'soh'] = 100  # 100-аас ихгүй байх
        battery_df.loc[battery_df['soh'] < 0, 'soh'] = 0      # 0-ээс багагүй байх
        
        # NaN утгуудыг арилгах (интерполяци хийх)
        if battery_df['soh'].isna().any():
            # Интерполяци хийх (шугаман)
            battery_df['soh'] = battery_df['soh'].interpolate(method='linear', limit_direction='both')
            # Хэрэв интерполяци хийсний дараа ч NaN үлдсэн бол урд/хойд утгаар дүүргэх
            battery_df['soh'] = battery_df['soh'].fillna(method='ffill').fillna(method='bfill')
            print(f"{battery_name}: NaN утгуудыг интерполяциар дүүргэсэн.")
        
        battery_df['cycle_number'] = range(1, len(battery_df) + 1)
        
        # Хүчдэлийн мэдээлэл байвал нэмэх
        if volt_col in df.columns and init_volt_col in df.columns:
            battery_df['volt_drop'] = df[init_volt_col] - df[volt_col]
            # Хэвийн бус утгуудыг арилгах
            battery_df.loc[battery_df['volt_drop'] < 0, 'volt_drop'] = 0
            battery_df.loc[battery_df['volt_drop'] > 10, 'volt_drop'] = np.nan
            battery_df['volt_drop'] = battery_df['volt_drop'].interpolate(method='linear', limit_direction='both')
            battery_df['volt_drop'] = battery_df['volt_drop'].fillna(0)
        
        # Үргэлжлэх хугацаа байвал нэмэх
        if 'duration_min' in df.columns:
            battery_df['duration_min'] = df['duration_min'].values
            # Хэвийн бус утгуудыг арилгах
            battery_df.loc[battery_df['duration_min'] < 0, 'duration_min'] = 0
            battery_df.loc[battery_df['duration_min'] > 24*60, 'duration_min'] = 24*60  # 24 цагаас ихгүй
        
        battery_data[battery_name] = battery_df
        
        print(f"\n{battery_name}: {len(battery_df)} цикл, одоогийн SOH: {battery_df['soh'].iloc[-1]:.2f}%")
    
    # Батерей тус бүрээр дүн шинжилгээ хийх
    all_forecasts = {}
    
    # График зурах
    if len(battery_strings) > 0:
        fig, axes = plt.subplots(len(battery_strings), 3, figsize=(18, 5*len(battery_strings)))
        if len(battery_strings) == 1:
            axes = axes.reshape(1, -1)
    else:
        print("Анхааруулга: График зурах батерей байхгүй")
        fig, axes = None, None
    
    for b_idx, (battery_name, battery_df) in enumerate(battery_data.items()):
        print(f"\n{'-'*40}")
        print(f"БАТЕРЕЙ: {battery_name}")
        print(f"{'-'*40}")
        
        if len(battery_df) < 5:
            print(f"{battery_name}: Хангалттай өгөгдөл байхгүй (5-аас дээш цикл шаардлагатай)")
            all_forecasts[battery_name] = None
            continue
        
        # Үндсэн статистик
        print(f"Нийт цикл: {len(battery_df)}")
        print(f"Хугацааны хүрээ: {battery_df['start_time'].min()} - {battery_df['start_time'].max()}")
        print(f"Одоогийн SOH: {battery_df['soh'].iloc[-1]:.2f}%")
        print(f"Хамгийн өндөр SOH: {battery_df['soh'].max():.2f}%")
        print(f"Хамгийн бага SOH: {battery_df['soh'].min():.2f}%")
        print(f"Дундаж SOH: {battery_df['soh'].mean():.2f}%")
        
        # SOH-ийн өөрчлөлтийн хурд
        battery_df['soh_change'] = battery_df['soh'].diff()
        
        # Сүүлийн үеийн бууралтын хурд
        recent_cycles = min(10, len(battery_df) // 3)
        recent_changes = battery_df['soh_change'].iloc[-recent_cycles:].dropna()
        
        if len(recent_changes) > 0:
            recent_avg_degradation = recent_changes.mean()
            print(f"Сүүлийн {recent_cycles} циклийн дундаж SOH бууралт: {recent_avg_degradation:.4f}%")
        
        # График 1: SOH
        if axes is not None:
            axes[b_idx, 0].plot(battery_df['cycle_number'], battery_df['soh'], 'b-', linewidth=1.5, label='SOH')
            axes[b_idx, 0].set_xlabel('Циклийн дугаар')
            axes[b_idx, 0].set_ylabel('SOH (%)')
            axes[b_idx, 0].set_title(f'{battery_name} - SOH')
            axes[b_idx, 0].grid(True, alpha=0.3)
            axes[b_idx, 0].axhline(y=80, color='r', linestyle='--', alpha=0.7, label='80% босго')
            axes[b_idx, 0].legend()
            
            # График 2: DoD (Depth of Discharge)
            battery_df['dod'] = 100 - battery_df['soh']
            axes[b_idx, 1].plot(battery_df['cycle_number'], battery_df['dod'], 'g-', linewidth=1.5, label='DoD')
            axes[b_idx, 1].set_xlabel('Циклийн дугаар')
            axes[b_idx, 1].set_ylabel('DoD (%)')
            axes[b_idx, 1].set_title(f'{battery_name} - Цэнэглэлтийн гүн')
            axes[b_idx, 1].grid(True, alpha=0.3)
            axes[b_idx, 1].legend()
            
            # График 3: Хүчдэлийн уналт (хэрэв байгаа бол)
            if 'volt_drop' in battery_df.columns:
                axes[b_idx, 2].plot(battery_df['cycle_number'], battery_df['volt_drop'], 'm-', linewidth=1.5, label='Voltage Drop')
                axes[b_idx, 2].set_xlabel('Циклийн дугаар')
                axes[b_idx, 2].set_ylabel('Хүчдэлийн уналт (V)')
                axes[b_idx, 2].set_title(f'{battery_name} - Хүчдэлийн уналт')
                axes[b_idx, 2].grid(True, alpha=0.3)
                axes[b_idx, 2].legend()
            else:
                axes[b_idx, 2].text(0.5, 0.5, 'Өгөгдөл байхгүй', 
                                   horizontalalignment='center', verticalalignment='center', transform=axes[b_idx, 2].transAxes)
                axes[b_idx, 2].set_title(f'{battery_name} - Хүчдэлийн уналт')
        
        # Богино хугацааны таамаглал
        if len(battery_df) >= 10:
            # Сүүлийн 10 цикл
            last_n = min(10, len(battery_df))
            last_cycles = battery_df.tail(last_n).copy()
            last_cycle_num = battery_df['cycle_number'].iloc[-1]
            last_date = battery_df['start_time'].iloc[-1]
            
            # Дараагийн 10 циклийн таамаглал (20 хоног)
            future_cycles_count = 10
            future_cycles_list = list(range(last_cycle_num + 1, last_cycle_num + future_cycles_count + 1))
            
            # Ирээдүйн огноонууд (2 хоног тутам)
            future_dates = [last_date + timedelta(days=(i+1)*measurement_interval_days) 
                           for i in range(future_cycles_count)]
            
            # --- Арга 1: Орон нутгийн шугаман регресс ---
            local_n = min(8, len(last_cycles))
            X_local = last_cycles['cycle_number'].iloc[-local_n:].values.reshape(-1, 1)
            y_local = last_cycles['soh'].iloc[-local_n:].values
            
            if len(X_local) >= 3:
                local_lr = LinearRegression()
                local_lr.fit(X_local, y_local)
                
                future_X_local = np.array(future_cycles_list).reshape(-1, 1)
                local_forecast = local_lr.predict(future_X_local)
                local_forecast = np.clip(local_forecast, 0, 100)
            else:
                local_forecast = np.full(future_cycles_count, battery_df['soh'].iloc[-1])
                local_lr = None
            
            # --- Арга 2: Экспоненциал жигдрүүлэлт ---
            try:
                if len(battery_df) >= 15:
                    hw_model = ExponentialSmoothing(
                        battery_df['soh'].values[-20:], 
                        trend='add', 
                        seasonal=None,
                        initialization_method='estimated'
                    ).fit()
                    hw_forecast = hw_model.forecast(future_cycles_count)
                    hw_forecast = np.clip(hw_forecast, 0, 100)
                else:
                    hw_forecast = np.full(future_cycles_count, battery_df['soh'].iloc[-1])
            except:
                hw_forecast = np.full(future_cycles_count, battery_df['soh'].iloc[-1])
            
            # Таамаглалын үр дүнг харуулах
            print(f"\n{'='*40}")
            print(f"{battery_name} - БОГИНО ХУГАЦААНЫ ТААМАГЛАЛ (дараагийн {future_cycles_count} цикл)")
            print(f"{'='*40}")
            print(f"{'Цикл':<8} {'Огноо':<12} {'Орон нут. рег':<15} {'Holt-Winters':<15}")
            print("-"*55)
            
            forecast_data = []
            for i, (cycle, date) in enumerate(zip(future_cycles_list, future_dates)):
                local_val = round(local_forecast[i], 2)
                hw_val = round(hw_forecast[i], 2)
                date_str = date.strftime('%Y-%m-%d')
                print(f"{cycle:<8} {date_str:<12} {local_val:<15} {hw_val:<15}")
                
                forecast_data.append({
                    'cycle': cycle,
                    'date': date,
                    'date_str': date_str,
                    'local_regression': local_val,
                    'holt_winters': hw_val
                })
            
            # 80% босгод хүрэх хугацаа
            print(f"\n{'='*40}")
            print(f"80% БОСГОД ХҮРЭХ ХУГАЦАА")
            print(f"{'='*40}")
            
            current_soh = battery_df['soh'].iloc[-1]
            
            if current_soh <= 80:
                print(f"АНХААРУУЛГА: {battery_name} батерей 80%-иас доош буурсан байна!")
                print(f"Одоогийн SOH: {current_soh:.2f}%")
            else:
                # Орон нутгийн регрессээр
                if local_lr is not None and hasattr(local_lr, 'coef_') and local_lr.coef_[0] < 0:
                    cycles_to_80 = (80 - current_soh) / local_lr.coef_[0]
                    if cycles_to_80 > 0:
                        days_to_80 = cycles_to_80 * measurement_interval_days
                        target_date = last_date + timedelta(days=days_to_80)
                        print(f"Орон нутгийн регрессээр:")
                        print(f"  {abs(cycles_to_80):.1f} циклийн дараа")
                        print(f"  {days_to_80:.1f} хоногийн дараа")
                        print(f"  Таамагласан огноо: {target_date.strftime('%Y-%m-%d')}")
                
                # Сүүлийн үеийн бууралтын хурдаар
                if len(recent_changes) > 0 and recent_avg_degradation < 0:
                    cycles_to_80_recent = (80 - current_soh) / recent_avg_degradation
                    if cycles_to_80_recent > 0:
                        days_to_80_recent = cycles_to_80_recent * measurement_interval_days
                        target_date_recent = last_date + timedelta(days=days_to_80_recent)
                        print(f"\nСүүлийн үеийн бууралтын хурдаар:")
                        print(f"  {abs(cycles_to_80_recent):.1f} циклийн дараа")
                        print(f"  {days_to_80_recent:.1f} хоногийн дараа")
                        print(f"  Таамагласан огноо: {target_date_recent.strftime('%Y-%m-%d')}")
            
            # Таамаглалын үр дүнг хадгалах
            all_forecasts[battery_name] = {
                'battery_name': battery_name,
                'current_soh': current_soh,
                'last_cycle': last_cycle_num,
                'last_date': last_date,
                'forecasts': forecast_data,
                'local_model': local_lr,
                'recent_degradation': recent_avg_degradation if len(recent_changes) > 0 else None
            }
    
    if fig is not None:
        plt.tight_layout()
        
        # Графикийг хадгалах
        plot_file = os.path.join(output_folder, f"{site_name}_battery_analysis.png")
        plt.savefig(plot_file, dpi=100, bbox_inches='tight')
        print(f"\nГрафик хадгалагдсан: {plot_file}")
        plt.show()
    
    # Анхны өгөгдөлд таамагласан утгуудыг нэмэх
    print(f"\n{'='*60}")
    print(f"ТААМАГЛАСАН УТГУУДЫГ CSV ФАЙЛД НЭМЖ БАЙНА")
    print(f"{'='*60}")
    
    # Анхны өгөгдлийг хуулах
    df_result = df.copy()
    df_result['cycle_number'] = range(1, len(df_result) + 1)
    df_result['is_forecast'] = False
    
    # Батерей тус бүрийн таамагласан утгуудыг нэмэх
    for battery_name, forecast_info in all_forecasts.items():
        if forecast_info is None:
            continue
        
        # Батерейны индекс (1-based)
        battery_idx = int(battery_name.replace('String', ''))
        forecast_col = f'forecast_cap_rate{battery_idx}'
        forecast_date_col = f'forecast_date{battery_idx}'
        
        # Эхлээд хоосон утгатай багана үүсгэх
        df_result[forecast_col] = np.nan
        df_result[forecast_date_col] = None
        
        # Таамагласан утгуудыг нэмэх (шинэ мөрүүдэд)
        last_cycle = len(df_result)
        
        for i, fc in enumerate(forecast_info['forecasts']):
            cycle_num = last_cycle + i + 1
            
            # Шинэ мөр үүсгэх
            new_row = {col: np.nan for col in df_result.columns}
            new_row['id'] = f"FC_{cycle_num}"
            new_row['IP'] = df_result['IP'].iloc[0] if 'IP' in df_result.columns else None
            new_row['site_location'] = df_result['site_location'].iloc[0] if 'site_location' in df_result.columns else None
            new_row['start_time'] = fc['date']  # Энэ нь Timestamp байна
            new_row['cycle_number'] = cycle_num
            new_row[forecast_col] = fc['local_regression']
            new_row[forecast_date_col] = fc['date_str']
            new_row['is_forecast'] = True
            new_row['batt_type'] = 'FORECAST'
            new_row['stop_cause'] = 'FORECAST'
            
            # Бусад багануудыг хоосон орхих
            df_result = pd.concat([df_result, pd.DataFrame([new_row])], ignore_index=True)
        
        print(f"{battery_name}: {len(forecast_info['forecasts'])} таамаглал нэмэгдлээ")
    
    # Хугацаагаар дахин эрэмбэлэх (одоо бүх start_time ижил төрөлтэй)
    df_result = df_result.sort_values('start_time').reset_index(drop=True)
    
    # Үр дүнг CSV файлд хадгалах
    result_file = os.path.join(output_folder, f"{site_name}_with_forecasts.csv")
    df_result.to_csv(result_file, index=False, encoding='utf-8-sig')
    print(f"\nТаамаглал нэмсэн файл хадгалагдсан: {result_file}")
    
    # Товч тайлан хадгалах
    summary_file = os.path.join(output_folder, f"{site_name}_summary.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"Сайт: {site_name}\n")
        f.write(f"Файл: {file_path}\n")
        f.write(f"Хэмжилтийн интервал: {measurement_interval_days} хоног\n")
        f.write(f"Боловсруулсан огноо: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*60 + "\n\n")
        
        f.write(f"Илрүүлсэн батерейнууд: {battery_strings}\n\n")
        
        for battery_name, forecast_info in all_forecasts.items():
            if forecast_info is None:
                continue
            
            f.write(f"{'='*40}\n")
            f.write(f"БАТЕРЕЙ: {battery_name}\n")
            f.write(f"{'='*40}\n")
            f.write(f"Одоогийн SOH: {forecast_info['current_soh']:.2f}%\n")
            f.write(f"Сүүлийн цикл: {forecast_info['last_cycle']}\n")
            f.write(f"Сүүлийн огноо: {forecast_info['last_date'].strftime('%Y-%m-%d')}\n")
            
            if forecast_info['recent_degradation'] is not None:
                f.write(f"Сүүлийн үеийн бууралтын хурд: {forecast_info['recent_degradation']:.4f}%/цикл\n")
                daily_degradation = forecast_info['recent_degradation'] / measurement_interval_days
                f.write(f"Өдрийн бууралтын хурд: {daily_degradation:.4f}%/хоног\n")
            
            f.write(f"\nДараагийн 5 циклийн таамаглал ({measurement_interval_days} хоног тутам):\n")
            f.write(f"{'Цикл':<8} {'Огноо':<12} {'SOH (%)':<10}\n")
            f.write("-"*35 + "\n")
            
            for i, fc in enumerate(forecast_info['forecasts'][:5]):
                f.write(f"{fc['cycle']:<8} {fc['date_str']:<12} {fc['local_regression']:<10.2f}\n")
            
            # 80% босгод хүрэх хугацаа
            current_soh = forecast_info['current_soh']
            if current_soh > 80 and forecast_info['recent_degradation'] is not None and forecast_info['recent_degradation'] < 0:
                cycles_to_80 = (80 - current_soh) / forecast_info['recent_degradation']
                if cycles_to_80 > 0:
                    days_to_80 = cycles_to_80 * measurement_interval_days
                    target_date = forecast_info['last_date'] + timedelta(days=days_to_80)
                    f.write(f"\n80% босгод хүрэх таамаглал:\n")
                    f.write(f"  {abs(cycles_to_80):.1f} циклийн дараа\n")
                    f.write(f"  {days_to_80:.1f} хоногийн дараа\n")
                    f.write(f"  {target_date.strftime('%Y-%m-%d')}\n")
            
            f.write("\n")
    
    print(f"Товч тайлан хадгалагдсан: {summary_file}")
    
    return df_result

def process_multiple_files(file_paths, output_folder='results', measurement_interval_days=2):
    """
    Олон CSV файлуудыг боловсруулах
    """
    results = {}
    all_summaries = []
    
    for file_path in file_paths:
        try:
            df_result = process_battery_file(file_path, output_folder, measurement_interval_days)
            if df_result is not None:
                file_name = os.path.basename(file_path).replace('.csv', '')
                results[file_name] = df_result
                
                # Батерей тус бүрийн хураангуй мэдээлэл цуглуулах
                site = file_name
                
                # forecast багануудаас мэдээлэл авах
                for col in df_result.columns:
                    if col.startswith('forecast_cap_rate'):
                        battery_idx = col.replace('forecast_cap_rate', '')
                        if battery_idx.isdigit():
                            battery_name = f"String{battery_idx}"
                            
                            # Сүүлийн таамагласан утга
                            forecast_rows = df_result[df_result['is_forecast'] == True]
                            if not forecast_rows.empty and col in forecast_rows.columns:
                                last_forecast = forecast_rows[col].iloc[-1] if not forecast_rows[col].isna().all() else None
                                
                                # Одоогийн SOH
                                current_soh_col = f'final_cap_rate{battery_idx}'
                                actual_rows = df_result[df_result['is_forecast'] == False]
                                current_soh = actual_rows[current_soh_col].iloc[-1] if not actual_rows.empty and current_soh_col in actual_rows.columns else None
                                
                                if last_forecast is not None and current_soh is not None:
                                    all_summaries.append({
                                        'site': site,
                                        'battery': battery_name,
                                        'current_soh': round(current_soh, 2),
                                        'forecast_soh_5cycles': round(last_forecast, 2),
                                        'forecast_date': forecast_rows[f'forecast_date{battery_idx}'].iloc[-1] if f'forecast_date{battery_idx}' in forecast_rows.columns else None,
                                        'analysis_date': datetime.now().strftime('%Y-%m-%d')
                                    })
        except Exception as e:
            print(f"Алдаа гарлаа: {file_path} - {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*60)
    print("БҮХ ФАЙЛ БОЛОВСРУУЛАЛТ ДУУСЛАА")
    print("="*60)
    print(f"Амжилттай боловсруулсан: {len(results)}/{len(file_paths)} файл")
    
    # Бүх үр дүнг нэгтгэсэн файл
    if all_summaries:
        combined_file = os.path.join(output_folder, "all_batteries_summary.csv")
        summary_df = pd.DataFrame(all_summaries)
        summary_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
        print(f"Бүх батерейны хураангуй тайлан: {combined_file}")
        
        # Маркдаун форматтай тайлан
        md_file = os.path.join(output_folder, "summary_report.md")
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# Батерейны Төлөв Болон Таамаглалын Тайлан\n\n")
            f.write(f"Тайлан гаргасан огноо: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            
            for site in summary_df['site'].unique():
                f.write(f"## {site}\n\n")
                site_data = summary_df[summary_df['site'] == site]
                
                for _, row in site_data.iterrows():
                    f.write(f"### {row['battery']}\n")
                    f.write(f"- Одоогийн SOH: {row['current_soh']:.2f}%\n")
                    f.write(f"- 5 циклийн дараах SOH: {row['forecast_soh_5cycles']:.2f}%\n")
                    f.write(f"- Таамагласан огноо: {row['forecast_date']}\n\n")
                
                f.write("---\n\n")
    
    return results
 
# Хэрэв скриптийг шууд ажиллуулбал
if __name__ == "__main__":
    # Тохиргоо
    MEASUREMENT_INTERVAL_DAYS = 2  # 2 хоног тутамд хэмждэг
    
    # Файлуудын жагсаалт
    file_list = ['10.63.43.90.csv', '10.63.43.114.csv']
    
    # Файлууд байгаа эсэхийг шалгах
    for i, file_path in enumerate(file_list):
        if not os.path.exists(file_path):
            print(f"АНХААРУУЛГА: {file_path} файл олдсонгүй!")
            # Хэрэв файл байхгүй бол өөр газраас хайх
            possible_paths = [
                f'../data/{file_path}',
                f'./data/{file_path}',
                f'./{file_path}'
            ]
            found = False
            for p in possible_paths:
                if os.path.exists(p):
                    file_list[i] = p
                    print(f"Олдсон: {p}")
                    found = True
                    break
            if not found:
                print(f"Файл олдсонгүй: {file_path}")
    
    # Боловсруулах
    results = process_multiple_files(file_list, measurement_interval_days=MEASUREMENT_INTERVAL_DAYS)
    
    print("\n" + "="*60)
    print("ПРОГРАМ АМЖИЛТТАЙ ДУУСЛАА")
    print("="*60)
    print(f"Үр дүнгийн хавтас: results/")
    print("Файлууд:")
    print("  - {site}_with_forecasts.csv - Таамаглал нэмсэн өгөгдөл")
    print("  - {site}_summary.txt - Товч тайлан")
    print("  - {site}_battery_analysis.png - Графикууд")
    print("  - all_batteries_summary.csv - Бүх батерейны хураангуй")
    print("  - summary_report.md - Маркдаун форматтай тайлан")