import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from statsmodels.tsa.ar_model import AutoReg
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')
import os

def process_battery_file(file_path, output_folder='results'):
    """
    Нэг CSV файл боловсруулж, статистик таамаглал хийнэ
    
    Parameters:
    file_path: CSV файлын зам
    output_folder: Үр дүнг хадгалах хавтас
    """
    
    # Хавтас байхгүй бол үүсгэх
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Файлын нэрээс сайтын нэрийг гаргаж авах
    file_name = os.path.basename(file_path)
    site_name = file_name.replace('.csv', '')
    
    print(f"\n" + "="*60)
    print(f"Боловсруулж байна: {site_name}")
    print(f"Файл: {file_path}")
    print("="*60)
    
    # 1. CSV файлыг унших
    try:
        df = pd.read_csv(file_path)
        print(f"Анхны өгөгдлийн хэмжээ: {len(df)} мөр")
    except Exception as e:
        print(f"Алдаа: Файлыг уншиж чадсангүй - {e}")
        return None
    
    # 2. Он сар өдрөөр эрэмбэлэх (start_time баганаар)
    if 'start_time' in df.columns:
        df['start_time'] = pd.to_datetime(df['start_time'])
        df = df.sort_values('start_time').reset_index(drop=True)
        print("Өгөгдлийг огноогоор эрэмбэлсэн.")
    else:
        print("Анхааруулга: 'start_time' багана олдсонгүй. index-ээр эрэмбэлнэ.")
    
    # 3. Зөвхөн шаардлагатай stop_cause-уудыг шүүх
    valid_stop_causes = ['MAX_TIME', 'BATTERY_CAPACITY', 'BATTERY_VOLTAGE']
    if 'stop_cause' in df.columns:
        initial_count = len(df)
        df_filtered = df[df['stop_cause'].isin(valid_stop_causes)].copy()
        print(f"Зөвшөөрөгдсөн stop_cause: {valid_stop_causes}")
        print(f"Шүүлтийн дараа: {len(df_filtered)} мөр үлдсэн ({initial_count - len(df_filtered)} мөр хасагдсан)")
        
        if len(df_filtered) == 0:
            print("Анхааруулга: Шүүлтийн дараа ямар ч өгөгдөл үлдсэнгүй.")
            return None
        df = df_filtered
    else:
        print("Анхааруулга: 'stop_cause' багана олдсонгүй. Бүх өгөгдлийг ашиглана.")
    
    # 4. Шинэ баганууд нэмэх
    # Циклийн дугаар
    df['cycle_number'] = range(1, len(df) + 1)
    
    # Цэнэглэлтийн гүн (Depth of Discharge - DoD)
    if 'final_cap_rate1' in df.columns:
        df['dod'] = 100 - df['final_cap_rate1']
        print("'dod' (цэнэглэлтийн гүн) багана нэмсэн.")
    else:
        print("Анхааруулга: 'final_cap_rate1' багана олдсонгүй.")
        return None
    
    # Хэрэв final_batt_volt1 байвал хүчдэлийн уналтыг тооцох
    if 'init_batt_volt1' in df.columns and 'final_batt_volt1' in df.columns:
        df['volt_drop'] = df['init_batt_volt1'] - df['final_batt_volt1']
        print("'volt_drop' (хүчдэлийн уналт) багана нэмсэн.")
    
    # 5. Үндсэн статистик мэдээлэл харуулах
    print("\n" + "-"*40)
    print("ҮНДСЭН СТАТИСТИК МЭДЭЭЛЭЛ")
    print("-"*40)
    print(f"Нийт цэнэглэлтийн цикл: {len(df)}")
    print(f"Хугацааны хүрээ: {df['start_time'].min()} - {df['start_time'].max()}")
    print(f"Одоогийн SOH (final_cap_rate1): {df['final_cap_rate1'].iloc[-1]:.2f}%")
    print(f"Хамгийн өндөр SOH: {df['final_cap_rate1'].max():.2f}%")
    print(f"Хамгийн бага SOH: {df['final_cap_rate1'].min():.2f}%")
    print(f"Дундаж SOH: {df['final_cap_rate1'].mean():.2f}%")
    
    if 'duration_min' in df.columns:
        print(f"Дундаж цэнэглэлтийн хугацаа: {df['duration_min'].mean():.1f} мин")
    
    # 6. SOH-ийн өөрчлөлтийн хурд тооцох
    df['soh_change'] = df['final_cap_rate1'].diff()
    avg_degradation_rate = df['soh_change'].mean()
    print(f"Цикл бүрийн дундаж SOH бууралт: {avg_degradation_rate:.3f}%")
    
    # 7. График зурах
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # График 1: SOH цаг хугацааны явцад
    axes[0, 0].plot(df['cycle_number'], df['final_cap_rate1'], 'b-', marker='.', linewidth=1)
    axes[0, 0].set_xlabel('Циклийн дугаар')
    axes[0, 0].set_ylabel('SOH (%)')
    axes[0, 0].set_title('SOH-ийн өөрчлөлт')
    axes[0, 0].grid(True, alpha=0.3)
    
    # График 2: DoD цаг хугацааны явцад
    axes[0, 1].plot(df['cycle_number'], df['dod'], 'r-', marker='.', linewidth=1)
    axes[0, 1].set_xlabel('Циклийн дугаар')
    axes[0, 1].set_ylabel('DoD (%)')
    axes[0, 1].set_title('Цэнэглэлтийн гүн (DoD)')
    axes[0, 1].grid(True, alpha=0.3)
    
    # График 3: Хэрэв хүчдэлийн уналт байвал
    if 'volt_drop' in df.columns:
        axes[1, 0].plot(df['cycle_number'], df['volt_drop'], 'g-', marker='.', linewidth=1)
        axes[1, 0].set_xlabel('Циклийн дугаар')
        axes[1, 0].set_ylabel('Хүчдэлийн уналт (V)')
        axes[1, 0].set_title('Хүчдэлийн уналт (init - final)')
        axes[1, 0].grid(True, alpha=0.3)
    else:
        # Хэрэв байхгүй бол үргэлжлэх хугацааны график
        if 'duration_min' in df.columns:
            axes[1, 0].plot(df['cycle_number'], df['duration_min'], 'm-', marker='.', linewidth=1)
            axes[1, 0].set_xlabel('Циклийн дугаар')
            axes[1, 0].set_ylabel('Хугацаа (мин)')
            axes[1, 0].set_title('Цэнэглэлтийн үргэлжлэх хугацаа')
            axes[1, 0].grid(True, alpha=0.3)
    
    # График 4: SOH болон таамаглалууд
    axes[1, 1].plot(df['cycle_number'], df['final_cap_rate1'], 'b-', linewidth=2, label='Бодит SOH')
    axes[1, 1].set_xlabel('Циклийн дугаар')
    axes[1, 1].set_ylabel('SOH (%)')
    axes[1, 1].set_title('Богино хугацааны таамаглал')
    axes[1, 1].grid(True, alpha=0.3)
    axes[1, 1].axhline(y=80, color='r', linestyle='--', alpha=0.7, label='80% босго (EoL)')
    
    # 8. Богино хугацааны таамаглал (Статистик аргууд)
    if len(df) >= 5:
        # Сүүлийн 5 циклийн өгөгдлийг ашиглах
        last_cycles = df.tail(10).copy()
        last_cycles = last_cycles.reset_index(drop=True)
        last_cycle_num = df['cycle_number'].iloc[-1]
        
        # Дараагийн 5 циклийн таамаглал
        future_cycles = list(range(last_cycle_num + 1, last_cycle_num + 6))
        future_cycles_idx = list(range(len(last_cycles), len(last_cycles) + 5))
        
        # --- Арга 1: Хандлагын шугам (Linear Regression) ---
        X = last_cycles[['cycle_number']].values
        y = last_cycles['final_cap_rate1'].values
        
        lr_model = LinearRegression()
        lr_model.fit(X, y)
        
        future_X = np.array(future_cycles).reshape(-1, 1)
        lr_forecast = lr_model.predict(future_X)
        
        # --- Арга 2: Экспоненциал жигдрүүлэлт (Holt-Winters) ---
        try:
            # Сүүлийн 10-аас доошгүй өгөгдөл шаардлагатай
            if len(df) >= 10:
                hw_model = ExponentialSmoothing(
                    df['final_cap_rate1'].values[-15:], 
                    trend='add', 
                    seasonal=None,
                    initialization_method='estimated'
                ).fit()
                hw_forecast = hw_model.forecast(5)
            else:
                hw_forecast = [None] * 5
        except Exception as e:
            print(f"Экспоненциал жигдрүүлэлт хийхэд алдаа гарлаа: {e}")
            hw_forecast = [None] * 5
        
        # --- Арга 3: Авторегресс (AR) ---
        try:
            if len(df) >= 10:
                ar_model = AutoReg(df['final_cap_rate1'].values[-20:], lags=3).fit()
                ar_forecast = ar_model.predict(start=len(df)-5, end=len(df)+4)[-5:]
            else:
                ar_forecast = [None] * 5
        except Exception as e:
            print(f"Авторегресс хийхэд алдаа гарлаа: {e}")
            ar_forecast = [None] * 5
        
        # Таамаглалуудыг график дээр харуулах
        # Бодит өгөгдлийн сүүлийн хэсэг
        plot_cycles = list(range(last_cycle_num - 9, last_cycle_num + 1))
        plot_actual = df[df['cycle_number'].isin(plot_cycles)]['final_cap_rate1'].values
        
        if len(plot_actual) > 0:
            # График дээр нэмэх
            axes[1, 1].plot(plot_cycles, plot_actual, 'b-', linewidth=2)
            axes[1, 1].plot(future_cycles, lr_forecast, 'r--', marker='o', label='Хандлагын шугам (LR)')
            
            if hw_forecast[0] is not None:
                axes[1, 1].plot(future_cycles, hw_forecast, 'g--', marker='s', label='Экспоненциал жигдрүүлэлт')
            
            if ar_forecast[0] is not None:
                axes[1, 1].plot(future_cycles, ar_forecast, 'm--', marker='^', label='Авторегресс (AR)')
            
            axes[1, 1].legend()
        
        # Таамаглалын үр дүнг харуулах
        print("\n" + "-"*40)
        print("БОГИНО ХУГАЦААНЫ ТААМАГЛАЛ (дараагийн 5 цикл)")
        print("-"*40)
        print(f"{'Цикл':<10} {'Хандлагын шугам':<15} {'Эксп. жигдр.':<15} {'Авторегресс':<15}")
        print("-"*60)
        
        for i, cycle in enumerate(future_cycles):
            lr_val = f"{lr_forecast[i]:.2f}%" if lr_forecast[i] is not None else "N/A"
            hw_val = f"{hw_forecast[i]:.2f}%" if i < len(hw_forecast) and hw_forecast[i] is not None else "N/A"
            ar_val = f"{ar_forecast[i]:.2f}%" if i < len(ar_forecast) and ar_forecast[i] is not None else "N/A"
            print(f"{cycle:<10} {lr_val:<15} {hw_val:<15} {ar_val:<15}")
        
        # 80% босгод хүрэх хугацааны тооцоо
        print("\n" + "-"*40)
        print("80% БОСГОД ХҮРЭХ ХУГАЦААНЫ ТООЦОО")
        print("-"*40)
        
        current_soh = df['final_cap_rate1'].iloc[-1]
        
        # Хандлагын шугамаар
        if lr_model.coef_[0] < 0:  # Хэрэв бууралттай бол
            cycles_to_80 = (80 - current_soh) / lr_model.coef_[0]
            if cycles_to_80 > 0:
                print(f"Хандлагын шугамаар: {abs(cycles_to_80):.1f} циклийн дараа 80%-д хүрнэ")
                
                # Огноог тооцоолох (ойролцоогоор)
                last_date = df['start_time'].iloc[-1]
                avg_days_per_cycle = (df['start_time'].iloc[-1] - df['start_time'].iloc[0]).days / max(len(df)-1, 1)
                days_to_80 = abs(cycles_to_80) * avg_days_per_cycle
                target_date = last_date + pd.Timedelta(days=days_to_80)
                print(f"Таамагласан огноо: {target_date.strftime('%Y-%m-%d')}")
            else:
                print("Хандлагын шугамаар: SOH нэмэгдэж байна (буурахгүй байна)")
        else:
            print("Хандлагын шугамаар: SOH буурахгүй байна")
        
        # Дундаж бууралтын хурдаар
        if avg_degradation_rate < 0:
            cycles_to_80_avg = (80 - current_soh) / avg_degradation_rate
            print(f"Дундаж бууралтын хурдаар: {abs(cycles_to_80_avg):.1f} циклийн дараа 80%-д хүрнэ")
    
    else:
        print("Богино хугацааны таамаглал хийхэд хангалттай өгөгдөл байхгүй (хамгийн багадаа 5 цикл шаардлагатай)")
    
    plt.tight_layout()
    
    # Графикийг хадгалах
    plot_file = os.path.join(output_folder, f"{site_name}_analysis.png")
    plt.savefig(plot_file, dpi=100, bbox_inches='tight')
    print(f"\nГрафик хадгалагдсан: {plot_file}")
    plt.show()
    
    # Үр дүнг CSV файлд хадгалах
    result_file = os.path.join(output_folder, f"{site_name}_processed.csv")
    df.to_csv(result_file, index=False)
    print(f"Боловсруулсан өгөгдөл хадгалагдсан: {result_file}")
    
    return df

def process_multiple_files(file_paths, output_folder='results'):
    """
    Олон CSV файлуудыг боловсруулах
    
    Parameters:
    file_paths: Файлын замуудын жагсаалт
    output_folder: Үр дүнг хадгалах хавтас
    """
    results = {}
    
    for file_path in file_paths:
        try:
            df_result = process_battery_file(file_path, output_folder)
            if df_result is not None:
                file_name = os.path.basename(file_path).replace('.csv', '')
                results[file_name] = df_result
        except Exception as e:
            print(f"Алдаа гарлаа: {file_path} - {e}")
    
    print("\n" + "="*60)
    print("БҮХ ФАЙЛ БОЛОВСРУУЛАЛТ ДУУСЛАА")
    print("="*60)
    print(f"Амжилттай боловсруулсан: {len(results)}/{len(file_paths)} файл")
    
    return results

# Хэрэв скриптийг шууд ажиллуулбал
if __name__ == "__main__":
    # Жишээ: Нэг файл боловсруулах
    process_battery_file('../data/10.63.43.90.csv')
    
    # Жишээ: Олон файл боловсруулах
    # file_list = ['10.63.43.90.csv']
    # results = process_multiple_files(file_list)
    
    print("\nПрограмм амжилттай дууслаа!")