import requests
import json
import pandas as pd
import os

CSV_FILES = [
    "ip_source/hw_ip_loc.csv",  
]

BASE_URL   = "http://10.60.32.14:8000/hwbattery/device/{ip}"
TOKEN      = "mcj5qDKjytO6QF7GFGXimC6B32cvrdVS" 
OUTPUT_DIR = "data_huawei"             

os.makedirs(OUTPUT_DIR, exist_ok=True)

headers = {"Authorization": f"Bearer {TOKEN}"}

all_ips = []
for csv_path in CSV_FILES:
    try:
        df = pd.read_csv(csv_path)
        ip_col = next((c for c in df.columns if c.strip().lower() in ['ip', 'ip address', 'ip_address']), None)
        if ip_col is None:
            print(f"{csv_path}: IP багана олдсонгүй. Баганууд: {list(df.columns)}")
            continue
        ips = df[ip_col].dropna().unique().tolist()
        print(f"{csv_path} → {len(ips)} IP")
        all_ips.extend(ips)
    except Exception as e:
        print(f"{csv_path}error : {e}")

all_ips = list(dict.fromkeys(all_ips))

success, failed = 0, 0
for ip in all_ips:
    url = BASE_URL.format(ip=ip)
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, list):
            df_out = pd.DataFrame(data)
        elif isinstance(data, dict):
            list_key = next((k for k, v in data.items() if isinstance(v, list)), None)
            if list_key:
                df_out = pd.DataFrame(data[list_key])
            else:
                df_out = pd.DataFrame([data])
        else:
            print(f"{ip}: json format error")
            with open(os.path.join(OUTPUT_DIR, f"{ip.replace('.', '_')}.json"), 'w') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            continue

        filename = os.path.join(OUTPUT_DIR, f"{ip.replace('.', '_')}.csv")
        df_out.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"{ip} → {filename} ({len(df_out)} мөр)")
        success += 1

    except requests.exceptions.Timeout:
        print(f"{ip}: Timeout")
        failed += 1
    except requests.exceptions.HTTPError as e:
        print(f"{ip}: HTTP error {e}")
        failed += 1
    except Exception as e:
        print(f"{ip}: {e}")
        failed += 1

print(f"\n{'='*40}")
print(f"Дууслаа:{success} амжилттай | {failed} алдаатай")
print(f"Файлууд '{OUTPUT_DIR}' хавтаст хадгалагдлаа")