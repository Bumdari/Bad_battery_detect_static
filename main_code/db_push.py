import psycopg2
import pandas as pd

DB_CONFIG = {
    'host'    : '10.60.32.10',
    'dbname'  : 'report',
    'user'    : 'ads_dodov',
    'password': 'Mobicom1',
    'port'    : 5432
}

ZTE_FILE    = 'output_analysis_zte.xlsx'
HUAWEI_FILE = 'output_analysis_huawei.xlsx'

def load_excel(filepath, device_type):
    df = pd.read_excel(filepath)
    df['device_type'] = device_type
    if 'last avg_current' not in df.columns:
        df['last avg_current'] = None
    return df

def upsert_to_postgres(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO bad_battery_static_sites (ip_address, site_location, device_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (ip_address)
            DO UPDATE SET
                site_location = EXCLUDED.site_location,
                device_type   = EXCLUDED.device_type
            RETURNING id
        """, (
            row['ip address'],
            row['site location'],
            row['device_type']
        ))
        site_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO bad_battery_static_records (
                site_id,
                battery_change_date,
                baseline,
                slope_30d_median,
                drop_percent,
                last_start_date,
                last_duration_min,
                last_stop_cause,
                avg_current,
                condition,
                status_forecast,
                recorded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            site_id,
            row['battery change date'],
            row['baseline'],
            row['slope(30d median)'],
            row['drop percent'],
            row['last start date'],
            row['last duration_min'],
            row['last stop cause'],
            row.get('last avg_current'),
            row['critical degrading stable'],
            row['status & forecast']
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    return inserted

def main():
    total = 0
    try:
        zte_df = load_excel(ZTE_FILE, 'ZTE')
        n = upsert_to_postgres(zte_df)
        total += n
    except FileNotFoundError:
        print(f" {ZTE_FILE} not found, skip")
    except Exception as e:
        print(f"ZTE error: {e}")

    try:
        huawei_df = load_excel(HUAWEI_FILE, 'Huawei')
        n = upsert_to_postgres(huawei_df)
        total += n
    except FileNotFoundError:
        print(f"{HUAWEI_FILE} not found, skip")
    except Exception as e:
        print(f"Huawei error: {e}")

    print(f"Done: {total}")

if __name__ == '__main__':
    main()