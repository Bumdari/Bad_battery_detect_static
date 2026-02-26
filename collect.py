import psycopg2
import pandas as pd
import os
from datetime import datetime

# ================= DB CONNECTION =================
conn = psycopg2.connect(
    dbname="report",
    user="ads_dodov",
    password="Mobicom1",
    host="172.27.53.231",
    port="5432"
)

cursor = conn.cursor()

# ================= PATH SETUP =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATE_DIR = os.path.join(BASE_DIR, "state")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

print("===== START EXPORT =====")

# ================= GET ALL UNIQUE IPs =================
cursor.execute('SELECT DISTINCT "IP" FROM public.battery_discharge')
ips = [row[0] for row in cursor.fetchall()]

total_new_rows = 0
updated_ips = 0

for ip in ips:

    print(f"\nProcessing IP: {ip}")

    state_file = os.path.join(STATE_DIR, f"{ip}.state")

    # ---------- Read last exported ID ----------
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            last_id = int(f.read().strip())
    else:
        last_id = 0

    # ---------- Fetch new rows ----------
    query = """
        SELECT *
        FROM public.battery_discharge
        WHERE "IP" = %s
        AND id > %s
        ORDER BY id ASC
    """

    df = pd.read_sql(query, conn, params=(ip, last_id))

    if df.empty:
        print("No new rows.")
        continue

    # ---------- CSV path ----------
    csv_path = os.path.join(DATA_DIR, f"{ip}.csv")

    # ---------- Append or Create ----------
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode='a', index=False, header=False)
    else:
        df.to_csv(csv_path, index=False)

    # ---------- Update state ----------
    new_last_id = df["id"].max()

    with open(state_file, "w") as f:
        f.write(str(new_last_id))

    total_new_rows += len(df)
    updated_ips += 1

    print(f"Inserted {len(df)} new rows.")

cursor.close()
conn.close()

print("\n===== SUMMARY =====")
print(f"Updated IP count : {updated_ips}")
print(f"New Rows Exported: {total_new_rows}")
print("===== END =====")