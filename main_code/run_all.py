import subprocess
import sys

steps = [
    ('ZTE data татах', 'data_collect_zt.py'),
    ('ZTE анализ',     'analysis_zte.py'),
    ('Huawei data татах', 'data_collect_huawei.py'),
    ('Huawei анализ',  'analysis_huawei.py'),
    ('Email илгээх',   'send_report.py'),
    ('DB хадгалах',    'db_push.py'),
]

for name, script in steps:
    print(f"\n {name} ({script})...")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"{script} алдаатай дууслаа — pipeline зогслоо")
        sys.exit(1)
    print(f"{name} дууслаа")

