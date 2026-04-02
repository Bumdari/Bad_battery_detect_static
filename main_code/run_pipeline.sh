cd /opt/bad_battery_detect_static/main_code

LOG_FILE="/opt/bad_battery_detect_static/main_code/pipeline.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "========================================" >> $LOG_FILE
echo "[$DATE] Pipeline эхэллээ" >> $LOG_FILE

run_step() {
    local name=$1
    local script=$2
    echo "[$DATE] $name ажиллаж байна..." >> $LOG_FILE
    python3 /opt/bad_battery_detect_static/main_code/$script >> $LOG_FILE 2>&1
    if [ $? -ne 0 ]; then
        echo "[$DATE] ✗ $script амжилтгүй болов, зогслоо" >> $LOG_FILE
        exit 1
    fi
    echo "[$DATE] ✓ $name дууслаа" >> $LOG_FILE
}

run_step "ZTE data татах"    "data_collect_zt.py"
run_step "ZTE анализ"        "analysis_zte.py"
run_step "Huawei data татах" "data_collect_huawei.py"
run_step "Huawei анализ"     "analysis_huawei.py"
run_step "Email илгээх"      "send_report.py"
run_step "DB хадгалах"       "db_push.py"

echo "[$DATE] ✓ Pipeline бүгд амжилттай дууслаа" >> $LOG_FILE
echo "========================================" >> $LOG_FILE