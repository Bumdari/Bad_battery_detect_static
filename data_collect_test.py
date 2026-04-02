import requests
import json

url = "http://172.27.53.235:8000/ztebattery/latest"

# "http://172.27.53.235:8000/hwbattery/latest"  - HW-iin buh device iin hamgiiin suuld avsan data
# "http://172.27.53.235:8000/hwbattery/device/{ip}" - HW-iin songoson device iin data-uud
# "http://172.27.53.235:8000/hwbattery/{ip}/latest" - HW-iin songoson device iin hamgiiin suuld avsan data
# "http://172.27.53.235:8000/hwbattery/{ip}/history?days=30" - HW device iin data-g udruur filter hiij avah
# "http://172.27.53.235:8000/hwbattery?page=1&limit=50" - HW buh data-g pagenation hiij harah
# "http://172.27.53.235:8000/hwbattery/date/2026-3-7" - HW data -g date eer filter hiij harah

# "http://172.27.53.235:8000/hwbattery/device/{ip}" 
# "http://172.27.53.235:8000/ztebattery/device/{ip}" - zte dr ztebattery gd ard n bh endpoint uud adilhan

headers = {
    "Authorization": "Bearer mcj5qDKjytO6QF7GFGXimC6B32cvrdVS"
}

r = requests.get(url, headers=headers)

data = r.json()

print(json.dumps(data, indent=4))