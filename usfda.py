# Quick script to pull data from the US Food and Drug Administration API.
# Requested by the regulatory team because the USFDA website can only show up to 500 records in total.
# Excel Power Query was used to parse the JSON files into tables. 

import requests
import time
import json

url = "https://api.fda.gov/device/event.json"
key = ""

codes = ["hwc", "hsb", "jds", "ktt", "ktw", "lxt", "nde", "ndh"]

def get_total(code):
    call = requests.get(url, headers={"api_key":key}, params={"limit": 1, "search":"device.device_report_product_code:" + code + " AND date_received:[20230101 TO 20250631]"})
    print(call.url)
    response = call.json()
    return int(response["meta"]["results"]["total"]) // 1000 + 1 

def get_data(delay, skip, code):
    while True:
        time.sleep(delay)
        try:
            response = requests.get(url, headers={"api_key":key}, params={"skip": skip, "limit": 1000, "search":"device.device_report_product_code:" + code + " AND date_received:[20230101 TO 20250631]"})
            response.raise_for_status()

            return response.json()["results"]
            
        except requests.exceptions.RequestException as e:
            print({e})
            delay = 30
            continue


if __name__ == "__main__":

    for code in codes:

        skip = 0
        total = get_total(code)
        print(f"{code} total: {total}")

        data = []
        for i in range(total):
            print(f"Call {i + 1} of {total}")
            call = get_data(0, skip, code)
            data.extend(call)
            skip += 1000

        with open("usfda_" + code + ".json", "w") as f:
            json.dump(data, f, indent=4)
