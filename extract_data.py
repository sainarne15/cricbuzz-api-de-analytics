import http.client
import csv
import json

conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "ac6b3f07f0msheb9bb9264f84dffp1f7bf8jsn6fc56a519adb",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

conn.request("GET", "/stats/v1/rankings/batsmen?formatType=odi", headers=headers)

res = conn.getresponse()

if res.status == 200:
    # Read the response data and decode it
    data = json.loads(res.read().decode('utf-8')).get('rank', [])  # Extracting the 'rank' data
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']  # Specify required field names

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
           # writer.writeheader()  # Write header row in CSV
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", res.status)
