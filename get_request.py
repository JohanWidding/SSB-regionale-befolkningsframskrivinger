import pandas as pd
import requests
import json
from jsonstat2_handeler import JSONStat2Handler

# Read the file and parse JSON into a dictionary
payload = json.load(open('payload.txt'))

url = "https://data.ssb.no/api/v0/no/table/14288/"

# Send POST-forespørsel
response = requests.post(url, json=payload)

data = JSONStat2Handler(response.json())
df = data.jsonstat_to_merged_dataframe(
    time_column = "Tid", 
    primary_category = "Region", 
    secondary_category = "ContentsCode", 
    columns_to_merge = ["Kjonn", "Alder"]
)

# Save the DataFrame as an Excel file
df.to_csv("output.csv", index=True)