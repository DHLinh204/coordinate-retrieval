import time
import datetime
import json
from pydantic import BaseModel
from fastapi import FastAPI
import pandas as pd
import requests
import gspread
from rapidfuzz import process, fuzz
from fuzzywuzzy import fuzz, process

from google.oauth2.service_account import Credentials

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)

app = FastAPI()

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

API_KEY = "AIzaSyAlO3YXKVNaMFrZuBRlL74WuaYJASJiOSg"

# ====================================================================

class SheetInput(BaseModel):
    sheet_url: str
    worksheet_name: str | None = None
    def get_worksheet(self):
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPE)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(self.sheet_url)

        worksheet_name = (self.worksheet_name or "").strip()
        try:
            if worksheet_name == "":
                worksheet = sheet.get_worksheet(0)
            else:
                worksheet = sheet.worksheet(worksheet_name)
        except Exception:
            worksheet = sheet.get_worksheet(0)
        return worksheet

    def update_coordinates(self, worksheet):
        df = pd.DataFrame(worksheet.get_all_records()).astype(object)

        for i, row in df.iterrows():
            if row.get('timestamp') != '':
                continue
            place_name = row['name(search)']
            # print(place_name)
            if not place_name:
                continue

            url = f'https://maps.googleapis.com/maps/api/place/textsearch/json?query={place_name}&language=vi&region=VN&key={API_KEY}'
            response = requests.get(url)
            data = response.json()

            if data['status'] == 'OK':
                # timestamp_str = None
                lat, lng = None, None
                for j, result in enumerate(data['results'][:3]):
                    lat = result['geometry']['location']['lat']
                    lng = result['geometry']['location']['lng']

                    name_col = f"name{j + 1}"
                    formatted_address = f"formatted_address{j + 1}"
                    df.loc[i, 'data'] = json.dumps(data['results'])
                    df.loc[i, name_col] = result.get('name')
                    df.loc[i, formatted_address] = result.get('formatted_address')

                if len(data['results']) == 1:
                    df.loc[i, 'name_valid'] = data['results'][0].get('name')
                    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df.loc[i, 'data'] = json.dumps(data['results'])
                    df.loc[i, 'lat'] = str(lat)
                    df.loc[i, 'lng'] = str(lng)
                    df.loc[i, f"timestamp"] = timestamp_str
            time.sleep(1)

        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    # ====================================================================

    def partial_match(self, worksheet):
        df = pd.DataFrame(worksheet.get_all_records()).astype(object)

        for i, row in df.iterrows():
            if row.get('timestamp') != "":
                continue
            place_name = row["name(search)"]
            # print(place_name)
            if not place_name:
                continue
            choice = []

            data_str = row.get('data', '')
            try:
                results = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            for j in range(1, 4):
                row_name = f"name{j}"
                name = row.get(row_name, '')
                if name:
                    choice.append(name)

            if choice:
                best_match = process.extractOne(place_name, choice, scorer=fuzz.partial_ratio)
                # print(best_match)
                df.loc[i, 'name_valid'] = best_match[0]
                timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df.loc[i, f"timestamp"] = timestamp_str
                best_result = next((r for r in results if r['name'] == best_match[0]), None)
                if best_result:
                    lat = str(best_result['geometry']['location']['lat'])
                    lng = str(best_result['geometry']['location']['lng'])
                    df.loc[i, 'lat'] = lat
                    df.loc[i, 'lng'] = lng
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    def update_if_name_valid_changed(self, worksheet):

        df = pd.DataFrame(worksheet.get_all_records()).astype(object)

        for i, row in df.iterrows():
            if row.get('timestamp') != "":
                continue
            name_valid = str(row.get("name_valid", "")).strip()
            data_str = row.get("data", "")

            try:
                results = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            matched_result = next(
                (r for r in results if r.get("name", "").strip() == name_valid),
                None
            )

            if matched_result:
                lat = str(matched_result["geometry"]["location"]["lat"])
                lng = str(matched_result["geometry"]["location"]["lng"])

                if df.loc[i, "lat"] != lat or df.loc[i, "lng"] != lng:
                    df.loc[i, "lat"] = lat
                    df.loc[i, "lng"] = lng
                    df.loc[i, "timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        worksheet.update([df.columns.values.tolist()] + df.values.tolist())


@app.post("/process_sheet/")
def process_sheet(sheet_input: SheetInput):
    worksheet = sheet_input.get_worksheet()
    sheet_input.update_coordinates(worksheet)
    sheet_input.partial_match(worksheet)

@app.put("/process_sheet/")
def update_sheet(sheet_input: SheetInput):
    worksheet = sheet_input.get_worksheet()
    sheet_input.update_if_name_valid_changed(worksheet)

# update_coordinates()
# partial_match()

