import os
import json
from pymongo import MongoClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime
import pytz

# --- Google Sheets setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Load from environment variable
credentials = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=SCOPES
)

SPREADSHEET_ID = '1HICF46gBeg5RFLJvqGBXx9qXmN869xEh-GdIUhsdsz4'

service = build('sheets', 'v4', credentials=credentials)
sheet_service = service.spreadsheets()

# --- MongoDB setup ---
mongo_client = MongoClient(
    "mongodb+srv://bmtechx:H5QMb3PptJPEomGx@cluster0.upld5qc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
db = mongo_client['test']

branch_collections = {
    "E-Commerce_website_Leads": "E-Commerce_website_Leads",
    "Static_Leads": "Static_Leads",
    "Digital_Marketing_4999": "Digital_Marketing_4999",
    "Unknown_Leads": "Unknown_Leads"
}

def fetch_data_from_mongo(collection_name):
    collection = db[collection_name]
    rows = []
    for doc in collection.find():
        phone = doc.get('phone', '')
        branch_name = collection_name
        messages = doc.get('messages', [])
        for m in messages:
            text = m.get('text', '')
            time_obj = m.get('time')
            if time_obj:
                if time_obj.tzinfo is None:
                    time_obj = time_obj.replace(tzinfo=pytz.UTC)
                time_local = time_obj.astimezone(pytz.timezone('Asia/Kolkata'))
                time_str = time_local.strftime('%Y-%m-%d %H:%M:%S')
            else:
                time_str = ''
            rows.append([time_str, phone, text, branch_name])
    return rows

def write_to_sheet(sheet_name, rows):
    headers = ['Timestamp', 'Phone', 'Message', 'Branch']
    data = [headers] + rows
    try:
        sheet_service.values().clear(spreadsheetId=SPREADSHEET_ID, range=sheet_name).execute()
        sheet_service.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_name,
            valueInputOption='RAW',
            body={'values': data}
        ).execute()
        print(f"Updated sheet '{sheet_name}' with {len(rows)} rows")
    except Exception as e:
        print(f"Error updating sheet '{sheet_name}': {e}")

if __name__ == "__main__":
    for branch, collection_name in branch_collections.items():
        try:
            print(f"Fetching data from collection '{collection_name}'...")
            rows = fetch_data_from_mongo(collection_name)
            print(f"Writing data to sheet/tab '{branch}'...")
            write_to_sheet(branch, rows)
        except Exception as e:
            print(f"Error processing collection '{collection_name}': {e}")

    print("Sync complete!")
