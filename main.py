import os
import json
import time
import threading
import pytz
from flask import Flask, jsonify
from pymongo import MongoClient
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- Google Sheets setup ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Prefer env var on Render; fall back to local file for dev
if "GOOGLE_CREDENTIALS" in os.environ:
    credentials = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=SCOPES
    )
else:
    credentials = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1HICF46gBeg5RFLJvqGBXx9qXmN869xEh-GdIUhsdsz4")

service = build('sheets', 'v4', credentials=credentials)
sheet_service = service.spreadsheets()

# --- MongoDB setup ---
mongo_uri = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://bmtechx:H5QMb3PptJPEomGx@cluster0.upld5qc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
mongo_client = MongoClient(mongo_uri)
db = mongo_client['test']

# Map Mongo collections -> Google Sheet tab names (identical here)
branch_collections = {
    "E-Commerce_website_Leads": "E-Commerce_website_Leads",
    "Static_Leads": "Static_Leads",
    "Digital_Marketing_4999": "Digital_Marketing_4999",
    "FSD_9999": "FSD_9999",
    "Unknown_Leads": "Unknown_Leads",
}

def ensure_tab_exists(sheet_name: str):
    """Create the Google Sheet tab if it's missing."""
    meta = sheet_service.get(spreadsheetId=SPREADSHEET_ID).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if sheet_name not in titles:
        print(f"🆕 Tab '{sheet_name}' not found. Creating…")
        sheet_service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "requests": [{
                    "addSheet": {"properties": {"title": sheet_name}}
                }]
            }
        ).execute()
        print(f"✅ Created tab '{sheet_name}'")

def fetch_data_from_mongo(collection_name: str):
    """Read all docs from a Mongo collection and flatten messages to rows."""
    collection = db[collection_name]
    rows = []
    for doc in collection.find():
        phone = doc.get('phone', '')
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
            rows.append([time_str, phone, text, collection_name])
    return rows

def write_to_sheet(sheet_name: str, rows):
    """Clear + write header + data into a tab."""
    try:
        ensure_tab_exists(sheet_name)

        headers = ['Timestamp', 'Phone', 'Message', 'Branch']
        data = [headers] + rows  # rows can be []

        # Clear existing range (whole sheet tab)
        sheet_service.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_name
        ).execute()

        # Always write at least the header row
        sheet_service.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_name,
            valueInputOption='RAW',
            body={'values': data}
        ).execute()

        print(f"✅ Updated sheet '{sheet_name}' with {len(rows)} data rows")
    except Exception as e:
        print(f"❌ Error updating sheet '{sheet_name}': {e}")

def sync_once():
    """One full sync for all branches with logging."""
    for branch, collection_name in branch_collections.items():
        try:
            print(f"📥 Fetching from Mongo '{collection_name}' …")
            rows = fetch_data_from_mongo(collection_name)
            print(f"➡  {collection_name}: {len(rows)} rows")

            print(f"📤 Writing to sheet tab '{branch}' …")
            write_to_sheet(branch, rows)
        except Exception as e:
            print(f"⚠ Error processing '{collection_name}': {e}")

def sync_loop():
    while True:
        print("🔁 Starting scheduled sync …")
        sync_once()
        print("⏳ Sync complete. Waiting 1 minute …")
        time.sleep(60)  # 1 minute for testing; increase later

# --- Flask web server ---
app = Flask(__name__)

@app.route('/')
def home():
    return "Service is running"

@app.route('/healthz')
def healthz():
    return "OK"

@app.route("/update/<branch>", methods=["POST"])
def update_branch(branch):
    """Manually trigger update for one branch (tab must match)."""
    try:
        if branch not in branch_collections:
            return jsonify({"status": "error", "message": f"Unknown branch '{branch}'"}), 400
        rows = fetch_data_from_mongo(branch_collections[branch])
        write_to_sheet(branch, rows)
        return jsonify({"status": "success", "rows_updated": len(rows)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/update_all", methods=["POST"])
def update_all():
    """Manually trigger a full sync for all branches."""
    try:
        sync_once()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Background sync (keep for periodic updates)
    threading.Thread(target=sync_loop, daemon=True).start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
