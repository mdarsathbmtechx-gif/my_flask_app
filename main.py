from flask import Flask, request, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure
from datetime import datetime
import json
import pytz
import os
import re

app = Flask(__name__)

# --- Configuration ---
# Use environment variable for security
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://bmtechx:H5QMb3PptJPEomGx@cluster0.upld5qc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
# Use environment variable for port, common on cloud platforms
PORT = int(os.getenv("PORT", 5000))

# --- MongoDB Connection ---
def get_mongo_client():
    """Establishes a connection to MongoDB and returns the client."""
    try:
        # Set a longer timeout to avoid issues with slow connections
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=30000)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("✅ Successfully connected to MongoDB!")
        return client
    except ConnectionFailure as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return None

# Attempt to connect to MongoDB on app startup
mongo_client = get_mongo_client()
db = mongo_client["test"] if mongo_client is not None else None
# Use a separate collection for storing phone number branches
branch_memory_col = db["branch_memory"] if db is not None else None

# --- Message Processing Logic ---

def detect_branch_from_message(msg: str) -> str:
    """
    Detects the branch based on keywords in the message content.
    This is more robust than a simple startswith check.
    """
    msg_lower = msg.lower().strip()
    
    # Use regular expressions for case-insensitive keyword matching
    # Keywords are more flexible than exact phrases
    if re.search(r'e-commerce|ecommerce|e commerce', msg_lower):
        return "E-Commerce_website_Leads"
    elif re.search(r'3999 website', msg_lower):
        return "Static_Leads"
    elif re.search(r'Hello Need info about Digital Marketing', msg_lower):
        return "Digital_Marketing_4999"    
    else:
        return "Unknown_Leads"

def detect_branch_with_memory(phone: str, msg: str) -> str:
    """
    Detects the branch using a stored memory for the phone number.
    
    This function has been modified to always re-evaluate the branch from
    the message content, rather than using a stored memory. This ensures that
    messages with new keywords are routed to the correct collection.
    """
    if db is None or branch_memory_col is None:
        print("MongoDB not available. Using message-based detection only.")
        return detect_branch_from_message(msg)

    phone_normalized = phone.strip()
    
    # --- CHANGE: Always detect the new branch from the message. ---
    branch = detect_branch_from_message(msg)

    # --- RETAIN: Save or update the branch memory for tracking purposes. ---
    try:
        branch_memory_col.update_one(
            {"phone": phone_normalized},
            {"$set": {"branch": branch}},
            upsert=True
        )
        print(f"💾 Updated branch to '{branch}' in memory for phone: {phone}")
    except PyMongoError as e:
        print(f"❌ MongoDB error saving branch memory: {e}")

    return branch

def extract_message(msg_payload):
    """
    Extracts the message text from the Interakt webhook payload,
    handling various formats including JSON strings.
    """
    # If the payload is a string, try to parse it as JSON first
    if isinstance(msg_payload, str):
        try:
            msg_payload = json.loads(msg_payload)
        except json.JSONDecodeError:
            # If parsing fails, just treat it as a regular string
            pass

    # Now, check if the payload is a dictionary or list
    if isinstance(msg_payload, dict):
        return (
            msg_payload.get("button_reply", {}).get("title") or
            msg_payload.get("list_reply", {}).get("title") or
            msg_payload.get("message") or
            msg_payload.get("text") or
            ""
        )
    # Handle list messages by joining them
    elif isinstance(msg_payload, list):
        return "\n---\n".join(str(m) for m in msg_payload)
    # If it's a simple string, return it directly
    return str(msg_payload)

def append_or_add_message(phone: str, new_msg: str, retries=3):
    """
    Appends a new message to an existing document or creates a new one
    in the appropriate collection. Includes retry logic for robustness.
    """
    now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))
    detected_branch = detect_branch_with_memory(phone, new_msg)

    if db is None:
        print("MongoDB not available. Skipping Mongo insertion.")
        return

    collection = db[detected_branch]

    for attempt in range(1, retries + 1):
        try:
            # Find and update the document in one atomic operation for safety
            result = collection.update_one(
                {"phone": phone},
                {
                    "$push": {"messages": {"text": new_msg, "time": now_ist}},
                    "$set": {"followup_status": "Pending", "updated_at": now_ist},
                    "$setOnInsert": {"created_at": now_ist}
                },
                upsert=True
            )
            
            if result.upserted_id:
                print(f"📌 Created new doc in '{detected_branch}' for phone: {phone}")
            else:
                print(f"✅ Updated '{detected_branch}' for phone: {phone}")

            break # Exit the retry loop on success
        except PyMongoError as e:
            print(f"❌ MongoDB error in '{detected_branch}' (Attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                print(f"Failed to write to '{detected_branch}' after {retries} attempts.")

# --- Flask Webhook Route ---
@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Endpoint for Interakt to send incoming messages.
    Parses the payload and saves the message to MongoDB.
    """
    data = request.get_json(silent=True)
    if data is None:
        print("❌ Received webhook with invalid JSON payload.")
        return jsonify({"status": "error", "reason": "invalid json"}), 400
    
    print("📩 Received webhook JSON:", json.dumps(data, indent=2))

    try:
        phone_number = None
        message = None
        
        # Check for the correct event type and data structure
        if data.get("type") == "message_received" and "data" in data:
            customer = data["data"].get("customer", {})
            phone_number = customer.get("channel_phone_number") or customer.get("phone_number")
            
            message_obj = data["data"].get("message", {})
            raw_message_payload = message_obj.get("message") or message_obj.get("text")
            message = extract_message(raw_message_payload)

        print(f"Extracted - Phone: {phone_number}, Message: {message[:100] if message else None}")

        if not phone_number:
            print("⚠ Missing phone number in payload!")
            return jsonify({"status": "ignored", "reason": "missing phone"}), 200

        if not message:
            print("⚠ Missing message content in payload!")
            return jsonify({"status": "ignored", "reason": "missing message"}), 200

        append_or_add_message(phone_number, message)

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"❌ Error in webhook processing: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400

if __name__ == "__main__":
    # In a cloud environment, you would typically use a production web server like Gunicorn.
    # For a simple deployment, you can run this command.
    # The 'host="0.0.0.0"' makes the app accessible from any public IP, which is required
    # for a cloud server.
    app.run(host="0.0.0.0", port=PORT, debug=True, use_reloader=False)
