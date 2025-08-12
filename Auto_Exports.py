import schedule
import time
import os
import sys
from datetime import datetime

# Set up logging to a file
log_file = 'auto_export_log.txt'
sys.stdout = open(log_file, 'a')
sys.stderr = sys.stdout

def export_to_sheets():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Exporting to Google Sheets...")
    exit_code = os.system("python Exports_Sheets.py")  # or "python3" if needed
    if exit_code == 0:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Export completed successfully.")
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Export failed with exit code {exit_code}.")

# Schedule: every hour
schedule.every(1).minutes.do(export_to_sheets)

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Auto export script started. Running every hour.")

# Run once at startup (optional, comment out if not needed)
export_to_sheets()

while True:
    schedule.run_pending()
    time.sleep(1)