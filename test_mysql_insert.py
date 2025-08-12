import mysql.connector
from datetime import datetime

try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Arsath00!!",
        database="interakt"
    )
    cursor = db.cursor()
    now = datetime.now()

    cursor.execute("""
        INSERT INTO interakt_data (phone, message, branch, followup_status, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, ("9999999999", "Test message", "Test_Branch", "Pending", now.strftime("%Y-%m-%d %H:%M:%S")))
    db.commit()
    print("Insert successful")
except Exception as e:
    print("MySQL error:", e)
finally:
    cursor.close()
    db.close()
