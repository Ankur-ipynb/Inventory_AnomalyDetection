import sqlite3
import re

# Connect to the database
conn = sqlite3.connect('D:\\inventory_project\\data\\ml_metrics.db')
cursor = conn.cursor()

# Add the category column if it doesn't exist
cursor.execute("PRAGMA table_info(dashboard_notifications)")
columns = [col[1] for col in cursor.fetchall()]
if 'category' not in columns:
    cursor.execute("ALTER TABLE dashboard_notifications ADD COLUMN category TEXT")

# Extract category from the notification text and update the table
cursor.execute("SELECT rowid, notification FROM dashboard_notifications WHERE category IS NULL")
rows = cursor.fetchall()

for rowid, notification in rows:
    # Extract category using regex
    category_match = re.search(r"Category: (.+?)\n", notification)
    if category_match:
        category = category_match.group(1)
        cursor.execute(
            "UPDATE dashboard_notifications SET category = ? WHERE rowid = ?",
            (category, rowid)
        )

# Commit changes and close
conn.commit()
conn.close()

print("Migration completed successfully.")