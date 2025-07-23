import sqlite3
import pandas as pd
import os

db_file = "ecommerce.db"
data_dir = "data"

csv_files = {
    "product_eligibility_table": "C:/Users/Yohan Krishna/PycharmProjects/Anarix/Product-Level Eligibility Table.csv",
    "product_ad_sales_metrics": "C:/Users/Yohan Krishna/PycharmProjects/Anarix/Product-Level Ad Sales and Metrics.csv",
    "product_total_sales_metrics": "C:/Users/Yohan Krishna/PycharmProjects/Anarix/Product-Level Total Sales and Metrics.csv"
}

# DB Connection
if os.path.exists(db_file):
    os.remove(db_file)
    print(f"Removed Existing DB '{db_file}'")

con = sqlite3.connect(db_file)
cursor = con.cursor()
print(f"Created New DB '{db_file}'\n")

# Load CSV to DB
for table_name, file_name in csv_files.items():
    csv_path = os.path.join(data_dir, file_name)
    if not os.path.join(csv_path):
        print(f"CSV not found at {csv_path}, Skipped table {table_name}")
        continue
    try:
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.lower()

        df.to_sql(table_name,con,if_exists="replace",index=False)
        print(f"{table_name} Table Created.")

    except Exception as e:
        print(f"Error {csv_path}: {e}")

# Printing DB
cursor.execute("SELECT name from sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    table_name = table[0]
    print(f"\nTable Name - {table_name}")
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    for col in columns:
        print(f"- {col[1]} ({col[2]})")  # col[2] - data type

con.close()
print("\nDB Setup Completed")
