import pandas as pd
from sqlalchemy import create_engine

# === 🔧 CONFIGURATION ===
host = "database-1.cluster-c8l2wyauq52o.us-east-1.rds.amazonaws.com"
user = "admin"
password = "Godisgreat1"
database = "rental_marketplace"
table_name = "user_viewing"
csv_file = r"C:\Users\user\Desktop\Amasah\Amalitech\labs\2--Batch ETL with Glue, Step Function\data\user_viewing.csv"

# === ✅ CONNECT TO AURORA ===
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# === 📥 READ CSV ===
df = pd.read_csv(csv_file)

# ✅ Clean/transform data
df["is_wishlisted"] = df["is_wishlisted"].fillna(0).astype(int)
df["viewed_at"] = pd.to_datetime(df["viewed_at"], errors="coerce")

# === 🚀 UPLOAD TO AURORA ===
try:
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print("✅ user_viewing.csv uploaded successfully!")
except Exception as e:
    print("❌ Upload failed:", e)
