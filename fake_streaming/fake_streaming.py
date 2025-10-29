import pandas as pd
import psycopg2
import time
import random
import numpy as np
from psycopg2 import OperationalError


# ==============================
# Hàm tiện ích
# ==============================
def find_col(df, candidates):
    """Tìm cột có tên gần đúng (bỏ phân biệt hoa thường, dấu gạch dưới)."""
    norm = [c.lower().replace("_", "") for c in df.columns]
    for cand in candidates:
        if cand.lower().replace("_", "") in norm:
            return df.columns[norm.index(cand.lower().replace("_", ""))]
    return None


def clean_date(val):
    """Trả về None nếu giá trị không hợp lệ hoặc NaN."""
    if pd.isna(val) or str(val).lower() in ["nan", "none", "null", ""]:
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def clean_value(val):
    """Chuyển np.* sang kiểu Python gốc để PostgreSQL hiểu."""
    if pd.isna(val):
        return None
    if isinstance(val, (np.generic, np.float64, np.int64)):
        return val.item()
    return val


# ==============================
# Kết nối PostgreSQL
# ==============================
try:
    conn = psycopg2.connect(
        dbname="northwind",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    print("Connected to PostgreSQL successfully.")
except OperationalError as e:
    print(f"Database connection failed: {e}")
    exit(1)


# ==============================
# Đọc CSV
# ==============================
orders = pd.read_csv("./orders.csv", on_bad_lines="skip")
order_details = pd.read_csv("./order-details.csv", on_bad_lines="skip")

orders.columns = orders.columns.str.strip()
order_details.columns = order_details.columns.str.strip()

print("Orders columns:", list(orders.columns))

# ==============================
# Ánh xạ tên cột
# ==============================
colmap_orders = {
    "order_id": find_col(orders, ["order_id", "orderid", "OrderID"]),
    "customer_id": find_col(orders, ["customer_id", "customerid", "CustomerID"]),
    "employee_id": find_col(orders, ["employee_id", "employeeid", "EmployeeID"]),
    "order_date": find_col(orders, ["order_date", "orderdate", "OrderDate"]),
    "required_date": find_col(orders, ["required_date", "requireddate", "RequiredDate"]),
    "shipped_date": find_col(orders, ["shipped_date", "shippeddate", "ShippedDate"]),
    "ship_via": find_col(orders, ["ship_via", "shipvia", "ShipVia"]),
    "freight": find_col(orders, ["freight", "Freight"]),
    "ship_name": find_col(orders, ["ship_name", "shipname", "ShipName"]),
    "ship_address": find_col(orders, ["ship_address", "shipaddress", "ShipAddress"]),
    "ship_city": find_col(orders, ["ship_city", "shipcity", "ShipCity"]),
    "ship_region": find_col(orders, ["ship_region", "shipregion", "ShipRegion"]),
    "ship_postal_code": find_col(orders, ["ship_postal_code", "shippostalcode", "ShipPostalCode"]),
    "ship_country": find_col(orders, ["ship_country", "shipcountry", "ShipCountry"]),
}

print("Column mapping:", colmap_orders)


# ==============================
# Streaming từng order
# ==============================
order_id_col = colmap_orders["order_id"]
if order_id_col is None:
    print("Không tìm thấy cột order_id trong orders.csv! Hãy kiểm tra tên cột trong file.")
    exit(1)

detail_orderid_col = find_col(order_details, ["order_id", "orderid", "OrderID"])

for _, order in orders.iterrows():
    try:
        order_id = clean_value(order[order_id_col])
        if order_id is None:
            continue

        values = tuple(
            clean_value(order.get(colmap_orders[col])) if "date" not in col else clean_date(order.get(colmap_orders[col]))
            for col in [
                "order_id", "customer_id", "employee_id", "order_date", "required_date",
                "shipped_date", "ship_via", "freight", "ship_name", "ship_address",
                "ship_city", "ship_region", "ship_postal_code", "ship_country"
            ]
        )

        cur.execute("""
            INSERT INTO orders (
                order_id, customer_id, employee_id, order_date, required_date, shipped_date,
                ship_via, freight, ship_name, ship_address, ship_city, ship_region,
                ship_postal_code, ship_country
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING;
        """, values)
        conn.commit()

        # Chi tiết đơn hàng
        details = order_details[order_details[detail_orderid_col] == order_id]
        for _, d in details.iterrows():
            cur.execute("""
                INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (order_id, product_id) DO NOTHING;
            """, (
                order_id,
                clean_value(d.get(find_col(order_details, ["product_id", "productid", "ProductID"]))),
                clean_value(d.get(find_col(order_details, ["unit_price", "unitprice", "UnitPrice"]))),
                clean_value(d.get(find_col(order_details, ["quantity", "Quantity"]))),
                clean_value(d.get(find_col(order_details, ["discount", "Discount"]))),
            ))
        conn.commit()

        print(f"Streamed order {order_id} with {len(details)} details.")
        time.sleep(random.uniform(2, 4))

    except Exception as e:
        print(f"Error while inserting order {order_id}: {e}")
        conn.rollback()

cur.close()
conn.close()
print("Streaming finished and connection closed.")
