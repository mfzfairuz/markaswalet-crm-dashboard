"""
MARKASWALET CRM — Backend API
FastAPI + MySQL (Cloud SQL)
"""

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from typing import Optional, List
import pandas as pd
import numpy as np
import os
import re
import io
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Markaswalet CRM API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DATABASE ─────────────────────────────────────────────────────────
DB_USER   = os.getenv("DB_USER", "markaswalet_app")
DB_PASS   = os.getenv("DB_PASS", "Markas2026")
DB_NAME   = os.getenv("DB_NAME", "markaswalet_crm")
DB_SOCKET = os.getenv("DB_SOCKET", "")
DB_HOST   = os.getenv("DB_HOST", "34.50.98.6")

if DB_SOCKET:
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@localhost/{DB_NAME}"
        f"?unix_socket={DB_SOCKET}"
    )
else:
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
    )

def get_db():
    with engine.connect() as conn:
        yield conn

# ── UTILS ─────────────────────────────────────────────────────────────
def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    p = re.sub(r'[^\d]', '', str(raw))
    if p.startswith('0'):   p = '62' + p[1:]
    elif p.startswith('8'): p = '62' + p
    if len(p) < 10 or not p.startswith('62'):
        return None
    return p[:13]

def rows_to_dict(result):
    keys = result.keys()
    return [dict(zip(keys, row)) for row in result.fetchall()]

# ════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ════════════════════════════════════════════════════════════════════
@app.get("/")
def root():
    return {"status": "ok", "app": "Markaswalet CRM API", "version": "1.0.0"}

@app.get("/health")
def health(conn=Depends(get_db)):
    result = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
    return {"status": "ok", "customers": result}

# ════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ════════════════════════════════════════════════════════════════════

@app.get("/customers")
def list_customers(
    search: Optional[str] = Query(None, description="Cari nama atau nomor HP"),
    segment: Optional[str] = Query(None, description="Filter segment: New/Returning/Loyal/Churn"),
    province: Optional[str] = Query(None, description="Filter provinsi"),
    platform: Optional[str] = Query(None, description="Filter platform: orderonline/mengantar"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    conn=Depends(get_db)
):
    """List semua customer dengan filter dan pagination"""
    offset = (page - 1) * limit

    where = ["1=1"]
    params = {}

    if search:
        # Cek apakah search adalah nomor HP
        phone = normalize_phone(search)
        if phone:
            where.append("customer_id LIKE :phone")
            params["phone"] = f"{phone}%"
        else:
            where.append("name LIKE :name")
            params["name"] = f"%{search}%"

    if segment:
        where.append("segment = :segment")
        params["segment"] = segment

    if province:
        where.append("province LIKE :province")
        params["province"] = f"%{province}%"

    if platform:
        where.append("(first_platform = :platform OR last_platform = :platform)")
        params["platform"] = platform

    where_sql = " AND ".join(where)

    # Total count
    total = conn.execute(
        text(f"SELECT COUNT(*) FROM customers WHERE {where_sql}"),
        params
    ).scalar()

    # Data
    result = conn.execute(
        text(f"""
            SELECT customer_id, name, city, province, segment,
                   total_orders, total_revenue, avg_order_value,
                   last_order_date, first_platform, last_platform,
                   recency_days, first_order_date
            FROM customers
            WHERE {where_sql}
            ORDER BY last_order_date DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset}
    )

    customers = rows_to_dict(result)

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": -(-total // limit),
        "data": customers
    }


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str, conn=Depends(get_db)):
    """Detail customer + order history"""

    # Customer info
    result = conn.execute(
        text("SELECT * FROM customers WHERE customer_id = :id"),
        {"id": customer_id}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Customer tidak ditemukan")

    customer = dict(zip(result.keys(), row))

    # Order history
    orders = conn.execute(
        text("""
            SELECT o.order_id, o.source_platform, o.order_date,
                   o.order_status, o.payment_method, o.net_revenue,
                   o.shipping_cost, o.shipping_provider, o.shipping_type,
                   o.total_qty, o.receipt_number,
                   GROUP_CONCAT(oi.product_name ORDER BY oi.id SEPARATOR ', ') as products
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
                AND o.source_platform = oi.source_platform
            WHERE o.customer_id = :id
            GROUP BY o.id
            ORDER BY o.order_date DESC
        """),
        {"id": customer_id}
    )
    customer["orders"] = rows_to_dict(orders)
    customer["order_count"] = len(customer["orders"])

    return customer


@app.get("/customers/phone/{phone}")
def lookup_by_phone(phone: str, conn=Depends(get_db)):
    """Lookup customer by nomor HP — auto normalize format"""
    normalized = normalize_phone(phone)
    if not normalized:
        raise HTTPException(status_code=400, detail="Format nomor HP tidak valid")

    result = conn.execute(
        text("SELECT * FROM customers WHERE customer_id = :id"),
        {"id": normalized}
    )
    row = result.fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Customer dengan nomor {normalized} tidak ditemukan"
        )

    customer = dict(zip(result.keys(), row))

    # Order history
    orders = conn.execute(
        text("""
            SELECT o.order_id, o.source_platform, o.order_date,
                   o.order_status, o.payment_method, o.net_revenue,
                   o.shipping_cost, o.total_qty,
                   GROUP_CONCAT(oi.product_name ORDER BY oi.id SEPARATOR ', ') as products
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
                AND o.source_platform = oi.source_platform
            WHERE o.customer_id = :id
            GROUP BY o.id
            ORDER BY o.order_date DESC
        """),
        {"id": normalized}
    )
    customer["orders"] = rows_to_dict(orders)

    return customer

# ════════════════════════════════════════════════════════════════════
# ORDERS
# ════════════════════════════════════════════════════════════════════

@app.get("/orders")
def list_orders(
    status: Optional[str] = None,
    platform: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    conn=Depends(get_db)
):
    """List orders dengan filter"""
    offset = (page - 1) * limit
    where = ["1=1"]
    params = {}

    if status:
        where.append("order_status = :status")
        params["status"] = status
    if platform:
        where.append("source_platform = :platform")
        params["platform"] = platform
    if date_from:
        where.append("order_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("order_date <= :date_to")
        params["date_to"] = date_to

    where_sql = " AND ".join(where)

    total = conn.execute(
        text(f"SELECT COUNT(*) FROM orders WHERE {where_sql}"), params
    ).scalar()

    result = conn.execute(
        text(f"""
            SELECT order_id, source_platform, order_date, customer_name,
                   order_status, payment_method, net_revenue, shipping_cost,
                   shipping_provider, total_qty
            FROM orders
            WHERE {where_sql}
            ORDER BY order_date DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset}
    )

    return {
        "total": total,
        "page": page,
        "pages": -(-total // limit),
        "data": rows_to_dict(result)
    }

# ════════════════════════════════════════════════════════════════════
# ANALYTICS
# ════════════════════════════════════════════════════════════════════

@app.get("/analytics/summary")
def analytics_summary(conn=Depends(get_db)):
    """Summary statistik utama"""

    # Revenue & order stats
    stats = conn.execute(text("""
        SELECT
            COUNT(*) as total_orders,
            SUM(net_revenue) as total_revenue,
            AVG(net_revenue) as avg_order_value,
            SUM(shipping_cost) as total_shipping,
            SUM(seller_shipping_discount) as total_subsidi_ongkir
        FROM orders
        WHERE order_status = 'completed'
    """)).fetchone()

    # Customer stats
    cust = conn.execute(text("""
        SELECT
            COUNT(*) as total_customers,
            SUM(CASE WHEN segment = 'Loyal' THEN 1 ELSE 0 END) as loyal,
            SUM(CASE WHEN segment = 'Returning' THEN 1 ELSE 0 END) as returning_cust,
            SUM(CASE WHEN segment = 'New' THEN 1 ELSE 0 END) as new_count,
            SUM(CASE WHEN segment = 'Churn' THEN 1 ELSE 0 END) as churn
        FROM customers
    """)).fetchone()

    # Revenue per bulan (12 bulan terakhir)
    monthly = conn.execute(text("""
        SELECT
            DATE_FORMAT(order_date, '%Y-%m') as month,
            COUNT(*) as orders,
            SUM(net_revenue) as revenue
        FROM orders
        WHERE order_status = 'completed'
          AND order_date >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
        GROUP BY month
        ORDER BY month
    """))

    # Top products — group by product_id
    top_products = conn.execute(text("""
        SELECT
            COALESCE(p.product_name, oi.product_name) as product_name,
            oi.product_category,
            COUNT(*) as order_count,
            SUM(oi.qty_item) as total_qty
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
            AND oi.source_platform = o.source_platform
        LEFT JOIN products p ON oi.product_id = p.product_id
        WHERE o.order_status = 'completed'
          AND oi.product_id IS NOT NULL
          AND oi.product_id NOT IN ('UNMAPPED','UNKNOWN','CGEB_UNKNOWN','CTPWSU_UNKNOWN')
        GROUP BY oi.product_id, p.product_name, oi.product_category
        ORDER BY total_qty DESC
        LIMIT 10
    """))

    # Top provinsi
    top_provinsi = conn.execute(text("""
        SELECT province, COUNT(*) as customers
        FROM customers
        WHERE province IS NOT NULL
        GROUP BY province
        ORDER BY customers DESC
        LIMIT 10
    """))

    return {
        "orders": {
            "total": stats[0],
            "total_revenue": float(stats[1] or 0),
            "avg_order_value": float(stats[2] or 0),
            "total_shipping": float(stats[3] or 0),
            "total_subsidi_ongkir": float(stats[4] or 0),
        },
        "customers": {
            "total": cust[0],
            "loyal": cust[1],
            "returning": cust[2],
            "new": cust[3] or 0,
            "churn": cust[4],
        },
        "monthly_revenue": rows_to_dict(monthly),
        "top_products": rows_to_dict(top_products),
        "top_provinsi": rows_to_dict(top_provinsi),
    }


@app.get("/analytics/revenue")
def revenue_by_period(
    group_by: str = Query("month", description="month/week/day"),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    conn=Depends(get_db)
):
    """Revenue breakdown by period"""
    fmt = {
        "month": "%Y-%m",
        "week": "%Y-%u",
        "day": "%Y-%m-%d"
    }.get(group_by, "%Y-%m")

    where = ["order_status = 'completed'"]
    params = {}
    if date_from:
        where.append("order_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("order_date <= :date_to")
        params["date_to"] = date_to

    result = conn.execute(text(f"""
        SELECT
            DATE_FORMAT(order_date, '{fmt}') as period,
            COUNT(*) as orders,
            SUM(net_revenue) as revenue,
            AVG(net_revenue) as avg_order_value,
            SUM(total_qty) as total_qty
        FROM orders
        WHERE {' AND '.join(where)}
        GROUP BY period
        ORDER BY period
    """), params)

    return {"data": rows_to_dict(result)}

# ════════════════════════════════════════════════════════════════════
# IMPORT DATA BULANAN
# ════════════════════════════════════════════════════════════════════

@app.post("/import/orderonline")
async def import_orderonline(file: UploadFile = File(...)):
    """Import file bulanan OrderOnline (.xlsx)"""
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(status_code=400, detail="File harus .xlsx, .xls, atau .csv")

    contents = await file.read()

    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents), dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal baca file: {str(e)}")

    # Preprocessing — akan di-expand nanti
    required_cols = ['order_id', 'product', 'phone', 'name', 'status',
                     'payment_method', 'net_revenue', 'created_at']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Kolom tidak ditemukan: {missing}"
        )

    return {
        "status": "received",
        "filename": file.filename,
        "rows": len(df),
        "message": "File diterima. Preprocessing akan segera diimplementasikan."
    }


@app.post("/import/mengantar")
async def import_mengantar(file: UploadFile = File(...)):
    """Import file bulanan Mengantar (.xlsx/.csv)"""
    if not file.filename.endswith(('.xlsx', '.xls', '.csv')):
        raise HTTPException(status_code=400, detail="File harus .xlsx, .xls, atau .csv")

    contents = await file.read()

    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents), dtype=str)
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal baca file: {str(e)}")

    return {
        "status": "received",
        "filename": file.filename,
        "rows": len(df),
        "message": "File diterima. Preprocessing akan segera diimplementasikan."
    }
