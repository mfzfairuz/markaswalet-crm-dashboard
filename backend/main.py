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
    allow_credentials=False,
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

# ── SCHEMA INIT ────────────────────────────────────────────────────────
@app.on_event("startup")
def init_schema():
    """Create audit/log tables kalau belum ada (idempotent)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS lead_track_history (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    lead_id BIGINT NOT NULL,
                    from_track VARCHAR(20),
                    to_track VARCHAR(20),
                    source VARCHAR(40),
                    changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_lead (lead_id),
                    INDEX idx_changed (changed_at),
                    INDEX idx_source (source)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            conn.commit()
    except Exception as e:
        print(f"Schema init warning: {e}")

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
    sort: str = Query("last_order_date", description="Sort column"),
    direction: str = Query("desc", description="asc/desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=5000),
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

    # Sort
    sort_col = sort if sort in ['total_orders','total_revenue','last_order_date','avg_order_value'] else 'last_order_date'
    sort_dir = 'ASC' if direction == 'asc' else 'DESC'

    # Data
    result = conn.execute(
        text(f"""
            SELECT customer_id, name, city, province, segment,
                   total_orders, total_revenue, avg_order_value,
                   last_order_date, first_platform, last_platform,
                   recency_days, first_order_date
            FROM customers
            WHERE {where_sql}
            ORDER BY {sort_col} {sort_dir}
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
    orders_raw = conn.execute(
        text("""
            SELECT o.order_id, o.source_platform, o.order_date,
                   o.order_status, o.payment_method, o.net_revenue,
                   o.shipping_cost, o.shipping_provider, o.shipping_type,
                   o.total_qty, o.receipt_number
            FROM orders o
            WHERE o.customer_id = :id
            ORDER BY o.order_date DESC
        """),
        {"id": customer_id}
    )
    orders_list = rows_to_dict(orders_raw)

    # Ambil items per order
    for order in orders_list:
        items = conn.execute(
            text("""
                SELECT oi.product_name, oi.product_raw, oi.qty_item, oi.product_category
                FROM order_items oi
                WHERE oi.order_id = :oid AND oi.source_platform = :platform
                  AND oi.is_parent_row = 1
                ORDER BY oi.id
            """),
            {"oid": order["order_id"], "platform": order["source_platform"]}
        )
        order["items"] = rows_to_dict(items)

    customer["orders"] = orders_list
    customer["order_count"] = len(orders_list)

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
    limit: int = Query(20, ge=1, le=5000),
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

        # Revenue per bulan — akuisisi vs repeat
    monthly = conn.execute(text("""
        SELECT
            DATE_FORMAT(o.order_date, '%Y-%m') as month,
            COUNT(*) as orders,
            SUM(o.net_revenue) as revenue,
            SUM(CASE WHEN DATE(o.order_date) = DATE(c.first_order_date)
                THEN o.net_revenue ELSE 0 END) as revenue_new,
            SUM(CASE WHEN DATE(o.order_date) > DATE(c.first_order_date)
                THEN o.net_revenue ELSE 0 END) as revenue_repeat
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.order_status = 'completed'
          AND o.order_date >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
        GROUP BY month
        ORDER BY month
    """))


    # Top products — group by product_id
    top_products = conn.execute(text("""
        SELECT
            MAX(COALESCE(p.product_name, oi.product_name)) as product_name,
            MAX(oi.product_category) as product_category,
            COUNT(*) as order_count,
            SUM(oi.qty_item) as total_qty
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
            AND oi.source_platform = o.source_platform
        LEFT JOIN products p ON oi.product_id = p.product_id
        WHERE o.order_status = 'completed'
          AND oi.product_id IS NOT NULL
          AND oi.product_id NOT IN ('UNMAPPED','UNKNOWN','CGEB_UNKNOWN','CTPWSU_UNKNOWN')
        GROUP BY oi.product_id
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
# RFM SEGMENTATION (Recency, Frequency, Monetary)
# ════════════════════════════════════════════════════════════════════
# Skor 1-5 per dimensi (NTILE MySQL 8). Segment label = kombinasi R/F/M
# mengikuti pola standar RFM.

RFM_BASE_CTE = """
    WITH base AS (
        SELECT
            customer_id, name, city, province,
            total_orders,
            COALESCE(total_revenue, 0) AS total_revenue,
            COALESCE(avg_order_value, 0) AS avg_order_value,
            last_order_date,
            CASE
                WHEN last_order_date IS NULL THEN NULL
                ELSE DATEDIFF(CURDATE(), last_order_date)
            END AS recency_days
        FROM customers
        WHERE total_orders > 0 AND last_order_date IS NOT NULL
    ),
    scored AS (
        SELECT
            b.*,
            (6 - NTILE(5) OVER (ORDER BY recency_days ASC))  AS r_score,
            NTILE(5) OVER (ORDER BY total_orders  ASC)       AS f_score,
            NTILE(5) OVER (ORDER BY total_revenue ASC)       AS m_score
        FROM base b
    ),
    labeled AS (
        SELECT s.*,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 4 AND m_score >= 4 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score <= 2                    THEN 'New Customers'
                WHEN r_score >= 3 AND f_score <= 3 AND m_score <= 3   THEN 'Promising'
                WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4   THEN 'Cannot Lose Them'
                WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3   THEN 'At Risk'
                WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2   THEN 'Hibernating'
                WHEN r_score = 1  AND f_score = 1  AND m_score = 1    THEN 'Lost'
                ELSE 'Others'
            END AS rfm_segment
        FROM scored s
    )
"""

RFM_SEGMENT_DESC = {
    "Champions":        "Baru order, sering, nilai besar. Jaga & reward.",
    "Loyal Customers":  "Repeat order konsisten. Tawarkan program loyalitas.",
    "New Customers":    "Baru sekali beli. Edukasi & onboarding.",
    "Promising":        "Transaksi baru, frekuensi masih rendah. Dorong repeat.",
    "Cannot Lose Them": "Dulu VIP, mulai menghilang. Prioritas win-back.",
    "At Risk":          "Dulu aktif, mulai jarang. Kirim reminder/promo.",
    "Hibernating":      "Lama tidak order, value rendah. Coba re-engage murah.",
    "Lost":             "Hampir tidak aktif. Arsipkan atau kampanye massal.",
    "Others":           "Belum masuk kategori di atas.",
}


@app.get("/analytics/rfm")
def rfm_summary(conn=Depends(get_db)):
    """Ringkasan RFM: jumlah customer dan revenue per segment."""
    result = conn.execute(text(f"""
        {RFM_BASE_CTE}
        SELECT
            rfm_segment,
            COUNT(*)                AS customers,
            SUM(total_orders)       AS total_orders,
            SUM(total_revenue)      AS total_revenue,
            AVG(total_revenue)      AS avg_revenue,
            AVG(recency_days)       AS avg_recency_days,
            AVG(total_orders)       AS avg_frequency
        FROM labeled
        GROUP BY rfm_segment
        ORDER BY total_revenue DESC
    """))
    rows = rows_to_dict(result)

    total_customers = sum(r["customers"] for r in rows) or 1
    total_revenue = sum(float(r["total_revenue"] or 0) for r in rows) or 1

    segments = []
    for r in rows:
        revenue = float(r["total_revenue"] or 0)
        segments.append({
            "segment": r["rfm_segment"],
            "description": RFM_SEGMENT_DESC.get(r["rfm_segment"], ""),
            "customers": int(r["customers"]),
            "customers_pct": round(r["customers"] / total_customers * 100, 1),
            "total_orders": int(r["total_orders"] or 0),
            "total_revenue": revenue,
            "revenue_pct": round(revenue / total_revenue * 100, 1),
            "avg_revenue": float(r["avg_revenue"] or 0),
            "avg_recency_days": float(r["avg_recency_days"] or 0),
            "avg_frequency": float(r["avg_frequency"] or 0),
        })

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_customers_scored": total_customers,
        "total_revenue": total_revenue,
        "segments": segments,
    }


@app.get("/analytics/rfm/customers")
def rfm_customers(
    segment: Optional[str] = Query(None, description="Filter segment RFM"),
    search: Optional[str] = Query(None, description="Cari nama / nomor HP"),
    sort: str = Query("total_revenue", description="total_revenue/total_orders/recency_days"),
    direction: str = Query("desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=5000),
    conn=Depends(get_db)
):
    """List customer dengan skor R/F/M + label segment."""
    offset = (page - 1) * limit
    where = ["1=1"]
    params = {}

    if segment:
        where.append("rfm_segment = :segment")
        params["segment"] = segment

    if search:
        phone = normalize_phone(search)
        if phone:
            where.append("customer_id LIKE :phone")
            params["phone"] = f"{phone}%"
        else:
            where.append("name LIKE :name")
            params["name"] = f"%{search}%"

    sort_col = sort if sort in ("total_revenue", "total_orders", "recency_days", "r_score", "f_score", "m_score") else "total_revenue"
    sort_dir = "ASC" if direction == "asc" else "DESC"
    where_sql = " AND ".join(where)

    total = conn.execute(
        text(f"{RFM_BASE_CTE} SELECT COUNT(*) FROM labeled WHERE {where_sql}"),
        params
    ).scalar()

    result = conn.execute(
        text(f"""
            {RFM_BASE_CTE}
            SELECT customer_id, name, city, province,
                   total_orders, total_revenue, avg_order_value,
                   last_order_date, recency_days,
                   r_score, f_score, m_score, rfm_segment
            FROM labeled
            WHERE {where_sql}
            ORDER BY {sort_col} {sort_dir}
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": limit, "offset": offset}
    )

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": -(-total // limit) if total else 0,
        "data": rows_to_dict(result),
    }


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
async def import_mengantar(file: UploadFile = File(...), conn=Depends(get_db)):
    """Import file bulanan Mengantar (.xls HTML table format)"""
    contents = await file.read()
    
    try:
        import io
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents), dtype=str)
        else:
            # Format asli adalah HTML table meski ekstensi .xls
            try:
                dfs = pd.read_html(io.BytesIO(contents))
                df = dfs[0]
            except:
                df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal baca file: {str(e)}")

    df = df.fillna("")

    def clean(val):
        s = str(val).strip().lstrip("'")
        return None if s in ("", "nan", "NaN") else s

    def clean_phone(val):
        s = str(val).strip().lstrip("'").replace(" ", "").replace("-", "")
        if not s or s in ("nan",): return None
        if s.startswith("0"): s = "62" + s[1:]
        if not s.startswith("62"): s = "62" + s
        return s

    def clean_date(val):
        s = str(val).strip()
        if not s or s in ("nan",): return None
        try:
            return pd.to_datetime(s, format="%d-%m-%Y %H:%M", errors="coerce")
        except:
            return None

    def map_status(val):
        s = str(val).upper().strip()
        if "DELIVERED" in s and "UN" not in s: return "completed"
        if s in ("RTS", "CANCELLED", "UNDELIVERED"): return "rts"
        if "CANCEL" in s: return "cancelled"
        return "processing_unpaid"

    def norm_courier(val):
        s = str(val).strip()
        m = {"JT": "J&T", "jt": "J&T", "lion": "Lion Parcel", "Sap": "SAP",
             "sap": "SAP", "iDexpress": "ID Express", "SiCepat": "SiCepat",
             "JNE": "JNE"}
        return m.get(s, s)

    # Load existing customers untuk matching
    cust_rows = conn.execute(text(
        "SELECT phone_raw, customer_id, name FROM customers WHERE phone_raw IS NOT NULL"
    )).fetchall()
    cust_map = {row[0]: (row[1], row[2]) for row in cust_rows}

    # Load existing orders + status (untuk deteksi perubahan, bukan cuma duplikat)
    existing_orders = {r[0]: r[1] for r in conn.execute(
        text("SELECT order_id, order_status FROM orders WHERE source_platform='mengantar'")
    ).fetchall()}

    inserted = 0
    status_updated = 0
    skipped = 0
    leads_synced = 0
    touched_customers = set()
    status_changes = {}  # {(from_status, to_status): count}

    for _, row in df.iterrows():
        order_id = clean(row.get("Order ID", ""))
        if not order_id: continue

        phone = clean_phone(row.get("Customer Phone Number", ""))
        name = clean(row.get("Customer Name", ""))
        province = clean(row.get("Province", ""))
        city = clean(row.get("City", ""))
        courier = norm_courier(row.get("Expedition", ""))
        new_status = map_status(row.get("Last Status", ""))
        order_date = clean_date(row.get("Create Date", ""))
        if hasattr(order_date, "to_pydatetime"): order_date = order_date.to_pydatetime()
        goods = clean(row.get("Goods Description", "")) or ""
        qty = int(pd.to_numeric(str(row.get("Quantity", "1")), errors="coerce") or 1)
        product_value = int(pd.to_numeric(str(row.get("Product Value", "0")), errors="coerce") or 0)
        shipping_fee = int(pd.to_numeric(str(row.get("Shipping Fee", "0")), errors="coerce") or 0)
        cod_val = pd.to_numeric(str(row.get("COD", "0")), errors="coerce") or 0
        payment = "COD" if cod_val > 0 else "Bank Transfer"
        net_revenue = product_value

        # Match ke customer
        customer_id = None
        customer_name = name
        if phone and phone in cust_map:
            customer_id, customer_name = cust_map[phone]
        elif phone:
            # Customer baru
            customer_id = phone
            conn.execute(text("""
                INSERT IGNORE INTO customers
                (customer_id, phone_raw, name, city, province, segment,
                 total_orders, total_revenue, avg_order_value, first_order_date, last_order_date, last_platform)
                VALUES (:cid, :phone, :name, :city, :province, 'New',
                 1, :rev, :rev, :dt, :dt, 'mengantar')
            """), {"cid": customer_id, "phone": phone, "name": name,
                   "city": city, "province": province, "rev": net_revenue, "dt": order_date})
            cust_map[phone] = (customer_id, name)

        if customer_id:
            touched_customers.add(customer_id)

        # Order sudah ada → cek apakah status / data perlu di-update
        if order_id in existing_orders:
            old_status = existing_orders[order_id]
            if old_status == new_status:
                skipped += 1
                continue
            # Status berubah (misal processing_unpaid → completed) → update
            conn.execute(text("""
                UPDATE orders SET
                    order_status = :status,
                    payment_method = :payment,
                    shipping_provider = :courier,
                    net_revenue = :rev,
                    shipping_cost = :ship,
                    total_qty = :qty
                WHERE order_id = :oid AND source_platform = 'mengantar'
            """), {"oid": order_id, "status": new_status, "payment": payment,
                   "courier": courier, "rev": net_revenue, "ship": shipping_fee, "qty": qty})
            existing_orders[order_id] = new_status
            status_updated += 1
            key = (old_status, new_status)
            status_changes[key] = status_changes.get(key, 0) + 1

            # Kalau status berubah jadi completed, sync leads converted juga
            if phone and new_status == "completed":
                result = conn.execute(text(
                    "UPDATE leads SET converted=1, customer_id=:cid WHERE phone=:phone AND converted=0"
                ), {"cid": customer_id, "phone": phone})
                leads_synced += result.rowcount
            continue

        # Order baru → insert
        conn.execute(text("""
            INSERT INTO orders (order_id, source_platform, customer_id, customer_name,
                order_date, order_status, payment_method, shipping_provider,
                net_revenue, shipping_cost, seller_shipping_discount, total_qty)
            VALUES (:oid, 'mengantar', :cid, :cname,
                :dt, :status, :payment, :courier,
                :rev, :ship, 0, :qty)
        """), {"oid": order_id, "cid": customer_id, "cname": customer_name,
               "dt": order_date, "status": new_status, "payment": payment,
               "courier": courier, "rev": net_revenue, "ship": shipping_fee, "qty": qty})

        # Insert order items dari Goods Description
        products = [p.strip() for p in goods.split(",") if p.strip()]
        if not products: products = [goods or "Unknown"]
        for prod in products:
            conn.execute(text("""
                INSERT INTO order_items (order_id, source_platform, product_raw, qty_item)
                VALUES (:oid, 'mengantar', :prod, 1)
            """), {"oid": order_id, "prod": prod})

        # Auto-sync leads converted
        if phone and new_status == "completed":
            result = conn.execute(text(
                "UPDATE leads SET converted=1, customer_id=:cid WHERE phone=:phone AND converted=0"
            ), {"cid": customer_id, "phone": phone})
            leads_synced += result.rowcount

        existing_orders[order_id] = new_status
        inserted += 1

    # Recalc customer stats untuk SEMUA customer yang ter-touch
    # (termasuk yang last_platform-nya orderonline tapi punya order Mengantar)
    if touched_customers:
        ids = list(touched_customers)
        from sqlalchemy import bindparam
        recalc_stmt = text("""
            UPDATE customers c SET
                total_orders = (SELECT COUNT(*) FROM orders
                                WHERE customer_id = c.customer_id AND order_status = 'completed'),
                total_revenue = (SELECT COALESCE(SUM(net_revenue), 0) FROM orders
                                 WHERE customer_id = c.customer_id AND order_status = 'completed'),
                last_order_date = (SELECT MAX(order_date) FROM orders
                                   WHERE customer_id = c.customer_id),
                first_order_date = COALESCE(c.first_order_date,
                    (SELECT MIN(order_date) FROM orders WHERE customer_id = c.customer_id)),
                avg_order_value = CASE
                    WHEN (SELECT COUNT(*) FROM orders
                          WHERE customer_id = c.customer_id AND order_status = 'completed') > 0
                    THEN (SELECT COALESCE(SUM(net_revenue), 0) FROM orders
                          WHERE customer_id = c.customer_id AND order_status = 'completed')
                       / (SELECT COUNT(*) FROM orders
                          WHERE customer_id = c.customer_id AND order_status = 'completed')
                    ELSE 0
                END,
                last_platform = (SELECT source_platform FROM orders
                                 WHERE customer_id = c.customer_id
                                 ORDER BY order_date DESC LIMIT 1)
            WHERE c.customer_id IN :ids
        """).bindparams(bindparam('ids', expanding=True))
        conn.execute(recalc_stmt, {"ids": ids})

        # Update segment untuk customer yang ter-touch
        seg_stmt = text("""
            UPDATE customers SET segment =
                CASE
                    WHEN total_orders >= 4 THEN 'Loyal'
                    WHEN total_orders >= 2 THEN 'Returning'
                    WHEN total_orders = 1 AND DATEDIFF(NOW(), last_order_date) <= 90 THEN 'New'
                    ELSE 'Churn'
                END
            WHERE customer_id IN :ids
        """).bindparams(bindparam('ids', expanding=True))
        conn.execute(seg_stmt, {"ids": ids})

    conn.commit()

    # Auto sync tracks setelah import mengantar — pakai logic kanonis
    try:
        sync_result = _recalc_tracks(conn, source="import_mengantar")
    except Exception as e:
        print(f"Auto sync tracks warning: {e}")
        sync_result = None

    return {
        "status": "success",
        "filename": file.filename,
        "inserted": inserted,
        "status_updated": status_updated,
        "skipped_duplicate": skipped,
        "leads_synced": leads_synced,
        "customers_recalc": len(touched_customers),
        "status_changes": [
            {"from": k[0], "to": k[1], "count": v}
            for k, v in sorted(status_changes.items(), key=lambda x: -x[1])
        ],
        "tracks_recalc": sync_result,
    }



# ─────────────────────────────────────────
# LEADS
# ─────────────────────────────────────────

def _recalc_tracks(conn, source: str = "manual"):
    """
    Hitung ulang kolom `track` untuk SEMUA leads dengan satu logic kanonis.
    Dipakai oleh /leads/sync-tracks, /leads/import, dan /import/mengantar
    supaya tidak ada drift antar endpoint.

    Aturan track (sesuai panduan broadcast):
    - Blacklist                        → Arsip
    - Pernah beli (phone di orders):
        last_order ≤ 90 hari           → T3-Fresh
        last_order ≤ 365 hari          → T3-Lama
        last_order > 365 hari          → T4-Winback
    - Belum pernah beli:
        converted = 1                  → T3-Fresh (match manual)
        usia dari created_at ≤ 14 hari → T1-Akuisisi
        usia dari last_msg ≤ 90 hari   → T2-Nurturing
        selain itu                     → T4-Winback

    Setiap perubahan track diaudit ke tabel `lead_track_history` dengan
    kolom `source` (mis. 'sync', 'import_mengantar', 'import_leads').
    """
    from datetime import datetime
    now = datetime.now()

    buyer_last_order = {}
    for r in conn.execute(text("""
        SELECT c.phone_raw, MAX(o.order_date) AS last_order
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.order_status = 'completed'
          AND c.phone_raw IS NOT NULL
        GROUP BY c.phone_raw
    """)).fetchall():
        buyer_last_order[r[0]] = r[1]

    rows = conn.execute(text("""
        SELECT id, phone, pipeline_status, converted,
               created_at, last_message_at, track
        FROM leads
    """)).fetchall()

    updated = 0
    track_counts = {}
    transitions = {}  # {(from, to): count}
    history_batch = []  # [(lead_id, from, to)]

    for row in rows:
        lid, phone, pipeline, converted, created_at, last_msg, current_track = row
        status = str(pipeline or "").strip()

        if status == "Blacklist":
            track = "Arsip"
        elif phone and phone in buyer_last_order:
            last_order = buyer_last_order[phone]
            days_since_buy = (now - last_order).days if last_order else 999
            if days_since_buy <= 90:
                track = "T3-Fresh"
            elif days_since_buy <= 365:
                track = "T3-Lama"
            else:
                track = "T4-Winback"
        else:
            days_since_created = (now - created_at).days if created_at else 999
            ref = last_msg or created_at
            days_old = (now - ref).days if ref else 999
            if converted and converted == 1:
                track = "T3-Fresh"
            elif days_since_created <= 14:
                track = "T1-Akuisisi"
            elif days_old <= 90:
                track = "T2-Nurturing"
            else:
                track = "T4-Winback"

        if current_track != track:
            conn.execute(text(
                "UPDATE leads SET track = :track WHERE id = :id"
            ), {"track": track, "id": lid})
            updated += 1
            key = (current_track or "(none)", track)
            transitions[key] = transitions.get(key, 0) + 1
            history_batch.append({
                "lead_id": lid,
                "from_track": current_track,
                "to_track": track,
                "source": source,
            })
        track_counts[track] = track_counts.get(track, 0) + 1

        if phone and phone in buyer_last_order:
            conn.execute(text(
                "UPDATE leads SET converted = 1 WHERE id = :id AND converted = 0"
            ), {"id": lid})

    if history_batch:
        try:
            conn.execute(text("""
                INSERT INTO lead_track_history (lead_id, from_track, to_track, source)
                VALUES (:lead_id, :from_track, :to_track, :source)
            """), history_batch)
        except Exception as e:
            print(f"Track history insert warning: {e}")

    conn.commit()

    transitions_list = [
        {"from": k[0], "to": k[1], "count": v}
        for k, v in sorted(transitions.items(), key=lambda x: -x[1])
    ]

    return {
        "processed": len(rows),
        "updated": updated,
        "distribution": track_counts,
        "transitions": transitions_list,
        "buyers_matched": len(buyer_last_order),
        "source": source,
    }


@app.get("/leads")
def list_leads(
    pipeline_status: Optional[str] = None,
    converted: Optional[str] = None,
    search: Optional[str] = None,
    kota: Optional[str] = None,
    label: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    track: Optional[str] = None,
    sort: str = Query("last_message_at", description="Sort column"),
    direction: str = Query("desc", description="asc/desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=5000),
    conn=Depends(get_db)
):
    """List leads dengan filter dan pagination"""
    offset = (page - 1) * limit
    where = ["1=1"]
    params = {}

    if pipeline_status:
        where.append("pipeline_status = :pipeline_status")
        params["pipeline_status"] = pipeline_status
    if converted == "true":
        where.append("l.converted = 1")
    elif converted == "false":
        where.append("l.converted = 0")
    if kota:
        where.append("kota LIKE :kota")
        params["kota"] = f"%{kota}%"
    if label:
        where.append("label_names LIKE :label")
        params["label"] = f"%{label}%"

    if track:
        if track == 'T4-Unpaid':
            where.append("track = 'T4-Winback' AND l.converted = 0")
        elif track == 'T4-Paid':
            where.append("track = 'T4-Winback' AND l.converted = 1")
        else:
            where.append("track = :track")
            params["track"] = track
    if search:
        where.append("(l.name LIKE :search OR l.phone LIKE :search)")
        params["search"] = f"%{search}%"
    if date_from:
        where.append("l.created_at >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("l.created_at <= :date_to")
        params["date_to"] = date_to + " 23:59:59"

    where_sql = " AND ".join(where)
    sort_col = sort if sort in ["last_message_at","created_at","name","pipeline_status"] else "last_message_at"
    sort_dir = "ASC" if direction == "asc" else "DESC"

    total = conn.execute(text(f"SELECT COUNT(*) FROM leads l WHERE {where_sql}"), params).scalar()

    result = conn.execute(text(f"""
        SELECT l.*, c.segment, c.total_orders, c.total_revenue
        FROM leads l
        LEFT JOIN customers c ON l.customer_id = c.customer_id
        WHERE {where_sql}
        ORDER BY l.{sort_col} {sort_dir}
        LIMIT :limit OFFSET :offset
    """), {**params, "limit": limit, "offset": offset})

    return {
        "total": total,
        "page": page,
        "pages": -(-total // limit),
        "data": rows_to_dict(result)
    }


@app.get("/leads/pipeline-stats")
def leads_pipeline_stats(conn=Depends(get_db)):
    """Stats per pipeline status"""
    result = conn.execute(text("""
        SELECT
            pipeline_status,
            COUNT(*) as total,
            SUM(converted) as converted
        FROM leads
        GROUP BY pipeline_status
        ORDER BY total DESC
    """))
    return {"data": rows_to_dict(result)}


# NOTE: Endpoint dengan path statis WAJIB di-register sebelum /leads/{lead_id}
# kalau tidak FastAPI akan match string apapun ke {lead_id} dan return 404.

@app.get("/leads/intake-trend")
def leads_intake_trend(
    weeks: int = Query(12, ge=1, le=52),
    conn=Depends(get_db)
):
    """
    Trend intake leads per minggu (untuk chart dashboard).
    Mingguan = ISO week (Senin-Minggu).
    """
    result = conn.execute(text("""
        SELECT
            YEARWEEK(created_at, 3) AS yearweek,
            DATE(DATE_SUB(created_at, INTERVAL WEEKDAY(created_at) DAY)) AS week_start,
            COUNT(*) AS total_leads,
            SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS converted_count
        FROM leads
        WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL :weeks WEEK)
        GROUP BY yearweek, week_start
        ORDER BY week_start ASC
    """), {"weeks": weeks})

    data = []
    for row in result:
        d = dict(zip(result.keys(), row))
        data.append({
            "yearweek": str(d["yearweek"]),
            "week_start": d["week_start"].isoformat() if d["week_start"] else None,
            "total_leads": int(d["total_leads"] or 0),
            "converted_count": int(d["converted_count"] or 0),
        })

    return {"weeks": weeks, "data": data}


@app.get("/leads/track-history")
def leads_track_history(
    lead_id: Optional[int] = None,
    source: Optional[str] = None,
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(100, ge=1, le=2000),
    conn=Depends(get_db)
):
    """Audit log perpindahan track leads."""
    where = ["changed_at >= DATE_SUB(NOW(), INTERVAL :days DAY)"]
    params = {"days": days, "limit": limit}
    if lead_id:
        where.append("lead_id = :lead_id")
        params["lead_id"] = lead_id
    if source:
        where.append("source = :source")
        params["source"] = source

    result = conn.execute(text(f"""
        SELECT h.id, h.lead_id, l.name, l.phone,
               h.from_track, h.to_track, h.source, h.changed_at
        FROM lead_track_history h
        LEFT JOIN leads l ON h.lead_id = l.id
        WHERE {' AND '.join(where)}
        ORDER BY h.changed_at DESC
        LIMIT :limit
    """), params)

    return {"data": rows_to_dict(result)}


@app.get("/leads/{lead_id}")
def get_lead(lead_id: str, conn=Depends(get_db)):
    """Detail lead by ID"""
    result = conn.execute(
        text("""
            SELECT l.*, c.segment, c.total_orders, c.total_revenue, c.last_order_date
            FROM leads l
            LEFT JOIN customers c ON l.customer_id = c.customer_id
            WHERE l.id = :id
        """),
        {"id": lead_id}
    ).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Lead tidak ditemukan")
    return dict(result._mapping)


@app.put("/leads/{lead_id}")
def update_lead(lead_id: str, body: dict, conn=Depends(get_db)):
    """Update lead — pipeline_status, note, label_names, handled_by"""
    allowed = ["pipeline_status", "note", "label_names", "handled_by_name", "kota", "name", "track", "rumah_walet", "usia_rbw", "ukuran_rbw", "jumlah_sarang", "lantai_rbw", "panen_per_3bulan"]
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Tidak ada field yang valid")

    set_sql = ", ".join([f"{k} = :{k}" for k in updates])
    conn.execute(
        text(f"UPDATE leads SET {set_sql}, updated_at = NOW() WHERE id = :id"),
        {**updates, "id": lead_id}
    )
    conn.commit()
    return {"status": "updated", "id": lead_id}


@app.post("/leads/import")
async def import_leads(file: UploadFile = File(...), conn=Depends(get_db)):
    """Import leads dari file Cekat export (.xlsx)"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal baca file: {str(e)}")

    # Normalize columns
    df = df.fillna("")
    inserted = 0
    updated = 0

    # Load existing customer phones for matching
    cust_rows = conn.execute(text(
        "SELECT phone_raw, customer_id FROM customers WHERE phone_raw IS NOT NULL"
    )).fetchall()
    cust_phones = {row[0]: row[1] for row in cust_rows}

    def clean(val):
        if val is None: return None
        s = str(val).strip()
        return None if s in ("", "nan", "NaN", "None") else s

    def clean_phone(val):
        if val is None: return None
        try:
            s = str(int(float(str(val)))).strip()
            return None if s in ("", "nan") else s
        except: return None

    def clean_dt(val):
        if val is None: return None
        try:
            dt = pd.to_datetime(str(val), errors="coerce", utc=True)
            if pd.isna(dt): return None
            return dt.to_pydatetime().replace(tzinfo=None)
        except: return None

    for _, row in df.iterrows():
        contact_id = clean(row.get("contact_id"))
        if not contact_id:
            continue

        phone = clean_phone(row.get("phone_number"))
        name = clean(row.get("display_name"))
        
        # Match ke customers — pastikan bukan nomor HP
        cust_id = cust_phones.get(phone) if phone else None
        if cust_id and cust_id == phone:
            cust_id = None

        data = {
            "contact_id": contact_id,
            "name": name,
            "phone": phone,
            "pipeline_status": clean(row.get("pipeline_status_name")),
            "stage_status": clean(row.get("stage_status")),
            "label_names": (clean(row.get("label_names")) or "")[:50] or None,
            "handled_by_name": clean(row.get("handled_by_name")),
            "inbox": clean(row.get("inboxes_name")),
            "note": clean(row.get("note")),
            "first_message": (clean(row.get("first_message")) or "")[:500] or None,
            "kota": clean(row.get("additional_kota")),
            "rumah_walet": (clean(row.get("additional_data rumah walet")) or "")[:20] or None,
            "usia_rbw": (clean(row.get("additional_usia rumah walet")) or "")[:20] or None,
            "ukuran_rbw": (clean(row.get("additional_ukuran rumah walet")) or "")[:20] or None,
            "jumlah_sarang": clean(row.get("additional_jumlah sarang (keping)")),
            "lantai_rbw": (clean(row.get("additional_lantai rumah walet")) or "")[:20] or None,
            "panen_per_3bulan": clean(row.get("additional_panen per 3 bulan")),
            "customer_id": cust_id,
            "converted": 1 if cust_id else 0,
            "created_at": clean_dt(row.get("created_at")),
            "last_message_at": clean_dt(row.get("last_message_at")),
        }

        # Upsert by contact_id
        existing = conn.execute(
            text("SELECT id FROM leads WHERE contact_id = :contact_id"),
            {"contact_id": contact_id}
        ).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE leads SET name=:name, phone=:phone, pipeline_status=:pipeline_status,
                stage_status=:stage_status, label_names=:label_names,
                handled_by_name=:handled_by_name, inbox=:inbox, note=:note,
                first_message=:first_message, kota=:kota,
                rumah_walet=:rumah_walet, usia_rbw=:usia_rbw, ukuran_rbw=:ukuran_rbw,
                jumlah_sarang=:jumlah_sarang, lantai_rbw=:lantai_rbw,
                panen_per_3bulan=:panen_per_3bulan,
                customer_id=:customer_id, converted=:converted,
                last_message_at=:last_message_at, updated_at=NOW()
                WHERE contact_id=:contact_id
            """), data)
            updated += 1
        else:
            conn.execute(text("""
                INSERT INTO leads (contact_id, name, phone, pipeline_status, stage_status,
                label_names, handled_by_name, inbox, note, first_message, kota,
                rumah_walet, usia_rbw, ukuran_rbw, jumlah_sarang, lantai_rbw,
                panen_per_3bulan, customer_id, converted, created_at, last_message_at)
                VALUES (:contact_id, :name, :phone, :pipeline_status, :stage_status,
                :label_names, :handled_by_name, :inbox, :note, :first_message, :kota,
                :rumah_walet, :usia_rbw, :ukuran_rbw, :jumlah_sarang, :lantai_rbw,
                :panen_per_3bulan, :customer_id, :converted, :created_at, :last_message_at)
            """), data)
            inserted += 1

    conn.commit()

    # Auto sync tracks setelah import leads — pakai logic kanonis
    try:
        sync_result = _recalc_tracks(conn, source="import_leads")
    except Exception as e:
        print(f"Auto sync tracks warning: {e}")
        sync_result = None

    return {
        "status": "success",
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated,
        "tracks_recalc": sync_result,
    }


@app.post("/leads/sync-tracks")
async def sync_tracks(conn=Depends(get_db)):
    """Hitung ulang track semua leads. Dipanggil manual / cron."""
    result = _recalc_tracks(conn, source="manual_sync")
    return {"status": "success", **result}
