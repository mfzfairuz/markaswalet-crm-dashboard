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


# ─────────────────────────────────────────
# LEADS
# ─────────────────────────────────────────

@app.get("/leads")
def list_leads(
    pipeline_status: Optional[str] = None,
    converted: Optional[str] = None,
    search: Optional[str] = None,
    kota: Optional[str] = None,
    label: Optional[str] = None,
    sort: str = Query("last_message_at", description="Sort column"),
    direction: str = Query("desc", description="asc/desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
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
        where.append("customer_id IS NOT NULL")
    elif converted == "false":
        where.append("customer_id IS NULL")
    if kota:
        where.append("kota LIKE :kota")
        params["kota"] = f"%{kota}%"
    if label:
        where.append("label_names LIKE :label")
        params["label"] = f"%{label}%"
    if search:
        where.append("(name LIKE :search OR phone LIKE :search)")
        params["search"] = f"%{search}%"

    where_sql = " AND ".join(where)
    sort_col = sort if sort in ["last_message_at","created_at","name","pipeline_status"] else "last_message_at"
    sort_dir = "ASC" if direction == "asc" else "DESC"

    total = conn.execute(text(f"SELECT COUNT(*) FROM leads WHERE {where_sql}"), params).scalar()

    result = conn.execute(text(f"""
        SELECT l.*, c.segment, c.total_orders, c.total_revenue
        FROM leads l
        LEFT JOIN customers c ON l.customer_id = c.customer_id
        WHERE {where_sql}
        ORDER BY l.{sort_col} {sort_dir}
        LIMIT :limit OFFSET :offset
    """), {{**params, "limit": limit, "offset": offset}})

    return {{
        "total": total,
        "page": page,
        "pages": -(-total // limit),
        "data": rows_to_dict(result)
    }}


@app.get("/leads/{{lead_id}}")
def get_lead(lead_id: str, conn=Depends(get_db)):
    """Detail lead by ID"""
    result = conn.execute(
        text("""
            SELECT l.*, c.segment, c.total_orders, c.total_revenue, c.last_order_date
            FROM leads l
            LEFT JOIN customers c ON l.customer_id = c.customer_id
            WHERE l.id = :id
        """),
        {{"id": lead_id}}
    ).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Lead tidak ditemukan")
    return dict(result._mapping)


@app.put("/leads/{{lead_id}}")
def update_lead(lead_id: str, body: dict, conn=Depends(get_db)):
    """Update lead — pipeline_status, note, label_names, handled_by"""
    allowed = ["pipeline_status", "note", "label_names", "handled_by_name", "kota"]
    updates = {{k: v for k, v in body.items() if k in allowed}}
    if not updates:
        raise HTTPException(status_code=400, detail="Tidak ada field yang valid")

    set_sql = ", ".join([f"{k} = :{k}" for k in updates])
    conn.execute(
        text(f"UPDATE leads SET {set_sql}, updated_at = NOW() WHERE id = :id"),
        {{**updates, "id": lead_id}}
    )
    conn.commit()
    return {{"status": "updated", "id": lead_id}}


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
    cust_phones = dict(conn.execute(text(
        "SELECT phone_raw, customer_id FROM customers WHERE phone_raw IS NOT NULL"
    )).fetchall())

    for _, row in df.iterrows():
        contact_id = str(row.get("contact_id", "")).strip()
        if not contact_id:
            continue

        phone = str(row.get("phone_number", "")).strip()
        name = str(row.get("display_name", "")).strip()

        # Match ke customers by phone
        customer_id = cust_phones.get(phone)

        data = {{
            "contact_id": contact_id,
            "name": name,
            "phone": phone,
            "pipeline_status": str(row.get("pipeline_status_name", "")).strip() or None,
            "stage_status": str(row.get("stage_status", "")).strip() or None,
            "label_names": str(row.get("label_names", "")).strip() or None,
            "handled_by_name": str(row.get("handled_by_name", "")).strip() or None,
            "inbox": str(row.get("inboxes_name", "")).strip() or None,
            "note": str(row.get("note", "")).strip() or None,
            "first_message": str(row.get("first_message", "")).strip()[:500] or None,
            "kota": str(row.get("additional_kota", "")).strip() or None,
            "rumah_walet": str(row.get("additional_data rumah walet", "")).strip() or None,
            "usia_rbw": str(row.get("additional_usia rumah walet", "")).strip() or None,
            "ukuran_rbw": str(row.get("additional_ukuran rumah walet", "")).strip() or None,
            "jumlah_sarang": str(row.get("additional_jumlah sarang (keping)", "")).strip() or None,
            "lantai_rbw": str(row.get("additional_lantai rumah walet", "")).strip() or None,
            "panen_per_3bulan": str(row.get("additional_panen per 3 bulan", "")).strip() or None,
            "customer_id": customer_id,
            "converted": 1 if customer_id else 0,
            "created_at": str(row.get("created_at", "")).strip() or None,
            "last_message_at": str(row.get("last_message_at", "")).strip() or None,
        }}

        # Upsert by contact_id
        existing = conn.execute(
            text("SELECT id FROM leads WHERE contact_id = :contact_id"),
            {{"contact_id": contact_id}}
        ).fetchone()

        if existing:
            conn.execute(text("""
                UPDATE leads SET name=:name, phone=:phone, pipeline_status=:pipeline_status,
                stage_status=:stage_status, label_names=:label_names,
                handled_by_name=:handled_by_name, note=:note, kota=:kota,
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
    return {{
        "status": "success",
        "inserted": inserted,
        "updated": updated,
        "total": inserted + updated
    }}


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
    return {{"data": rows_to_dict(result)}}
