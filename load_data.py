"""
MARKASWALET — Load Data ke Cloud SQL
=====================================
Script ini load data dari CSV ke database markaswalet_crm.

Urutan load:
1. products  (tidak ada foreign key dependency)
2. customers (tidak ada foreign key dependency)
3. orders    (FK ke customers)
4. order_items (FK ke products)

Jalankan dari Cloud Shell:
python3 load_data.py
"""

import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, text
import numpy as np
import os

# ── CONFIG ───────────────────────────────────────────────────────────
DB_HOST     = '34.50.98.6'       # Primary Address dari Cloud SQL
DB_USER     = 'markaswalet_app'
DB_PASS     = 'AppMW2026!'
DB_NAME     = 'markaswalet_crm'
DB_PORT     = 3306

# Path CSV (sudah diupload ke Cloud Shell)
PATH_CUSTOMERS   = 'master_customers_v2.csv'
PATH_ORDERS      = 'master_orders.csv'
PATH_ORDER_ITEMS = 'master_order_items.csv'
PATH_PRODUCTS    = 'Product_Database_2025.csv'  # export dari xlsx

# ── Connect ───────────────────────────────────────────────────────────
print('Connecting to Cloud SQL...')
engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASS}@localhost/{DB_NAME}?unix_socket=/tmp/cloudsql/markaswalet-dashboard:asia-southeast2:markaswalet-db',
    connect_args={'connect_timeout': 30}
)

def test_connection():
    with engine.connect() as conn:
        result = conn.execute(text('SELECT 1'))
        print('✅ Connected to markaswalet_crm!')

test_connection()

def clean_df(df):
    """Replace NaN dengan None untuk MySQL compatibility"""
    return df.where(pd.notna(df), None)

# ════════════════════════════════════════════════════════════════════
# 1. LOAD PRODUCTS
# ════════════════════════════════════════════════════════════════════
print()
print('Loading products...')

try:
    df_prod = pd.read_excel('Product_Database_2025.xlsx')
    
    # Clean price columns (ada format Rp277,065)
    for col in ['product_price', 'product_cost']:
        df_prod[col] = df_prod[col].astype(str)\
            .str.replace('Rp','').str.replace(',','').str.strip()
        df_prod[col] = pd.to_numeric(df_prod[col], errors='coerce')
    
    # Hanya ambil kolom yang ada di schema
    cols = ['product_id','product_category','product_subcategory',
            'product_name','product_variant','product_catalogue',
            'product_price','product_cost','product_weight']
    df_prod = df_prod[cols].dropna(subset=['product_id'])
    df_prod = clean_df(df_prod)
    
    # Tambah peralatan cuci (kode baru)
    new_prod = pd.DataFrame([{
        'product_id':          'EDEQPC01',
        'product_category':    'Education',
        'product_subcategory': 'Equipment',
        'product_name':        'Peralatan Cuci Sarang Walet',
        'product_variant':     None,
        'product_catalogue':   None,
        'product_price':       None,
        'product_cost':        None,
        'product_weight':      None,
    }])
    df_prod = pd.concat([df_prod, new_prod], ignore_index=True)
    
    df_prod.to_sql('products', engine, if_exists='append',
                   index=False, chunksize=100)
    print(f'  ✅ Products loaded: {len(df_prod):,} rows')

except Exception as e:
    print(f'  ❌ Products error: {e}')

# ════════════════════════════════════════════════════════════════════
# 2. LOAD CUSTOMERS
# ════════════════════════════════════════════════════════════════════
print()
print('Loading customers...')

try:
    df_cust = pd.read_csv(PATH_CUSTOMERS, dtype=str)
    
    # Kolom sesuai schema
    cols = ['customer_id','name','phone_raw','address','subdistrict',
            'city','province','all_provinces','all_cities',
            'first_order_date','last_order_date',
            'recency_days','recency_months','tenure_days',
            'total_orders','total_revenue','avg_order_value','total_qty',
            'first_platform','last_platform','platforms_used','segment']
    
    # Hanya ambil kolom yang ada
    available = [c for c in cols if c in df_cust.columns]
    df_cust = df_cust[available].copy()
    
    # Convert numeric
    for col in ['recency_days','tenure_days','total_orders']:
        if col in df_cust.columns:
            df_cust[col] = pd.to_numeric(df_cust[col], errors='coerce')
    for col in ['recency_months','total_revenue','avg_order_value','total_qty']:
        if col in df_cust.columns:
            df_cust[col] = pd.to_numeric(df_cust[col], errors='coerce')
    
    # Datetime
    for col in ['first_order_date','last_order_date']:
        if col in df_cust.columns:
            df_cust[col] = pd.to_datetime(df_cust[col], errors='coerce')
    
    # Segment validation
    valid_segments = ['New','Returning','Loyal','Churn','Unknown']
    if 'segment' in df_cust.columns:
        df_cust['segment'] = df_cust['segment'].where(
            df_cust['segment'].isin(valid_segments), 'Unknown')
    
    df_cust = clean_df(df_cust)
    df_cust.to_sql('customers', engine, if_exists='append',
                   index=False, chunksize=500)
    print(f'  ✅ Customers loaded: {len(df_cust):,} rows')

except Exception as e:
    print(f'  ❌ Customers error: {e}')

# ════════════════════════════════════════════════════════════════════
# 3. LOAD ORDERS
# ════════════════════════════════════════════════════════════════════
print()
print('Loading orders...')

try:
    df_ord = pd.read_csv(PATH_ORDERS, dtype=str)
    
    # Datetime
    for col in ['order_date','completed_date']:
        df_ord[col] = pd.to_datetime(df_ord[col], errors='coerce')
    
    # Numeric
    for col in ['net_revenue','gross_revenue','shipping_cost','other_cost']:
        df_ord[col] = pd.to_numeric(df_ord[col], errors='coerce')
    
    # Boolean
    df_ord['is_hpp'] = df_ord['is_hpp'].map(
        {'True':1,'False':0,'1':1,'0':0}).fillna(0).astype(int)
    
    # Kolom sesuai schema
    cols = ['order_id','source_platform','source_month',
            'order_date','completed_date','customer_id','customer_name',
            'order_status','payment_status','payment_method',
            'net_revenue','gross_revenue','shipping_cost','other_cost',
            'courier','receipt_number','utm_source','utm_campaign',
            'handled_by','is_hpp']
    available = [c for c in cols if c in df_ord.columns]
    df_ord = df_ord[available].copy()
    df_ord = clean_df(df_ord)
    
    df_ord.to_sql('orders', engine, if_exists='append',
                  index=False, chunksize=500)
    print(f'  ✅ Orders loaded: {len(df_ord):,} rows')

except Exception as e:
    print(f'  ❌ Orders error: {e}')

# ════════════════════════════════════════════════════════════════════
# 4. LOAD ORDER ITEMS
# ════════════════════════════════════════════════════════════════════
print()
print('Loading order items...')

try:
    df_items = pd.read_csv(PATH_ORDER_ITEMS, dtype=str)
    
    # Numeric
    df_items['qty_item'] = pd.to_numeric(df_items['qty_item'], errors='coerce').fillna(1)
    
    # Boolean
    for col in ['is_parent_row','is_hpp']:
        df_items[col] = df_items[col].map(
            {'True':1,'False':0,'1':1,'0':0}).fillna(0).astype(int)
    
    # Kolom sesuai schema
    cols = ['order_id','source_platform','product_raw','product_id',
            'product_name','product_category','qty_item',
            'is_parent_row','variation_raw','mapping_notes','is_hpp']
    available = [c for c in cols if c in df_items.columns]
    df_items = df_items[available].copy()
    
    # Set product_id = None kalau tidak ada di products table
    # (agar tidak violate FK constraint)
    with engine.connect() as conn:
        valid_pids = pd.read_sql('SELECT product_id FROM products', conn)
        valid_set  = set(valid_pids['product_id'].tolist())
    
    df_items['product_id'] = df_items['product_id'].where(
        df_items['product_id'].isin(valid_set), None)
    
    df_items = clean_df(df_items)
    df_items.to_sql('order_items', engine, if_exists='append',
                    index=False, chunksize=500)
    print(f'  ✅ Order items loaded: {len(df_items):,} rows')

except Exception as e:
    print(f'  ❌ Order items error: {e}')

# ════════════════════════════════════════════════════════════════════
# VERIFY
# ════════════════════════════════════════════════════════════════════
print()
print('='*50)
print('VERIFICATION')
print('='*50)

with engine.connect() as conn:
    for tbl in ['products','customers','orders','order_items','users']:
        count = conn.execute(text(f'SELECT COUNT(*) FROM {tbl}')).scalar()
        print(f'  {tbl:<15}: {count:,} rows')

print()
print('✅ Data load selesai!')
