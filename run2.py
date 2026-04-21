import pandas as pd
from sqlalchemy import create_engine, text

SOCKET = '/tmp/cloudsql/markaswalet-dashboard:asia-southeast2:markaswalet-db'
engine = create_engine(f'mysql+pymysql://markaswalet_app:AppMW2026!@localhost/markaswalet_crm?unix_socket={SOCKET}')

with engine.connect() as conn:
    conn.execute(text('SET FOREIGN_KEY_CHECKS=0'))
    conn.execute(text('TRUNCATE TABLE order_items'))
    conn.execute(text('TRUNCATE TABLE orders'))
    conn.commit()
print('Cleared')

# Load orders
df = pd.read_csv('master_orders.csv')
df['order_date']     = pd.to_datetime(df['order_date'], errors='coerce')
df['completed_date'] = pd.to_datetime(df['completed_date'], errors='coerce')
for col in ['net_revenue','gross_revenue','shipping_cost','other_cost']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df['is_hpp'] = df['is_hpp'].map({'True':1,'False':0}).fillna(0).astype(int)
# Fix customer_id jadi string bukan float
df['customer_id'] = df['customer_id'].apply(lambda x: str(int(float(x))) if pd.notna(x) and str(x) not in ('','nan') else None)
df = df.where(pd.notna(df), None)

with engine.connect() as conn:
    conn.execute(text('SET FOREIGN_KEY_CHECKS=0'))
    conn.commit()
df.to_sql('orders', engine, if_exists='append', index=False, chunksize=500)
print(f'Orders: {len(df):,}')

# Load order items
df2 = pd.read_csv('master_order_items.csv')
df2['qty_item'] = pd.to_numeric(df2['qty_item'], errors='coerce').fillna(1)
for col in ['is_parent_row','is_hpp']:
    df2[col] = df2[col].map({'True':1,'False':0}).fillna(0).astype(int)
df2 = df2.where(pd.notna(df2), None)
df2.to_sql('order_items', engine, if_exists='append', index=False, chunksize=500)
print(f'Order items: {len(df2):,}')

with engine.connect() as conn:
    conn.execute(text('SET FOREIGN_KEY_CHECKS=1'))
    conn.commit()
    for t in ['products','customers','orders','order_items']:
        c = conn.execute(text(f'SELECT COUNT(*) FROM {t}')).scalar()
        print(f'  {t}: {c:,}')
