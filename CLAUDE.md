# CLAUDE.md

Panduan untuk Claude Code saat bekerja di repositori ini.

## Gambaran Proyek

**Markaswalet CRM Dashboard** — aplikasi web full-stack untuk akselerasi business development bisnis sarang burung walet. Mengelola customer, order, lead pipeline, analytics, dan import data bulanan dari platform marketplace (OrderOnline & Mengantar).

## Arsitektur

```
┌──────────────────┐      ┌────────────────────┐      ┌──────────────┐
│  Firebase Host   │ ───► │  Cloud Run (API)   │ ───► │  Cloud SQL   │
│  frontend/       │      │  backend/main.py   │      │  MySQL 8     │
│  (static SPA)    │      │  FastAPI + uvicorn │      │  markaswalet │
└──────────────────┘      └────────────────────┘      └──────────────┘
```

- **Frontend**: vanilla HTML/CSS/JS SPA, di-host di Firebase Hosting
- **Backend**: FastAPI (Python 3.11), containerized, di-deploy ke Cloud Run
- **Database**: MySQL 8 di Google Cloud SQL (region `asia-southeast2`, instance `markaswalet-db`)
- **CI/CD**: Cloud Build (`cloudbuild.yaml` + `backend/deploy.sh`)

## Struktur Direktori

```
markaswalet-crm-dashboard/
├── backend/                  # FastAPI REST API (SUMBER KEBENARAN)
│   ├── main.py               # Semua endpoint (~1.068 baris)
│   ├── Dockerfile            # Python 3.11-slim, uvicorn di port 8080
│   ├── deploy.sh             # Deploy ke Cloud Run (asia-southeast2)
│   └── requirements.txt
├── frontend/                 # SPA statis (SUMBER KEBENARAN)
│   ├── index.html            # Dashboard utama (~98 KB, single file)
│   ├── panduan.html          # Halaman panduan pengguna
│   ├── panduan_embed.html    # Versi embed — di-iframe dari index.html
│   ├── 404.html
│   ├── firebase.json         # "public": "." → folder ini di-deploy sebagai root
│   └── .firebaserc           # project: markaswalet-dashboard
├── load_data.py              # Bulk load awal CSV → Cloud SQL (products/customers/orders/order_items)
├── run.py                    # Reload orders + order_items (TRUNCATE dulu)
├── run2.py                   # Varian run.py dengan fix tipe customer_id
├── cloudbuild.yaml           # Trigger GCR build (legacy; deploy aktif via backend/deploy.sh)
└── README.md
```

## Perintah Umum

### Backend — jalanin lokal
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

### Backend — deploy ke Cloud Run
```bash
cd backend
./deploy.sh          # build image + deploy ke Cloud Run
```

### Frontend — deploy ke Firebase Hosting
```bash
cd frontend
firebase deploy --only hosting
```

### Load data awal (dari Cloud Shell)
```bash
python3 load_data.py # butuh CSV: master_customers_v2.csv, master_orders.csv, dll.
```

## Tech Stack

| Layer      | Tools                                                        |
|------------|--------------------------------------------------------------|
| Backend    | FastAPI 0.111, SQLAlchemy 2.0, pandas 2.2, pymysql, uvicorn  |
| Frontend   | Vanilla HTML/CSS/JS, Google Fonts (Syne + DM Sans)           |
| Database   | MySQL 8 (Cloud SQL)                                          |
| Auth       | *belum ada* — endpoint terbuka, CORS `*`                     |
| Deploy     | Docker, Google Cloud Build, Cloud Run, Firebase Hosting      |

## Endpoint API (backend/main.py)

| Method | Path                             | Fungsi                                             |
|--------|----------------------------------|----------------------------------------------------|
| GET    | `/`, `/health`                   | Health check                                       |
| GET    | `/customers`                     | List + filter (segment, province, platform, search)|
| GET    | `/customers/{customer_id}`       | Detail + riwayat order                             |
| GET    | `/customers/phone/{phone}`       | Cari customer by nomor HP (auto-normalisasi)       |
| GET    | `/orders`                        | List order + filter status/platform/tanggal        |
| GET    | `/analytics/summary`             | Summary stats (total customer, revenue, dll.)      |
| GET    | `/analytics/revenue`             | Revenue breakdown per periode                      |
| GET    | `/analytics/rfm`                 | Ringkasan segmen RFM (counts, revenue per segment) |
| GET    | `/analytics/rfm/customers`       | List customer + skor R/F/M (filter segment/search) |
| POST   | `/import/orderonline`            | Upload data bulanan OrderOnline (Excel)            |
| POST   | `/import/mengantar`              | Upload data bulanan Mengantar (Excel)              |
| GET    | `/leads`                         | List leads dengan pipeline status                  |
| GET    | `/leads/pipeline-stats`          | Stats per pipeline stage                           |
| GET    | `/leads/intake-trend`            | Trend intake leads mingguan (chart dashboard)      |
| GET    | `/leads/track-history`           | Audit log perpindahan track                        |
| GET    | `/leads/{lead_id}`               | Detail lead                                        |
| PUT    | `/leads/{lead_id}`               | Update status/catatan/konversi ke customer         |
| POST   | `/leads/import`                  | Import leads (dari Cekat CRM)                      |
| POST   | `/leads/sync-tracks`             | Recompute kolom `track` semua leads (manual/cron)  |

## Data Model (MySQL)

- **customers** — PK `customer_id` berbasis nomor HP ternormalisasi (+62...). Kolom: `name`, `segment` (New/Returning/Loyal/Churn), `total_orders`, `total_revenue`, `avg_order_value`, `last_order_date`, `province`, `city`, `first_platform`, `last_platform`
- **orders** — `order_id`, `customer_id` (FK), `source_platform` (orderonline|mengantar), `order_date`, `order_status`, `payment_method`, `net_revenue`, `shipping_cost`, `total_qty`, `receipt_number`
- **order_items** — `order_id`, `source_platform`, `product_id`, `product_name`, `product_category`, `qty_item`, `is_parent_row`
- **products** — `product_id`, `product_name`
- **leads** — `id`, `contact_id` (Cekat), `name`, `phone`, `pipeline_status`, `converted`, `customer_id` (FK), `label_names`, `note`, `kota`, `track` (T1-Akuisisi/T2-Nurturing/T3-Fresh/T3-Lama/T4-Winback/Arsip), plus field domain walet: `rumah_walet`, `usia_rbw`, `ukuran_rbw`, `jumlah_sarang`, `lantai_rbw`, `panen_per_3bulan`
- **lead_track_history** — `id`, `lead_id`, `from_track`, `to_track`, `source` (sync/import_mengantar/import_leads/manual_sync), `changed_at`. Auto-created di startup app.

## Konvensi & Gotchas

### Normalisasi nomor HP (WAJIB)
Semua lookup customer pakai fungsi `normalize_phone()` di `backend/main.py:55`:
- `08xxx`  → `628xxx`
- `8xxx`   → `628xxx`
- Non-digit dibuang, panjang max 13 char, harus diawali `62`

Jangan bikin query `WHERE phone = ...` dengan raw input — selalu normalisasi dulu.

### Konfigurasi Database
Kredensial dari env var (default di-hardcode di `backend/main.py:34-38` untuk dev). **Jangan commit kredensial baru**. Di produksi di-set via Cloud Run env vars di `deploy.sh`.

Koneksi:
- **Cloud Run** → Unix socket (`DB_SOCKET=/cloudsql/<instance>`)
- **Lokal** → TCP ke `DB_HOST` (IP Cloud SQL)

### Frontend SPA
`frontend/index.html` adalah **single file ~98 KB** — HTML, CSS, dan JS semua inline. Routing halaman pakai `data-page` + div `.page` dengan class toggling. Halaman yang ada:
- `page-dashboard`, `page-customers`, `page-orders`, `page-import`, `page-leads`, `page-panduan`, `page-lookup`, `page-export`, `page-importleads`

Saat edit, **hati-hati dengan ukuran file** — kalau nambah fitur besar, pertimbangkan pecah jadi JS terpisah.

`panduan_embed.html` di-iframe dari `page-panduan` (lihat baris `<iframe src="/panduan_embed.html">` di `frontend/index.html:660`). Jangan hapus file ini.

### CORS
Dibuka `*` untuk semua origin (dev-friendly). Kalau mau produksi ketat, whitelist domain Firebase di `backend/main.py:27`.

### Data Import
Endpoint `/import/orderonline` dan `/import/mengantar` menerima upload Excel bulanan. Skeleton sudah ada di `backend/main.py:453` dan `:489`. Preprocessing masih TODO — hati-hati kalau refactor.

### Script Utilitas Data
- `load_data.py` — **destruktif** (insert ke tabel kosong), hanya untuk initial setup
- `run.py`, `run2.py` — **destruktif** (TRUNCATE + reload orders), hanya jalan kalau disengaja

Jangan jalankan otomatis dari Claude Code — selalu konfirmasi ke user.

## Branch & Git

- Branch utama: `main`
- Branch feature Claude: `claude/learn-code-structure-ZnQtT` (atau sesuai task)
- Deploy otomatis: trigger via push (Cloud Build)

## Task yang Perlu Konfirmasi User

Ini perlu konfirmasi eksplisit sebelum eksekusi:
1. Jalankan `load_data.py`, `run.py`, `run2.py` (destruktif)
2. `firebase deploy` atau `./deploy.sh` (affect produksi)
3. Perubahan schema DB / migrasi
4. Hapus file yang tidak jelas statusnya

## Hal Yang Tidak Ada (Roadmap)

- Auth & RBAC
- Test suite (unit/integration)
- CLAUDE.md sebelumnya (file ini baru ditambahkan)
- Logging terstruktur
- Rate limiting
- Backup otomatis
