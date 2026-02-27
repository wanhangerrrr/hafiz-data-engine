# Public Data Platform - Production Grade

Platform data engineering yang modular, scalable, dan production-ready untuk memproses dataset publik.

## Arsitektur Platform

Platform ini mengikuti pola arsitektur ELT (Extract, Load, Transform) modern dengan lapisan berikut:

1.  **Ingestion Layer**: Download file, verifikasi integritas dengan SHA256, dan simpan ke **Raw Storage** (PostgreSQL JSONB). Mendukung *Idempotency* (tidak ada duplikasi file yang sama).
2.  **Staging Layer**: Validasi kualitas data (Data Quality) dan transformasi ke tabel terstruktur. Mendukung mode `FULL` dan `INCREMENTAL`.
3.  **Warehouse Layer**: Data dimodelkan menggunakan *Star Schema* (Fact & Dimension tables) untuk kebutuhan analitik.
4.  **Monitoring**: Mencatat audit trail di `ingestion_log` dan riwayat eksekusi pipeline di `pipeline_run_history`.

## Struktur Folder

```text
public-data-platform/
├── config/             # Konfigurasi YAML (Daftar dataset & load mode)
├── data/               # Penyimpanan file lokal (Raw & Processed)
├── database/           # Schema SQL & modul koneksi terpusat
├── ingestion/          # Logika ingestion & loader raw
├── transforms/         # Transformasi ke Staging & Data Quality
├── warehouse/          # Pemodelan data (Fact & Dimension)
├── logs/               # Log pipeline sistem
├── scripts/            # Script utilitas & test
├── run_pipeline.py     # Entrypoint orchestrator utama
├── .env                # Environment variables (DB Credentials)
└── docker-compose.yml  # PostgreSQL Infrastructure
```

## Setup & Instalasi

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Start Infrastructure**:
    ```bash
    docker compose up -d
    ```
3.  **Initialize Database**:
    Jalankan isi dari `database/schema.sql` di database Anda (Port 5433).

## Cara Menjalankan

Selalu jalankan dari root direktori proyek.

### 1. Menjalankan Seluruh Pipeline (Recommended)
Orchestrator akan menjalankan Ingestion -> Staging -> Warehouse secara berurutan.
```bash
python -m run_pipeline
```

### 2. Menjalankan Layer Secara Terpisah
- **Ingestion**: `python -m ingestion.ingest`
- **Staging**: `python -m transforms.load_staging`
- **Warehouse**: `python -m warehouse.load_warehouse`

## Monitoring
- Periksa `logs/pipeline.log` untuk detail eksekusi skrip.
- Query tabel `pipeline_run_history` untuk melihat durasi dan status setiap run.
- Query tabel `ingestion_log` untuk melihat status setiap file (Success/Skipped).
