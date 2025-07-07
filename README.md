# YouTube Trending ELT Pipeline

This project builds an ELT (Extract, Load, Transform) pipeline to process YouTube Trending data using:

- **Python**
- **PySpark**
- **DuckDB**
- **Amazon S3**
- **Airflow** (optional for orchestration)

---

##  Project Structure

.
└── youtube-trending-elt/
    ├── airflow/
    │   ├── logs
    │   └── dags
    ├── data/
    │   ├── raw
    │   └── processed/
    │       └── parquet
    ├── scripts/
    │   ├── extract/
    │   │   └── extract_youtube_data.py
    │   ├── load/
    │   │   └── load_parquet_to_s3.py
    │   └── transform/
    │       ├── transform_to_dw_1.py
    │       └── transform_to_dw_2.py
    ├── sql/
    │   ├── config_dw.py
    │   └── datawarehouse.sql
    ├── datawarehouse.duckdb
    ├── CHANGELOG.md
    └── README.md

---

##  Features

- Download and preprocess trending YouTube data per country
- Normalize data into:
  - `dim_video`, `dim_channel`, `dim_category`, `dim_time`
  - `fact_views`
- Avoid duplicates using `ON CONFLICT DO NOTHING` constraints
- Store final output in **DuckDB** as data warehouse
- File system supports both **local** and **Amazon S3**

---

##  Setup Instructions

### 1. Clone repository

```bash
git clone https://github.com/thangtran1805/Youtube-Trending-ELT.git
cd Youtube-Trending-ELT
```

### 2. Create `.env` file

```dotenv
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_BUCKET_NAME=your_bucket_name
```

### 3. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Run Transform Scripts

```bash
python scripts/transform/transform_to_dw_1.py  # For dim tables
python scripts/transform/transform_to_dw_2.py  # For fact_views
```

---

##  Notes

- Make sure `.duckdb` file is not tracked in Git (`.gitignore` set)
- Data is processed in batches per parquet file
- Designed for scalability and automation

---
