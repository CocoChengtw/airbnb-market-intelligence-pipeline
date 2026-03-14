# Airbnb Data Pipeline

A three-layer **Bronze → Silver → Gold** ETL pipeline for Airbnb listing data. Uses Apache Spark and Apache Sedona for geospatial processing, orchestrated by Apache Airflow on a GCP VM, with final output loaded into Snowflake.

---

## Project Structure

```
final_project/
├── scripts/
│   ├── silver_pipeline.py        # Bronze → Silver (Spark + Sedona spatial join)
│   ├── gold_pipeline.py          # Silver → Gold (aggregated analytical tables)
│   └── snowflake_ingest.py       # Gold → Snowflake (pandas write_pandas)
├── dags/
│   └── airbnb_pipeline_dag.py    # Airflow DAG (chains the three jobs)
├── bronze2silver_pipeline.ipynb  # Original development notebook (kept for reference)
├── reddit_collector.py           # Reddit keyword trend collector (standalone script)
└── setup_vm.sh                   # One-time GCP VM setup script
```

---

## Data Architecture

```
/shared/data/
├── bronze/                              # Raw data (read-only)
│   ├── city=los_angeles/
│   │   ├── snapshot_date=2025-03-01/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-01/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_ca_2024/               # Census tract shapefiles (2024)
│   │   ├── shape_ca_2025/               # Census tract shapefiles (2025)
│   │   └── income_la/                   # ACS B19013 median income data
│   ├── city=new_york_city/
│   │   ├── snapshot_date=2025-03-01/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-01/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_nyc_2024/
│   │   ├── shape_nyc_2025/
│   │   └── income_nyc/
│   ├── city=portland/
│   │   ├── snapshot_date=2025-03-03/listings.csv.gz
│   │   ├── snapshot_date=2025-06-17/listings.csv.gz
│   │   ├── snapshot_date=2025-09-06/listings.csv.gz
│   │   ├── snapshot_date=2025-12-04/listings.csv.gz
│   │   ├── shape_oregon_2024/
│   │   ├── shape_oregon_2025/
│   │   └── income_oregon/
│   └── city=san_francisco/
│       ├── snapshot_date=2025-03-01/listings.csv.gz
│       ├── snapshot_date=2025-06-17/listings.csv.gz
│       ├── snapshot_date=2025-09-01/listings.csv.gz
│       ├── snapshot_date=2025-12-04/listings.csv.gz
│       └── income_sf/                   # Uses shape_ca_2025 from city=los_angeles/
├── silver/
│   └── listings_enriched/               # Cleaned + geo-enriched + income-joined (parquet)
│       └── city=*/snapshot_date=*/
└── golden/
    ├── fact_neighborhood_month/         # Neighborhood-level monthly stats
    ├── fact_host_summary/               # Host-level summary per neighborhood
    └── fact_listing_summary/            # Individual listing metrics
```

### Cities and Snapshots

| City | Snapshot Dates |
|------|---------------|
| Los Angeles | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |
| New York City | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |
| Portland | 2025-03-03, 2025-06-17, 2025-09-06, 2025-12-04 |
| San Francisco | 2025-03-01, 2025-06-17, 2025-09-01, 2025-12-04 |

---

## Pipeline Overview

```
silver  ──────────────►  gold  ──────────────►  snowflake_ingest
  Spark + Sedona           PySpark                Python
  CSV → spatial join       Read silver →          Read gold parquet
  + income join            → 3 gold tables        → write_pandas to
  ~30–60 min               ~10 min                  Snowflake ~5 min
```

### Task 1 — `silver`

1. Read `listings.csv.gz` for each city and snapshot date from `/shared/data/bronze`
2. Clean coordinates (cast to double, filter invalid values)
3. Spatial join with Census tract shapefiles using **Apache Sedona** `ST_Contains`
4. Join ACS B19013 median household income by tract GEOID
5. Write partitioned parquet to `/shared/data/silver/listings_enriched` (partitioned by `city` / `snapshot_date`)

### Task 2 — `gold`

Produces three analytical tables from the silver layer:

| Gold Table | Description |
|-----------|-------------|
| `fact_neighborhood_month` | Listing count, review count, avg occupancy, and avg income per neighborhood per month |
| `fact_host_summary` | Host count and avg listings per host per neighborhood |
| `fact_listing_summary` | Full metrics per individual listing (room type, reviews, revenue, etc.) |

### Task 3 — `snowflake_ingest`

Loads the three gold parquet tables into Snowflake using `write_pandas`:

| Snowflake Table | Source |
|----------------|--------|
| `FACT_NEIGHBORHOOD_MONTH` | `/shared/data/golden/fact_neighborhood_month` |
| `FACT_HOST_SUMMARY` | `/shared/data/golden/fact_host_summary` |
| `FACT_LISTING_SUMMARY` | `/shared/data/golden/fact_listing_summary` |

---

## Requirements

| Package | Version |
|---------|---------|
| Python | 3.10+ |
| Apache Spark | 3.5.x |
| Apache Sedona | 1.8.1 |
| Apache Airflow | 2.9.x |
| Java | 17 |
| snowflake-connector-python | latest |

---

## GCP VM Deployment

### 1. Upload the project to the VM

```bash
gcloud compute scp --recurse ./final_project USER@VM_NAME:~/
```

### 2. Run the setup script (one-time only)

```bash
bash ~/final_project/setup_vm.sh
```

The script handles: Java 17 installation, Python package installation, Airflow initialization, admin account creation, setting `dags_folder` to `/shared/final_project/dags`, and starting the scheduler and webserver.

### 3. Open firewall for the Airflow UI

```bash
gcloud compute firewall-rules create allow-airflow \
  --allow tcp:8080 \
  --target-tags airflow-vm
```

### 4. Verify Airflow is running

Open `http://<VM_EXTERNAL_IP>:8080` in your browser. Login with username `admin` and password `project405_team9!` (**change this immediately**).

---

## Running the Pipeline

```bash
# Verify the DAG is loaded
airflow dags list | grep airbnb

# Trigger the pipeline
airflow dags trigger airbnb_pipeline
```

---

## Schedule Configuration

Edit `SCHEDULE` in [dags/airbnb_pipeline_dag.py](dags/airbnb_pipeline_dag.py):

```python
SCHEDULE = None          # Manual trigger only (default)
```
