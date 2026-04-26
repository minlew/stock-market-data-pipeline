# 📈 Crypto Data Pipeline (Docker + Postgres + dbt)

An end-to-end data pipeline that ingests cryptocurrency data from the CoinGecko API, stores it in PostgreSQL, and transforms it into analytics-ready models using dbt.

---

## 🚀 Tech Stack

* Python (data ingestion)
* PostgreSQL (data warehouse)
* Docker (containerised database)
* dbt (data transformation + testing)
* CoinGecko API (data source)

---

## 🏗️ Architecture

1. **Ingestion (Python)**

   * Fetches crypto prices from CoinGecko
   * Loads raw data into PostgreSQL (`raw_crypto_prices`)
   * Handles duplicates using `ON CONFLICT`

2. **Storage (Postgres via Docker)**

   * Runs locally in a container
   * Persistent storage via Docker volume

3. **Transformation (dbt)**

   * `stg_crypto_prices`: clean staging layer
   * `fact_crypto_prices`: analytics model with:

     * price changes
     * percentage changes
     * latest price flags

4. **Testing (dbt)**

   * Not null checks
   * Uniqueness constraints
   * Accepted values validation

---

## 📂 Project Structure

```
.
├── ingestion/                 # Python ingestion script
├── dbt/crypto_pipeline/       # dbt project
├── docker-compose.yml         # Postgres container
├── .env                       # Local environment variables (ignored)
├── requirements.txt
```

---

## ⚙️ Setup Instructions

### 1. Clone repo

```bash
git clone <your-repo-url>
cd stock-market-data-pipeline
```

---

### 2. Create environment file

Create `.env`:

```
DB_HOST=127.0.0.1
DB_PORT=55432
DB_NAME=crypto_db
DB_USER=crypto_user
DB_PASSWORD=crypto_pass
```

---

### 3. Start Postgres (Docker)

```bash
docker-compose up -d
```

Check:

```bash
docker ps
```

---

### 4. Install dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 5. Run ingestion

```bash
python ingestion/fetch_crypto_prices.py
```

---

### 6. Run dbt models

```bash
cd dbt/crypto_pipeline

dbt run
dbt test
```

---

## 📊 Example Queries

```sql
SELECT *
FROM analytics.fact_crypto_prices
WHERE is_latest_price = true;
```

```sql
SELECT coin_id, price_change_pct_since_previous_ingestion
FROM analytics.fact_crypto_prices
ORDER BY last_updated_at DESC;
```

---

## 🧪 dbt Tests

* `not_null`
* `unique`
* `accepted_values`

Run:

```bash
dbt test
```

---

## 📌 Key Features

* Idempotent ingestion (`ON CONFLICT DO NOTHING`)
* Environment-based configuration (`.env`)
* Dockerised database
* Layered dbt models (staging → marts)
* Data quality testing with dbt

---

## 🚧 Future Improvements

* Add scheduling (Airflow / cron)
* Add dashboards (Power BI / Tableau)
* Implement incremental dbt models
* Improve API retry logic