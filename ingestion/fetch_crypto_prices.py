import os
import logging
from datetime import datetime, timezone
import psycopg2
import requests

from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCY = "usd"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_crypto_prices (
                id SERIAL PRIMARY KEY,
                coin_id TEXT NOT NULL,
                currency TEXT NOT NULL,
                price NUMERIC NOT NULL,
                market_cap NUMERIC,
                volume_24h NUMERIC,
                price_change_24h NUMERIC,
                last_updated_at TIMESTAMP,
                ingested_at TIMESTAMP NOT NULL
            );
            """
        )
    conn.commit()


def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"

    params = {
        "ids": ",".join(COINS),
        "vs_currencies": VS_CURRENCY,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def insert_prices(conn, data):
    ingested_at = datetime.now(timezone.utc)

    rows = []

    for coin_id, values in data.items():
        last_updated_at = datetime.fromtimestamp(
            values.get("last_updated_at"),
            tz=timezone.utc,
        )

        rows.append(
            (
                coin_id,
                VS_CURRENCY,
                values.get(VS_CURRENCY),
                values.get(f"{VS_CURRENCY}_market_cap"),
                values.get(f"{VS_CURRENCY}_24h_vol"),
                values.get(f"{VS_CURRENCY}_24h_change"),
                last_updated_at,
                ingested_at,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO raw_crypto_prices (
                coin_id,
                currency,
                price,
                market_cap,
                volume_24h,
                price_change_24h,
                last_updated_at,
                ingested_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_id, last_updated_at)
            DO NOTHING;
            """,
            rows,
        )

    conn.commit()
    return len(rows)


def main():
    logging.info("Starting crypto price ingestion")

    data = fetch_prices()

    with get_connection() as conn:
        create_table(conn)
        rows_inserted = insert_prices(conn, data)

    logging.info("Inserted %s rows into raw_crypto_prices", rows_inserted)


if __name__ == "__main__":
    main()