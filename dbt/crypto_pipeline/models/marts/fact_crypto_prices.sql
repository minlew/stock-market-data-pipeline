select
    coin_id,
    currency,
    price,
    market_cap,
    volume_24h,
    price_change_24h,
    last_updated_at,
    ingested_at
from {{ ref('stg_crypto_prices') }}
