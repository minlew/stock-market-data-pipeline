with prices as (

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

),

ranked as (

    select
        *,
        row_number() over (
            partition by coin_id
            order by last_updated_at desc
        ) as latest_price_rank,

        lag(price) over (
            partition by coin_id
            order by last_updated_at
        ) as previous_price,

        price - lag(price) over (
            partition by coin_id
            order by last_updated_at
        ) as price_change_since_previous_ingestion,

        round(
            (
                (price - lag(price) over (
                    partition by coin_id
                    order by last_updated_at
                ))
                / nullif(lag(price) over (
                    partition by coin_id
                    order by last_updated_at
                ), 0)
            ) * 100,
            4
        ) as price_change_pct_since_previous_ingestion

    from prices

)

select
    coin_id,
    currency,
    price,
    previous_price,
    price_change_since_previous_ingestion,
    price_change_pct_since_previous_ingestion,
    market_cap,
    volume_24h,
    price_change_24h,
    last_updated_at,
    ingested_at,
    latest_price_rank,
    case
        when latest_price_rank = 1 then true
        else false
    end as is_latest_price
from ranked