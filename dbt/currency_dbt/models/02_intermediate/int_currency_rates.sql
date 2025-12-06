{{ 
  config(
    materialized = 'table'
  ) 
}}

WITH source AS (
                 
        SELECT
            filename,
            loaded_at_utc,
            quotes
        FROM {{ ref('stg_currency_rates') }}

    ),

    -- 1. LVL: explode dates --> every date has a row in the raseult
    date_level as (

        select
            filename,
            loaded_at_utc,
            key   as rate_date,
            value as daily_quotes
        from source,
            json_each(source.quotes)

    ),

    -- 2. LVL: explode currency pair & rate
    -- We have some missing dates in the api response --> daily_quotes = [] --> these rows disappear iduring this step (2nd LVL flattening)
    currency_level as (

        select
            filename,
            loaded_at_utc,
            cast(rate_date as date) as rate_date,
            key   as currency_pair,
            value::double as rate
        from date_level,
            json_each(date_level.daily_quotes)

    )

    select
        filename,
        loaded_at_utc,
        rate_date,
        currency_pair,
        rate
    from currency_level