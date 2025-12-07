-- 1️. Filter only trading pairs & USDSEK
with filtered as (

    select
        rate_date,
        loaded_at_utc,
        currency_pair,
        rate as rate_usd
    from {{ ref('int_currency_rates') }}
    where currency_pair in (
        'USDCAD', 'USDEUR', 'USDGBP', 'USDCHF',
        'USDDKK', 'USDNOK', 'USDHUF', 'USDSEK'
    )

),

-- 2. USDSEK sparated (to conversion)
usdsek as (

    select
        rate_date,
        rate_usd as usdsek_rate
    from filtered
    where currency_pair = 'USDSEK'

),

-- 3. convert into SEK & reciproc calculation
base as (

    select
        f.rate_date,
        f.loaded_at_utc,
        f.currency_pair,
        f.rate_usd,
        (u.usdsek_rate / f.rate_usd) as rate_sek
    from filtered f
    left join usdsek u
        on f.rate_date = u.rate_date

),

-- 4. Rolling max/min + max drawdown 1y,3y,5y,10y (with calendar dates)
metrics as (

    select
        *,

        -- ===== 1Y =====
        max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '365 days' preceding and current row
        ) as rolling_max_1y,

        min(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '365 days' preceding and current row
        ) as rolling_min_1y,

        (rate_sek /
         max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '365 days' preceding and current row
         )) - 1 as drawdown_1y,

        -- ===== 3Y =====
        max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1095 days' preceding and current row
        ) as rolling_max_3y,

        min(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1095 days' preceding and current row
        ) as rolling_min_3y,

        (rate_sek /
         max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1095 days' preceding and current row
         )) - 1 as drawdown_3y,

        -- ===== 5Y =====
        max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1825 days' preceding and current row
        ) as rolling_max_5y,

        min(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1825 days' preceding and current row
        ) as rolling_min_5y,

        (rate_sek /
         max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '1825 days' preceding and current row
         )) - 1 as drawdown_5y,

        -- ===== 10Y =====
        max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '3650 days' preceding and current row
        ) as rolling_max_10y,

        min(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '3650 days' preceding and current row
        ) as rolling_min_10y,

        (rate_sek /
         max(rate_sek) over (
            partition by currency_pair
            order by rate_date
            range between interval '3650 days' preceding and current row
         )) - 1 as drawdown_10y

    from base
),

-- Final mart table
final as (
    select
        rate_date,
        loaded_at_utc,
        case
            when currency_pair = 'USDSEK' then 'USD'
            else right(currency_pair, 3)
        end as currency,

        case
            when currency_pair = 'USDSEK' then rate_usd
            else rate_sek
        end as rate,

        rolling_max_1y,
        rolling_min_1y,
        drawdown_1y,

        rolling_max_3y,
        rolling_min_3y,
        drawdown_3y,

        rolling_max_5y,
        rolling_min_5y,
        drawdown_5y,

        rolling_max_10y,
        rolling_min_10y,
        drawdown_10y
    from metrics
)

select
    {{ dbt_utils.generate_surrogate_key([
            "cast(rate_date as varchar)",
            "currency"
        ]) }} as fx_rate_sk,
    *
from final
order by rate_date, currency
