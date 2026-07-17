-- Scoped to the project's declared data range (2024). Extend the spine bounds
-- if the data scope ever grows to more years (see CLAUDE.md).

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
    ) }}
)

select
    cast(date_day as date)                 as date_day,
    year(date_day)                         as year,
    month(date_day)                        as month,
    day(date_day)                          as day_of_month,
    dayofweek(date_day)                    as day_of_week,
    dayname(date_day)                      as day_name,
    monthname(date_day)                    as month_name,
    dayofweek(date_day) in (0, 6)          as is_weekend
from spine
