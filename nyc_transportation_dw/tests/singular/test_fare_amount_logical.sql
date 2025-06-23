{{ config(severity='warn') }}

with fare_component_validation as (
    select
        trip_key,
        taxi_type_name,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        abs(total_amount - (fare_amount + coalesce(tip_amount, 0) + coalesce(tolls_amount, 0))) as variance,
        case
            when fare_amount <= 0 then 'zero_or_negative_fare'
            when total_amount < fare_amount then 'total_less_than_fare'
            when tip_amount < 0 then 'negative_tip'
            when tolls_amount < 0 then 'negative_tolls'
            when abs(total_amount - (fare_amount + coalesce(tip_amount, 0) + coalesce(tolls_amount, 0))) > 5.00 then 'large_fare_variance'
            when fare_amount > 500 then 'excessive_fare'
            else 'valid'
        end as fare_validation_flag
    from {{ ref('fact_taxi_trips') }}
    where pickup_date_key >= current_date - 30
)

select 
    fare_validation_flag as issue_type,
    count(*) as issue_count,
    avg(fare_amount) as avg_fare,
    avg(variance) as avg_variance,
    case 
        when count(*) > 100 then 'CRITICAL'
        when count(*) > 50 then 'HIGH'  
        when count(*) > 10 then 'MEDIUM'
        else 'LOW'
    end as severity_level
from fare_component_validation
where fare_validation_flag != 'valid'
group by fare_validation_flag
having count(*) > 5