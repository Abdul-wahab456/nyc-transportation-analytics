{{
  config(
    materialized='table',
    docs={'node_color': 'red'}
  )
}}

with yellow_taxi_quality as (
    select
        'yellow_taxi' as taxi_type,
        count(*) as total_records,
        sum(case when vendorid is not null then 1 else 0 end) as non_null_vendor,
        sum(case when pickup_datetime is not null then 1 else 0 end) as non_null_pickup,
        sum(case when dropoff_datetime is not null then 1 else 0 end) as non_null_dropoff,
        sum(case when data_quality_flag = 'valid' then 1 else 0 end) as valid_records,
        sum(case when is_valid_trip then 1 else 0 end) as business_valid_trips,
        min(fare_amount) as min_fare,
        max(fare_amount) as max_fare,
        avg(fare_amount) as avg_fare,
        min(pickup_datetime) as earliest_pickup,
        max(pickup_datetime) as latest_pickup
    from {{ ref('stg_yellow_taxi') }}
),

quality_summary as (
    select
        taxi_type,
        total_records,
        round((non_null_vendor * 100.0 / total_records), 2) as completeness_vendor_pct,
        round((non_null_pickup * 100.0 / total_records), 2) as completeness_pickup_pct,
        round((non_null_dropoff * 100.0 / total_records), 2) as completeness_dropoff_pct,
        round((valid_records * 100.0 / total_records), 2) as validity_pct,
        round((business_valid_trips * 100.0 / total_records), 2) as business_validity_pct,
        min_fare,
        max_fare,
        avg_fare,
        earliest_pickup,
        latest_pickup,
        round(((completeness_vendor_pct * 0.3) + (validity_pct * 0.4) + (business_validity_pct * 0.3)), 2) as overall_quality_score,
        case
            when round(((completeness_vendor_pct * 0.3) + (validity_pct * 0.4) + (business_validity_pct * 0.3)), 2) >= 95 then 'A'
            when round(((completeness_vendor_pct * 0.3) + (validity_pct * 0.4) + (business_validity_pct * 0.3)), 2) >= 90 then 'B'
            when round(((completeness_vendor_pct * 0.3) + (validity_pct * 0.4) + (business_validity_pct * 0.3)), 2) >= 80 then 'C'
            when round(((completeness_vendor_pct * 0.3) + (validity_pct * 0.4) + (business_validity_pct * 0.3)), 2) >= 70 then 'D'
            else 'F'
        end as quality_grade,
        current_timestamp() as analysis_timestamp
    from yellow_taxi_quality
)

select * from quality_summary
order by overall_quality_score desc