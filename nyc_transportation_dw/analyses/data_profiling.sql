/*
Comprehensive data profiling analysis for NYC Transportation Data Warehouse
Run with: dbt compile --select analysis.data_profiling
Then execute the compiled SQL in Snowflake for detailed data profiling
*/

-- ================================================================
-- 1. OVERALL DATA VOLUME AND COVERAGE
-- ================================================================

-- Total records and date range coverage
select
    'Data Volume Summary' as analysis_section,
    count(*) as total_trip_records,
    count(distinct pickup_date_key) as unique_dates,
    min(pickup_date_key) as earliest_date,
    max(pickup_date_key) as latest_date,
    datediff('day', min(pickup_date_key), max(pickup_date_key)) as date_range_days,
    count(distinct pickup_location_key) as unique_pickup_locations,
    count(distinct dropoff_location_key) as unique_dropoff_locations,
    count(distinct taxi_type_key) as taxi_types,
    round(count(*) / count(distinct pickup_date_key), 0) as avg_trips_per_day
from {{ ref('fact_taxi_trips') }};

-- ================================================================
-- 2. DATA DISTRIBUTION BY TAXI TYPE
-- ================================================================

-- Trip volume and revenue by taxi type
select
    'Taxi Type Distribution' as analysis_section,
    taxi_type_key,
    taxi_type_name,
    count(*) as trip_count,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as trip_percentage,
    sum(total_amount) as total_revenue,
    round(sum(total_amount) * 100.0 / sum(sum(total_amount)) over(), 2) as revenue_percentage,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration
from {{ ref('fact_taxi_trips') }}
group by taxi_type_key, taxi_type_name
order by trip_count desc;

-- ================================================================
-- 3. TEMPORAL PATTERNS
-- ================================================================

-- Monthly trip patterns
select
    'Monthly Patterns' as analysis_section,
    pickup_month,
    count(*) as trips,
    sum(total_amount) as revenue,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    count(distinct pickup_date_key) as active_days
from {{ ref('fact_taxi_trips') }}
group by pickup_month
order by pickup_month;

-- Hourly patterns
select
    'Hourly Patterns' as analysis_section,
    pickup_hour,
    count(*) as trips,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    sum(case when is_rush_hour then 1 else 0 end) as rush_hour_trips
from {{ ref('fact_taxi_trips') }}
group by pickup_hour
order by pickup_hour;

-- Day of week patterns
select
    'Day of Week Patterns' as analysis_section,
    pickup_day_name,
    pickup_day_of_week,
    count(*) as trips,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration
from {{ ref('fact_taxi_trips') }}
group by pickup_day_name, pickup_day_of_week
order by pickup_day_of_week;

-- ================================================================
-- 4. GEOGRAPHIC PATTERNS
-- ================================================================

-- Borough distribution
select
    'Borough Distribution' as analysis_section,
    pickup_borough,
    count(*) as pickup_trips,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as pickup_percentage,
    round(avg(fare_amount), 2) as avg_pickup_fare,
    round(avg(trip_distance), 2) as avg_pickup_distance
from {{ ref('fact_taxi_trips') }}
where pickup_borough is not null
group by pickup_borough
order by pickup_trips desc;

-- ================================================================
-- 5. FINANCIAL ANALYSIS
-- ================================================================

-- Fare amount distribution
select
    'Fare Distribution' as analysis_section,
    fare_category,
    count(*) as trips,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage,
    min(fare_amount) as min_fare,
    max(fare_amount) as max_fare,
    round(avg(fare_amount), 2) as avg_fare,
    round(median(fare_amount), 2) as median_fare
from {{ ref('fact_taxi_trips') }}
group by fare_category
order by min_fare;

-- Tip analysis
select
    'Tip Analysis' as analysis_section,
    case 
        when tip_percentage = 0 then 'No Tip'
        when tip_percentage <= 0.15 then 'Low Tip (≤15%)'
        when tip_percentage <= 0.20 then 'Standard Tip (15-20%)'
        when tip_percentage <= 0.25 then 'Good Tip (20-25%)'
        else 'High Tip (>25%)'
    end as tip_category,
    count(*) as trips,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as percentage,
    round(avg(tip_amount), 2) as avg_tip_amount,
    round(avg(tip_percentage), 4) as avg_tip_percentage
from {{ ref('fact_taxi_trips') }}
where fare_amount > 0
group by tip_category
order by avg_tip_percentage;