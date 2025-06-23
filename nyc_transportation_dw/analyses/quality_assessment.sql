/*
Data Quality Assessment Analysis for NYC Transportation Data Warehouse
Comprehensive quality scoring and recommendations
Run with: dbt compile --select analysis.quality_assessment
*/

-- ================================================================
-- 1. OVERALL DATA QUALITY SCORECARD
-- ================================================================

with quality_metrics as (
    select
        count(*) as total_records,
        
        -- Completeness metrics (% of non-null values)
        round((sum(case when pickup_location_key is not null then 1 else 0 end) * 100.0 / count(*)), 2) as pickup_location_completeness,
        round((sum(case when dropoff_location_key is not null then 1 else 0 end) * 100.0 / count(*)), 2) as dropoff_location_completeness,
        round((sum(case when fare_amount is not null then 1 else 0 end) * 100.0 / count(*)), 2) as fare_completeness,
        round((sum(case when trip_distance is not null then 1 else 0 end) * 100.0 / count(*)), 2) as distance_completeness,
        round((sum(case when trip_duration_minutes is not null then 1 else 0 end) * 100.0 / count(*)), 2) as duration_completeness,
        
        -- Validity metrics (% of logically valid values)
        round((sum(case when fare_amount > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as positive_fare_rate,
        round((sum(case when trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as positive_distance_rate,
        round((sum(case when trip_duration_minutes > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as positive_duration_rate,
        round((sum(case when pickup_datetime < dropoff_datetime then 1 else 0 end) * 100.0 / count(*)), 2) as logical_timestamp_rate,
        
        -- Business rule compliance
        round((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as efficiency_compliance_rate,
        round((sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) * 100.0 / count(*)), 2) as high_quality_rate,
        
        -- Consistency metrics
        round((sum(case when pickup_location_key != dropoff_location_key then 1 else 0 end) * 100.0 / count(*)), 2) as different_locations_rate,
        round((sum(case when abs(base_fare_percentage - 100) <= 20 then 1 else 0 end) * 100.0 / count(*)), 2) as fare_consistency_rate
        
    from {{ ref('fact_taxi_trips') }}
)

select
    'Overall Data Quality Scorecard' as assessment_category,
    total_records,
    
    -- Individual dimension scores
    pickup_location_completeness,
    dropoff_location_completeness,
    fare_completeness,
    distance_completeness,
    duration_completeness,
    
    positive_fare_rate,
    positive_distance_rate,
    positive_duration_rate,
    logical_timestamp_rate,
    
    efficiency_compliance_rate,
    high_quality_rate,
    different_locations_rate,
    fare_consistency_rate,
    
    -- Composite scores
    round((pickup_location_completeness + dropoff_location_completeness + fare_completeness + 
           distance_completeness + duration_completeness) / 5, 2) as overall_completeness_score,
    
    round((positive_fare_rate + positive_distance_rate + positive_duration_rate + 
           logical_timestamp_rate) / 4, 2) as overall_validity_score,
    
    round((efficiency_compliance_rate + high_quality_rate + different_locations_rate + 
           fare_consistency_rate) / 4, 2) as overall_consistency_score,
    
    -- Final composite quality score (weighted average)
    round(((pickup_location_completeness + dropoff_location_completeness + fare_completeness + 
            distance_completeness + duration_completeness) / 5 * 0.3) +
          ((positive_fare_rate + positive_distance_rate + positive_duration_rate + 
            logical_timestamp_rate) / 4 * 0.4) +
          ((efficiency_compliance_rate + high_quality_rate + different_locations_rate + 
            fare_consistency_rate) / 4 * 0.3), 2) as final_quality_score
            
from quality_metrics;

-- ================================================================
-- 2. QUALITY ASSESSMENT BY TAXI TYPE
-- ================================================================

select
    'Quality by Taxi Type' as assessment_category,
    taxi_type_key,
    taxi_type_name,
    count(*) as records,
    
    -- Completeness by taxi type
    round((sum(case when fare_amount is not null and fare_amount > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as valid_fare_rate,
    round((sum(case when trip_distance is not null and trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as valid_distance_rate,
    round((sum(case when pickup_datetime < dropoff_datetime then 1 else 0 end) * 100.0 / count(*)), 2) as valid_timestamp_rate,
    
    -- Service quality by taxi type
    round((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as efficiency_rate,
    round((sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) * 100.0 / count(*)), 2) as high_quality_rate,
    round((sum(case when is_likely_satisfied_customer = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as satisfaction_rate,
    
    -- Data richness
    round(avg(case when tip_amount > 0 then 1 else 0 end) * 100, 2) as tip_data_availability,
    round(avg(case when tolls_amount > 0 then 1 else 0 end) * 100, 2) as tolls_data_availability,
    
    -- Composite quality score by taxi type
    round(((sum(case when fare_amount is not null and fare_amount > 0 then 1 else 0 end) * 100.0 / count(*)) * 0.3) +
          ((sum(case when trip_distance is not null and trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)) * 0.3) +
          ((sum(case when pickup_datetime < dropoff_datetime then 1 else 0 end) * 100.0 / count(*)) * 0.4), 2) as taxi_type_quality_score
          
from {{ ref('fact_taxi_trips') }}
group by taxi_type_key, taxi_type_name
order by taxi_type_quality_score desc;

-- ================================================================
-- 3. TEMPORAL QUALITY PATTERNS
-- ================================================================

select
    'Quality by Time Period' as assessment_category,
    pickup_month,
    count(*) as records,
    
    -- Quality metrics by month
    round((sum(case when fare_amount > 0 and trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as basic_validity_rate,
    round((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as efficiency_rate,
    round((sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) * 100.0 / count(*)), 2) as high_quality_rate,
    
    -- Outlier rates
    round((sum(case when fare_amount > 200 then 1 else 0 end) * 100.0 / count(*)), 2) as high_fare_outlier_rate,
    round((sum(case when trip_distance > 50 then 1 else 0 end) * 100.0 / count(*)), 2) as long_distance_outlier_rate,
    round((sum(case when trip_duration_minutes > 120 then 1 else 0 end) * 100.0 / count(*)), 2) as long_duration_outlier_rate,
    
    -- Average quality metrics
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(tip_percentage), 4) as avg_tip_percentage
    
from {{ ref('fact_taxi_trips') }}
group by pickup_month
order by pickup_month;

-- ================================================================
-- 4. GEOGRAPHIC QUALITY ASSESSMENT
-- ================================================================

select
    'Quality by Borough' as assessment_category,
    pickup_borough,
    count(*) as pickup_records,
    
    -- Geographic data quality
    round((sum(case when pickup_location_key is not null then 1 else 0 end) * 100.0 / count(*)), 2) as location_completeness,
    round((sum(case when pickup_zone_name is not null then 1 else 0 end) * 100.0 / count(*)), 2) as zone_name_completeness,
    
    -- Service quality by borough
    round((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as efficiency_rate,
    round((sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) * 100.0 / count(*)), 2) as high_quality_rate,
    round(avg(tip_percentage), 4) as avg_tip_percentage,
    
    -- Financial quality
    round(avg(fare_amount), 2) as avg_fare,
    round(stddev(fare_amount), 2) as fare_variability,
    
    -- Trip characteristics
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration
    
from {{ ref('fact_taxi_trips') }}
where pickup_borough is not null
group by pickup_borough
order by pickup_records desc;

-- ================================================================
-- 5. DATA ANOMALY DETECTION
-- ================================================================

-- Identify potential data quality issues
with anomaly_detection as (
    select
        trip_key,
        taxi_type_name,
        pickup_datetime,
        fare_amount,
        trip_distance,
        trip_duration_minutes,
        avg_speed_mph,
        tip_percentage,
        
        -- Flag potential anomalies
        case
            when fare_amount <= 0 then 'zero_negative_fare'
            when fare_amount > 500 then 'excessive_fare'
            when trip_distance <= 0 then 'zero_negative_distance'
            when trip_distance > 100 then 'excessive_distance'
            when trip_duration_minutes <= 0 then 'zero_negative_duration'
            when trip_duration_minutes > 300 then 'excessive_duration'
            when avg_speed_mph > 80 then 'excessive_speed'
            when avg_speed_mph < 1 and trip_distance > 1 then 'impossible_slow_speed'
            when tip_percentage > 1 then 'excessive_tip'
            when tip_percentage < 0 then 'negative_tip'
            when fare_amount > 0 and trip_distance = 0 then 'fare_no_distance'
            when trip_distance > 0 and fare_amount = 0 then 'distance_no_fare'
            else 'normal'
        end as anomaly_type
        
    from {{ ref('fact_taxi_trips') }}
    where pickup_date_key >= current_date - 30  -- Recent data only
)

select
    'Data Anomaly Summary' as assessment_category,
    anomaly_type,
    count(*) as anomaly_count,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as anomaly_percentage,
    round(avg(fare_amount), 2) as avg_fare_in_anomaly,
    round(avg(trip_distance), 2) as avg_distance_in_anomaly,
    min(pickup_datetime) as first_occurrence,
    max(pickup_datetime) as last_occurrence,
    
    -- Severity classification
    case
        when anomaly_type in ('zero_negative_fare', 'zero_negative_distance', 'zero_negative_duration') then 'CRITICAL'
        when anomaly_type in ('excessive_fare', 'excessive_distance', 'excessive_duration', 'excessive_speed') then 'HIGH'
        when anomaly_type in ('impossible_slow_speed', 'fare_no_distance', 'distance_no_fare') then 'MEDIUM'
        when anomaly_type in ('excessive_tip', 'negative_tip') then 'LOW'
        else 'NORMAL'
    end as severity_level
    
from anomaly_detection
group by anomaly_type
having anomaly_type != 'normal'
order by anomaly_count desc;

-- ================================================================
-- 6. DATA QUALITY RECOMMENDATIONS
-- ================================================================

with quality_summary as (
    select
        count(*) as total_records,
        sum(case when fare_amount <= 0 then 1 else 0 end) as invalid_fare_count,
        sum(case when trip_distance <= 0 then 1 else 0 end) as invalid_distance_count,
        sum(case when pickup_datetime >= dropoff_datetime then 1 else 0 end) as invalid_timestamp_count,
        sum(case when pickup_location_key is null then 1 else 0 end) as missing_pickup_location_count,
        sum(case when fare_amount > 200 then 1 else 0 end) as excessive_fare_count,
        sum(case when trip_distance > 50 then 1 else 0 end) as excessive_distance_count
    from {{ ref('fact_taxi_trips') }}
)

select
    'Data Quality Recommendations' as assessment_category,
    total_records,
    
    -- Issue identification
    invalid_fare_count,
    round(invalid_fare_count * 100.0 / total_records, 2) as invalid_fare_pct,
    invalid_distance_count,
    round(invalid_distance_count * 100.0 / total_records, 2) as invalid_distance_pct,
    invalid_timestamp_count,
    round(invalid_timestamp_count * 100.0 / total_records, 2) as invalid_timestamp_pct,
    missing_pickup_location_count,
    round(missing_pickup_location_count * 100.0 / total_records, 2) as missing_location_pct,
    
    -- Priority recommendations
    case
        when invalid_fare_count * 100.0 / total_records > 5 then 'URGENT: Fix fare amount validation'
        when invalid_distance_count * 100.0 / total_records > 5 then 'URGENT: Fix trip distance validation'
        when invalid_timestamp_count * 100.0 / total_records > 1 then 'HIGH: Fix timestamp logic validation'
        when missing_pickup_location_count * 100.0 / total_records > 2 then 'MEDIUM: Improve location data collection'
        else 'LOW: Monitor data quality metrics regularly'
    end as primary_recommendation,
    
    -- Overall quality assessment
    case
        when (invalid_fare_count + invalid_distance_count + invalid_timestamp_count) * 100.0 / total_records < 1 then 'EXCELLENT'
        when (invalid_fare_count + invalid_distance_count + invalid_timestamp_count) * 100.0 / total_records < 5 then 'GOOD'
        when (invalid_fare_count + invalid_distance_count + invalid_timestamp_count) * 100.0 / total_records < 10 then 'FAIR'
        else 'POOR'
    end as overall_data_quality_rating
    
from quality_summary;

-- ================================================================
-- 7. MONTHLY DATA QUALITY TRENDS
-- ================================================================

select
    'Monthly Quality Trends' as assessment_category,
    pickup_year,
    pickup_month,
    count(*) as monthly_records,
    
    -- Quality trend metrics
    round((sum(case when fare_amount > 0 and trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)), 2) as basic_validity_trend,
    round((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)), 2) as efficiency_trend,
    round((sum(case when pickup_location_key is not null then 1 else 0 end) * 100.0 / count(*)), 2) as location_completeness_trend,
    
    -- Data volume indicators
    round(count(*) * 1.0 / lag(count(*)) over (order by pickup_year, pickup_month) - 1, 2) as month_over_month_growth,
    
    -- Quality score trend
    round(((sum(case when fare_amount > 0 and trip_distance > 0 then 1 else 0 end) * 100.0 / count(*)) * 0.4) +
          ((sum(case when is_efficient_trip = 1 then 1 else 0 end) * 100.0 / count(*)) * 0.3) +
          ((sum(case when pickup_location_key is not null then 1 else 0 end) * 100.0 / count(*)) * 0.3), 2) as monthly_quality_score
          
from {{ ref('fact_taxi_trips') }}
group by pickup_year, pickup_month
order by pickup_year, pickup_month;