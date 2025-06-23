{{
  config(
    materialized='table',
    docs={'node_color': 'blue'}
  )
}}

/*
Daily summary report for NYC taxi operations
- Aggregates key metrics by date and taxi type
- Provides day-over-day comparisons
- Supports operational dashboards and executive reporting
*/

with daily_base_metrics as (
    
    select
        pickup_date_key as report_date,
        taxi_type_key,
        taxi_type_name,
        
        -- Volume metrics
        count(*) as total_trips,
        count(distinct vendor_id) as active_vendors,
        sum(passenger_count) as total_passengers,
        
        -- Distance and time metrics
        sum(trip_distance) as total_distance_miles,
        sum(trip_duration_minutes) as total_duration_minutes,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(avg(trip_duration_minutes), 2) as avg_trip_duration,
        round(avg(avg_speed_mph), 2) as avg_speed_mph,
        
        -- Financial metrics
        sum(fare_amount) as total_fare_amount,
        sum(tip_amount) as total_tips,
        sum(tolls_amount) as total_tolls,
        sum(total_amount) as total_revenue,
        round(avg(fare_amount), 2) as avg_fare_amount,
        round(avg(total_amount), 2) as avg_total_amount,
        round(avg(tip_percentage), 4) as avg_tip_percentage,
        
        -- Service quality metrics
        sum(case when is_efficient_trip = 1 then 1 else 0 end) as efficient_trips,
        sum(case when is_likely_satisfied_customer = 1 then 1 else 0 end) as satisfied_customers,
        sum(case when service_quality_rating = 'excellent' then 1 else 0 end) as excellent_service_trips,
        sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) as high_quality_trips,
        
        -- Time-based patterns
        sum(case when is_rush_hour then 1 else 0 end) as rush_hour_trips,
        sum(case when is_weekend then 1 else 0 end) as weekend_trips,
        sum(case when time_period = 'morning' then 1 else 0 end) as morning_trips,
        sum(case when time_period = 'afternoon' then 1 else 0 end) as afternoon_trips,
        sum(case when time_period = 'evening' then 1 else 0 end) as evening_trips,
        sum(case when time_period = 'night' then 1 else 0 end) as night_trips,
        
        -- Geographic patterns
        sum(case when trip_geography_type = 'Inter-Borough' then 1 else 0 end) as inter_borough_trips,
        sum(case when pickup_is_airport or dropoff_is_airport then 1 else 0 end) as airport_trips,
        sum(case when pickup_is_tourism_area or dropoff_is_tourism_area then 1 else 0 end) as tourism_trips,
        
        -- Payment patterns
        sum(case when payment_type = 1 then 1 else 0 end) as credit_card_trips,
        sum(case when payment_type = 2 then 1 else 0 end) as cash_trips,
        
        -- Special events and conditions
        max(case when is_federal_holiday then 1 else 0 end) as is_federal_holiday,
        max(federal_holiday) as federal_holiday_name,
        max(special_event) as special_event_name
        
    from {{ ref('fact_taxi_trips') }}
    group by 
        pickup_date_key, taxi_type_key, taxi_type_name
        
),

daily_with_calculations as (
    
    select
        *,
        
        -- Calculated percentages
        round((efficient_trips * 100.0 / total_trips), 2) as efficiency_rate_pct,
        round((satisfied_customers * 100.0 / total_trips), 2) as satisfaction_rate_pct,
        round((high_quality_trips * 100.0 / total_trips), 2) as quality_rate_pct,
        round((rush_hour_trips * 100.0 / total_trips), 2) as rush_hour_pct,
        round((weekend_trips * 100.0 / total_trips), 2) as weekend_pct,
        round((inter_borough_trips * 100.0 / total_trips), 2) as inter_borough_pct,
        round((airport_trips * 100.0 / total_trips), 2) as airport_trips_pct,
        round((tourism_trips * 100.0 / total_trips), 2) as tourism_trips_pct,
        round((credit_card_trips * 100.0 / total_trips), 2) as credit_card_pct,
        
        -- Revenue efficiency
        case 
            when total_duration_minutes > 0 
            then round((total_revenue * 60.0) / total_duration_minutes, 2)
            else 0
        end as revenue_per_hour,
        
        case 
            when total_distance_miles > 0 
            then round(total_revenue / total_distance_miles, 2)
            else 0
        end as revenue_per_mile,
        
        -- Utilization metrics
        case 
            when total_passengers > 0 
            then round(total_revenue / total_passengers, 2)
            else 0
        end as revenue_per_passenger,
        
        round(total_passengers::float / total_trips, 2) as avg_passengers_per_trip
        
    from daily_base_metrics
    
),

daily_with_comparisons as (
    
    select
        dwc.*,
        
        -- Date context from date dimension
        dd.day_name,
        dd.month_name,
        dd.quarter,
        dd.is_weekend as date_is_weekend,
        dd.is_business_day,
        dd.season,
        dd.week_of_month,
        
        -- Day-over-day comparisons
        lag(total_trips) over (
            partition by taxi_type_key 
            order by report_date
        ) as prev_day_trips,
        
        lag(total_revenue) over (
            partition by taxi_type_key 
            order by report_date
        ) as prev_day_revenue,
        
        lag(avg_fare_amount) over (
            partition by taxi_type_key 
            order by report_date
        ) as prev_day_avg_fare,
        
        -- Week-over-week comparisons
        lag(total_trips, 7) over (
            partition by taxi_type_key 
            order by report_date
        ) as prev_week_trips,
        
        lag(total_revenue, 7) over (
            partition by taxi_type_key 
            order by report_date
        ) as prev_week_revenue,
        
        -- Moving averages (7-day)
        round(avg(total_trips) over (
            partition by taxi_type_key 
            order by report_date 
            rows between 6 preceding and current row
        ), 2) as trips_7day_avg,
        
        round(avg(total_revenue) over (
            partition by taxi_type_key 
            order by report_date 
            rows between 6 preceding and current row
        ), 2) as revenue_7day_avg,
        
        round(avg(avg_fare_amount) over (
            partition by taxi_type_key 
            order by report_date 
            rows between 6 preceding and current row
        ), 2) as fare_7day_avg
        
    from daily_with_calculations dwc
    left join {{ ref('dim_date') }} dd 
        on dwc.report_date = dd.date_day
        
),

final_with_variance as (
    
    select
        report_date,
        taxi_type_key,
        taxi_type_name,
        day_name,
        month_name,
        quarter,
        season,
        is_federal_holiday,
        federal_holiday_name,
        special_event_name,
        is_business_day,
        
        -- Core metrics
        total_trips,
        active_vendors,
        total_passengers,
        total_distance_miles,
        total_duration_minutes,
        total_revenue,
        
        -- Averages
        avg_trip_distance,
        avg_trip_duration,
        avg_speed_mph,
        avg_fare_amount,
        avg_total_amount,
        avg_tip_percentage,
        avg_passengers_per_trip,
        
        -- Efficiency metrics
        efficiency_rate_pct,
        satisfaction_rate_pct,
        quality_rate_pct,
        revenue_per_hour,
        revenue_per_mile,
        revenue_per_passenger,
        
        -- Pattern percentages
        rush_hour_pct,
        weekend_pct,
        inter_borough_pct,
        airport_trips_pct,
        tourism_trips_pct,
        credit_card_pct,
        
        -- Time distribution
        morning_trips,
        afternoon_trips,
        evening_trips,
        night_trips,
        
        -- Comparisons and trends
        prev_day_trips,
        prev_day_revenue,
        prev_week_trips,
        prev_week_revenue,
        trips_7day_avg,
        revenue_7day_avg,
        fare_7day_avg,
        
        -- Variance calculations
        case 
            when prev_day_trips > 0 
            then round(((total_trips - prev_day_trips) * 100.0 / prev_day_trips), 2)
            else null
        end as trips_day_over_day_pct,
        
        case 
            when prev_day_revenue > 0 
            then round(((total_revenue - prev_day_revenue) * 100.0 / prev_day_revenue), 2)
            else null
        end as revenue_day_over_day_pct,
        
        case 
            when prev_week_trips > 0 
            then round(((total_trips - prev_week_trips) * 100.0 / prev_week_trips), 2)
            else null
        end as trips_week_over_week_pct,
        
        case 
            when prev_week_revenue > 0 
            then round(((total_revenue - prev_week_revenue) * 100.0 / prev_week_revenue), 2)
            else null
        end as revenue_week_over_week_pct,
        
        -- Performance vs. average
        case 
            when trips_7day_avg > 0 
            then round(((total_trips - trips_7day_avg) * 100.0 / trips_7day_avg), 2)
            else null
        end as trips_vs_7day_avg_pct,
        
        case 
            when revenue_7day_avg > 0 
            then round(((total_revenue - revenue_7day_avg) * 100.0 / revenue_7day_avg), 2)
            else null
        end as revenue_vs_7day_avg_pct,
        
        -- Data quality
        current_timestamp() as report_generated_at,
        report_date as analysis_date
        
    from daily_with_comparisons
    
)

select * from final_with_variance
order by report_date desc, taxi_type_key