/*
Reusable macros for data quality checks across the project
*/

-- Macro to calculate data quality score for any table
{% macro calculate_data_quality_score(table_name, key_columns=[]) %}
    
    select
        '{{ table_name }}' as table_name,
        count(*) as total_records,
        
        -- Completeness checks for key columns
        {% for column in key_columns %}
        sum(case when {{ column }} is not null then 1 else 0 end) as non_null_{{ column }},
        round((sum(case when {{ column }} is not null then 1 else 0 end) * 100.0 / count(*)), 2) as completeness_{{ column }}_pct,
        {% endfor %}
        
        -- Overall completeness score
        round((
            {% for column in key_columns %}
            sum(case when {{ column }} is not null then 1 else 0 end) {{ '+' if not loop.last else '' }}
            {% endfor %}
        ) * 100.0 / (count(*) * {{ key_columns | length }}), 2) as overall_completeness_pct,
        
        current_timestamp() as calculated_at
        
    from {{ table_name }}

{% endmacro %}

-- Macro to identify outliers using IQR method
{% macro detect_outliers(column_name, table_name) %}
    
    with quartiles as (
        select
            percentile_cont(0.25) within group (order by {{ column_name }}) as q1,
            percentile_cont(0.75) within group (order by {{ column_name }}) as q3
        from {{ table_name }}
        where {{ column_name }} is not null
    ),
    
    outlier_bounds as (
        select
            q1,
            q3,
            q3 - q1 as iqr,
            q1 - (1.5 * (q3 - q1)) as lower_bound,
            q3 + (1.5 * (q3 - q1)) as upper_bound
        from quartiles
    )
    
    select
        '{{ column_name }}' as column_name,
        count(*) as total_records,
        sum(case when {{ column_name }} < ob.lower_bound or {{ column_name }} > ob.upper_bound then 1 else 0 end) as outlier_count,
        round((sum(case when {{ column_name }} < ob.lower_bound or {{ column_name }} > ob.upper_bound then 1 else 0 end) * 100.0 / count(*)), 2) as outlier_percentage,
        min({{ column_name }}) as min_value,
        max({{ column_name }}) as max_value,
        avg({{ column_name }}) as avg_value,
        ob.q1,
        ob.q3,
        ob.lower_bound,
        ob.upper_bound
    from {{ table_name }}, outlier_bounds ob
    where {{ column_name }} is not null

{% endmacro %}

-- Macro to validate business rules
{% macro validate_business_rules(pickup_dt, dropoff_dt, distance, fare, passenger_count) %}
    
    case
        when {{ pickup_dt }} >= {{ dropoff_dt }} then 'invalid_timestamps'
        when {{ distance }} < 0 then 'negative_distance'
        when {{ fare }} < 0 then 'negative_fare'
        when {{ passenger_count }} <= 0 then 'invalid_passenger_count'
        when {{ distance }} > {{ var('max_trip_distance') }} then 'excessive_distance'
        when {{ fare }} > {{ var('max_fare_amount') }} then 'excessive_fare'
        when datediff('hour', {{ pickup_dt }}, {{ dropoff_dt }}) > {{ var('max_trip_duration_hours') }} then 'excessive_duration'
        when datediff('minute', {{ pickup_dt }}, {{ dropoff_dt }}) < {{ var('min_trip_duration_minutes') }} then 'too_short_duration'
        else 'valid'
    end

{% endmacro %}

-- Macro to create a data quality summary for any model
{% macro create_data_quality_summary(model_name) %}
    
    select
        '{{ model_name }}' as model_name,
        count(*) as total_records,
        sum(case when data_quality_flag = 'valid' then 1 else 0 end) as valid_records,
        round((sum(case when data_quality_flag = 'valid' then 1 else 0 end) * 100.0 / count(*)), 2) as validity_percentage,
        
        -- Count each type of data quality issue
        sum(case when data_quality_flag = 'invalid_timestamps' then 1 else 0 end) as invalid_timestamps,
        sum(case when data_quality_flag = 'negative_distance' then 1 else 0 end) as negative_distance,
        sum(case when data_quality_flag = 'negative_fare' then 1 else 0 end) as negative_fare,
        sum(case when data_quality_flag = 'excessive_distance' then 1 else 0 end) as excessive_distance,
        sum(case when data_quality_flag = 'excessive_fare' then 1 else 0 end) as excessive_fare,
        sum(case when data_quality_flag = 'invalid_passenger_count' then 1 else 0 end) as invalid_passenger_count,
        sum(case when data_quality_flag = 'excessive_duration' then 1 else 0 end) as excessive_duration,
        sum(case when data_quality_flag = 'too_short_duration' then 1 else 0 end) as too_short_duration,
        
        current_timestamp() as analysis_timestamp
        
    from {{ ref(model_name) }}

{% endmacro %}