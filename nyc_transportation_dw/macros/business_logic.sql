{% macro categorize_trip_distance(distance_column) %}
    case 
        when {{ distance_column }} <= 1 then 'short'
        when {{ distance_column }} <= 5 then 'medium'
        when {{ distance_column }} <= 15 then 'long'
        else 'very_long'
    end
{% endmacro %}

-- Macro to categorize fare amounts
{% macro categorize_fare_amount(fare_column) %}
    case 
        when {{ fare_column }} <= 10 then 'low'
        when {{ fare_column }} <= 25 then 'medium'
        when {{ fare_column }} <= 50 then 'high'
        else 'premium'
    end
{% endmacro %}

-- Macro to determine time period from hour
{% macro get_time_period(hour_column) %}
    case 
        when {{ hour_column }} between 6 and 11 then 'morning'
        when {{ hour_column }} between 12 and 17 then 'afternoon'
        when {{ hour_column }} between 18 and 21 then 'evening'
        else 'night'
    end
{% endmacro %}

-- Macro to calculate tip percentage safely
{% macro calculate_tip_percentage(tip_amount, fare_amount) %}
    case 
        when {{ fare_amount }} > 0 then {{ tip_amount }} / {{ fare_amount }}
        else 0
    end
{% endmacro %}