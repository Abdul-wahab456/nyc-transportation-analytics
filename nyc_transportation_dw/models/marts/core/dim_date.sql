{{
  config(
    materialized='table',
    schema='MARTS'
  )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )
    }}
),

date_dimension AS (
    SELECT 
        date_day,
        
        -- Date attributes
        EXTRACT(year FROM date_day) AS year,
        EXTRACT(quarter FROM date_day) AS quarter,
        EXTRACT(month FROM date_day) AS month,
        EXTRACT(day FROM date_day) AS day,
        EXTRACT(week FROM date_day) AS week_of_year,
        EXTRACT(dayofweek FROM date_day) AS day_of_week,
        EXTRACT(dayofyear FROM date_day) AS day_of_year,
        
        -- Month names
        TO_CHAR(date_day, 'MMMM') AS month_name,
        TO_CHAR(date_day, 'MON') AS month_name_short,
        
        -- Day names
        TO_CHAR(date_day, 'DDDD') AS day_name,
        TO_CHAR(date_day, 'DY') AS day_name_short,
        
        -- Quarter strings
        'Q' || EXTRACT(quarter FROM date_day)::VARCHAR AS quarter_name,
        
        -- Year-Month combinations
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        
        -- Week of month (1-5)
        CEIL(EXTRACT(day FROM date_day) / 7.0) AS week_of_month,
        
        -- Season calculation
        CASE 
            WHEN EXTRACT(month FROM date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(month FROM date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(month FROM date_day) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(month FROM date_day) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        
        -- Weekend/Weekday flags
        CASE 
            WHEN EXTRACT(dayofweek FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN EXTRACT(dayofweek FROM date_day) NOT IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekday,
        
        -- First and last day flags
        CASE 
            WHEN date_day = DATE_TRUNC('month', date_day) THEN TRUE 
            ELSE FALSE 
        END AS is_first_day_of_month,
        
        CASE 
            WHEN date_day = LAST_DAY(date_day) THEN TRUE 
            ELSE FALSE 
        END AS is_last_day_of_month,
        
        -- Federal holidays (expanded list for NYC)
        CASE 
            WHEN (EXTRACT(month FROM date_day) = 1 AND EXTRACT(day FROM date_day) = 1) THEN 'New Year''s Day'
            WHEN (EXTRACT(month FROM date_day) = 1 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21) THEN 'Martin Luther King Jr. Day'
            WHEN (EXTRACT(month FROM date_day) = 2 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21) THEN 'Presidents Day'
            WHEN (EXTRACT(month FROM date_day) = 5 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 25 AND 31) THEN 'Memorial Day'
            WHEN (EXTRACT(month FROM date_day) = 7 AND EXTRACT(day FROM date_day) = 4) THEN 'Independence Day'
            WHEN (EXTRACT(month FROM date_day) = 9 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 1 AND 7) THEN 'Labor Day'
            WHEN (EXTRACT(month FROM date_day) = 10 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 8 AND 14) THEN 'Columbus Day'
            WHEN (EXTRACT(month FROM date_day) = 11 AND EXTRACT(day FROM date_day) = 11) THEN 'Veterans Day'
            WHEN (EXTRACT(month FROM date_day) = 11 AND EXTRACT(dayofweek FROM date_day) = 4 
                  AND EXTRACT(day FROM date_day) BETWEEN 22 AND 28) THEN 'Thanksgiving'
            WHEN (EXTRACT(month FROM date_day) = 12 AND EXTRACT(day FROM date_day) = 25) THEN 'Christmas'
            ELSE NULL
        END AS federal_holiday,
        
        -- Federal holiday flag
        CASE 
            WHEN (EXTRACT(month FROM date_day) = 1 AND EXTRACT(day FROM date_day) = 1) 
              OR (EXTRACT(month FROM date_day) = 1 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21)
              OR (EXTRACT(month FROM date_day) = 2 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21)
              OR (EXTRACT(month FROM date_day) = 5 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 25 AND 31)
              OR (EXTRACT(month FROM date_day) = 7 AND EXTRACT(day FROM date_day) = 4)
              OR (EXTRACT(month FROM date_day) = 9 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 1 AND 7)
              OR (EXTRACT(month FROM date_day) = 10 AND EXTRACT(dayofweek FROM date_day) = 1 
                  AND EXTRACT(day FROM date_day) BETWEEN 8 AND 14)
              OR (EXTRACT(month FROM date_day) = 11 AND EXTRACT(day FROM date_day) = 11)
              OR (EXTRACT(month FROM date_day) = 11 AND EXTRACT(dayofweek FROM date_day) = 4 
                  AND EXTRACT(day FROM date_day) BETWEEN 22 AND 28)
              OR (EXTRACT(month FROM date_day) = 12 AND EXTRACT(day FROM date_day) = 25)
            THEN TRUE
            ELSE FALSE
        END AS is_federal_holiday,
        
        -- Business day (weekday and not a federal holiday)
        CASE 
            WHEN EXTRACT(dayofweek FROM date_day) NOT IN (0, 6) 
              AND NOT (
                (EXTRACT(month FROM date_day) = 1 AND EXTRACT(day FROM date_day) = 1) 
                OR (EXTRACT(month FROM date_day) = 1 AND EXTRACT(dayofweek FROM date_day) = 1 
                    AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21)
                OR (EXTRACT(month FROM date_day) = 2 AND EXTRACT(dayofweek FROM date_day) = 1 
                    AND EXTRACT(day FROM date_day) BETWEEN 15 AND 21)
                OR (EXTRACT(month FROM date_day) = 5 AND EXTRACT(dayofweek FROM date_day) = 1 
                    AND EXTRACT(day FROM date_day) BETWEEN 25 AND 31)
                OR (EXTRACT(month FROM date_day) = 7 AND EXTRACT(day FROM date_day) = 4)
                OR (EXTRACT(month FROM date_day) = 9 AND EXTRACT(dayofweek FROM date_day) = 1 
                    AND EXTRACT(day FROM date_day) BETWEEN 1 AND 7)
                OR (EXTRACT(month FROM date_day) = 10 AND EXTRACT(dayofweek FROM date_day) = 1 
                    AND EXTRACT(day FROM date_day) BETWEEN 8 AND 14)
                OR (EXTRACT(month FROM date_day) = 11 AND EXTRACT(day FROM date_day) = 11)
                OR (EXTRACT(month FROM date_day) = 11 AND EXTRACT(dayofweek FROM date_day) = 4 
                    AND EXTRACT(day FROM date_day) BETWEEN 22 AND 28)
                OR (EXTRACT(month FROM date_day) = 12 AND EXTRACT(day FROM date_day) = 25)
              )
            THEN TRUE
            ELSE FALSE
        END AS is_business_day,
        
        -- NYC specific special events (can be expanded)
        CASE 
            WHEN (EXTRACT(month FROM date_day) = 12 AND EXTRACT(day FROM date_day) = 31) THEN 'New Year''s Eve'
            WHEN (EXTRACT(month FROM date_day) = 7 AND EXTRACT(day FROM date_day) = 4) THEN '4th of July Fireworks'
            WHEN (EXTRACT(month FROM date_day) = 11 AND EXTRACT(dayofweek FROM date_day) = 4 
                  AND EXTRACT(day FROM date_day) BETWEEN 22 AND 28) THEN 'Macy''s Thanksgiving Parade'
            WHEN (EXTRACT(month FROM date_day) = 9 AND EXTRACT(day FROM date_day) = 11) THEN '9/11 Memorial'
            ELSE NULL
        END AS special_event,
        
        -- School periods (NYC school calendar approximation)
        CASE 
            WHEN (EXTRACT(month FROM date_day) = 7 OR EXTRACT(month FROM date_day) = 8) THEN 'Summer Break'
            WHEN (EXTRACT(month FROM date_day) = 12 AND EXTRACT(day FROM date_day) > 20) 
              OR (EXTRACT(month FROM date_day) = 1 AND EXTRACT(day FROM date_day) < 8) THEN 'Winter Break'
            WHEN (EXTRACT(month FROM date_day) = 4 AND EXTRACT(day FROM date_day) BETWEEN 1 AND 15) THEN 'Spring Break'
            WHEN EXTRACT(month FROM date_day) IN (9, 10, 11, 1, 2, 3, 4, 5, 6) THEN 'School Year'
            ELSE 'School Year'
        END AS school_period,
        
        -- Relative date calculations
        DATEDIFF('day', date_day, CURRENT_DATE()) AS days_ago,
        DATEDIFF('week', date_day, CURRENT_DATE()) AS weeks_ago,
        DATEDIFF('month', date_day, CURRENT_DATE()) AS months_ago,
        DATEDIFF('year', date_day, CURRENT_DATE()) AS years_ago
        
    FROM date_spine
)

SELECT * FROM date_dimension