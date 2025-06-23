{% test test_logical_timestamps(model, pickup_column, dropoff_column) %}
    select *
    from {{ model }}
    where {{ pickup_column }} is not null
      and {{ dropoff_column }} is not null
      and {{ pickup_column }} >= {{ dropoff_column }}
{% endtest %}