{% test high_discount_check(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE 
        ({{ column_name }} > 50 AND is_high_discount = false) OR
        ({{ column_name }} <= 50 AND is_high_discount = true)
{% endtest %}