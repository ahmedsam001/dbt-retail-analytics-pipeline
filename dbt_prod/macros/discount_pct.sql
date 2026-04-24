{% macro discount_pct(discount_amount, gross_amount) %}

round(
    case
        when {{ gross_amount }} > 0
        then ({{ discount_amount }} / {{ gross_amount }}) * 100
        else 0
    end,
2)

{% endmacro %}