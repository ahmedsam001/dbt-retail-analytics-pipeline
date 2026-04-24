{% macro high_discount(discount_amount, gross_amount) %}
    case
        when gross_amount > 0 and (discount_amount / gross_amount) > 0.5 then true
        else false
    end
{% endmacro %}