{% macro return_category(return_reason) %}
    case
        when lower(trim(return_reason)) in ('damaged', 'defective', 'broken')
            then 'Product Quality'
        when lower(trim(return_reason)) in ('late delivery', 'wrong item', 'not as described')
            then 'Fulfillment Issue'
        when lower(trim(return_reason)) in ('changed mind', 'no longer needed')
            then 'Customer Decision'
        else 'Other'
    end
{% endmacro %}