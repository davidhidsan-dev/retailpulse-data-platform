{% test not_empty(model) %}
select 1 as empty_model
where not exists (select 1 from {{ model }})
{% endtest %}
