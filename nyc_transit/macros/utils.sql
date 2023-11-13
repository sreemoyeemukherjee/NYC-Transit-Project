{% macro flag_to_bool(column_name, true_value="Y", false_value="N", null_value=" ") -%}
(case
    when {{column_name}} = '{{true_value}}' then true
    when {{column_name}} = '{{false_value}}' then false
    when {{column_name}} = '{{null_value}}' then false  -- updated nulls to false to have all bool values in column
    when {{column_name}} is null then false -- updated nulls to false to have all bool values in column
    else {{column_name}}
end)::bool
{%- endmacro %}