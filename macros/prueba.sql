{% test not_null_singular(model, columns=GET_UDF_PARAM('COLS_FOR_NULL_TEST').split(',')) %}

    SELECT
    {%- for col in columns %}
        {{col}} {%- if not loop.last -%}, {%- endif -%}
    {% endfor %}
    FROM 
        {{ model }}
    WHERE 
    {%- for col in columns %}
        {{col}} IS NULL {% if not loop.last -%} OR {%- endif %}
    {%- endfor -%}


{% endtest %}

{%- macro GET_UDF_PARAM(PARAM) -%}

    {%- set dict = ({
        "COLS_FOR_NULL_TEST":"COL1,COL3",
        "OTHER_PARAM": "OTHER_VALUE"
    })-%}

    {{ dict[PARAM] }}

{%- endmacro -%}