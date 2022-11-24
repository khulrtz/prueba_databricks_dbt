{{
    config(
        materialized='incremental',
            )
}}


SELECT CURRENT_TIMESTAMP, '{{target.name}}'