{{ config (
    materialized = "view",
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_testnet__traces') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_testnet_block_lookback') }}
    )
