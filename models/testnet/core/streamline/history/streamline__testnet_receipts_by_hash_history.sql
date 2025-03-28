{% set node_secret_path = var("GLOBAL_NODE_SECRET_PATH") %}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {"external_table": 'testnet_receipts_by_hash',
        "sql_limit" :"2000000",
        "producer_batch_size" :"40000",
        "worker_batch_size" :"40000",
        "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_testnet_history']
) }}


{# Main query starts here #}

WITH 
    last_3_days AS (
        SELECT block_number
        FROM {{ ref("_testnet_block_lookback") }}
    ),

to_do AS (
    SELECT 
        block_number,
        tx_hash
    FROM {{ ref("testnet__fact_transactions") }}
    WHERE 
        (block_number IS NOT NULL 
        AND tx_hash IS NOT NULL)
        AND block_number <= (SELECT block_number FROM last_3_days)

    EXCEPT

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('streamline__testnet_receipts_by_hash_complete') }}
    WHERE 1=1
        AND block_number <= (SELECT block_number FROM last_3_days)
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    WHERE block_number >= (SELECT block_number FROM {{ ref("_testnet_block_lookback") }})

    LIMIT 120000
)
SELECT
    block_number,
    tx_hash,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    live.udf_api(
        'POST',
        'https://lb.drpc.org/ogrpc?network=mezo-testnet&dkey={API_KEY}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'eth_getTransactionReceipt',
            'params', ARRAY_CONSTRUCT(tx_hash)
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks

ORDER BY partition_key DESC, block_number DESC

LIMIT 120000