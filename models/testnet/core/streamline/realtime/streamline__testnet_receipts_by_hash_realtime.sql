{% set node_secret_path = var("GLOBAL_NODE_SECRET_PATH") %}

{# Set up dbt configuration #}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {"external_table": 'testnet_receipts_by_hash',
        "sql_limit" :"80000",
        "producer_batch_size" :"40000",
        "worker_batch_size" :"40000",
        "async_concurrent_requests" :"20",
        "sql_source" :"{{this.identifier}}"
        }
    ),
    tags = ['streamline_testnet_realtime']
) }}

{# Main query starts here #}
{# Start by invoking LQ for the last hour of blocks #}

WITH numbered_blocks AS (

    SELECT
        block_number_hex,
        block_number,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS row_num
    FROM
        (
            SELECT
                *
            FROM
                {{ ref('streamline__testnet_blocks') }}
            ORDER BY
                block_number DESC
            LIMIT
                2000
        )
), batched_blocks AS (
    SELECT
        block_number_hex,
        block_number,
        100 AS rows_per_batch,
        CEIL(
            row_num / rows_per_batch
        ) AS batch_number,
        MOD(
            row_num - 1,
            rows_per_batch
        ) + 1 AS row_within_batch
    FROM
        numbered_blocks
),
batched_calls AS (
    SELECT
        batch_number,
        ARRAY_AGG(
            utils.udf_json_rpc_call(
                'eth_getBlockByNumber',
                [block_number_hex, false]
            )
        ) AS batch_request
    FROM
        batched_blocks
    GROUP BY
        batch_number
),
rpc_requests AS (
    SELECT
        live.udf_api(
            'POST',
            'https://lb.drpc.org/ogrpc?network=mezo-testnet&dkey={API_KEY}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            batch_request,
            '{{ node_secret_path }}'
        ) AS resp
    FROM
        batched_calls
),
blocks AS (
    SELECT
        utils.udf_hex_to_int(
            VALUE :result :number :: STRING
        ) :: INT AS block_number,
        VALUE :result :transactions AS tx_hashes
    FROM
        rpc_requests,
        LATERAL FLATTEN (
            input => resp :data
        )
),
flat_tx_hashes AS (
    SELECT
        block_number,
        VALUE :: STRING AS tx_hash
    FROM
        blocks,
        LATERAL FLATTEN (
            input => tx_hashes
        )
),
to_do AS (

    SELECT 
        block_number,
        tx_hash
    FROM (
        SELECT
            block_number,
            tx_hash
        FROM
            flat_tx_hashes
        WHERE 1=1
            AND block_number >= (SELECT block_number FROM {{ ref('_testnet_block_lookback') }})

        UNION ALL
        SELECT
            block_number,
            tx_hash
        FROM
            {{ ref('test_gold_testnet__fact_transactions_recent') }}

    )

    EXCEPT

    SELECT
        block_number,
        tx_hash
    FROM
        {{ ref('streamline__testnet_receipts_by_hash_complete') }}
    WHERE 1=1
        AND block_number >= (SELECT block_number FROM {{ ref('_testnet_block_lookback') }})
),
ready_blocks AS (
    SELECT
        block_number,
        tx_hash
    FROM
        to_do

    LIMIT 80000
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

LIMIT 80000
