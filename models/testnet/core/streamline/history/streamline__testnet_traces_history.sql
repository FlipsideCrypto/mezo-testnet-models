{% set node_secret_path = var("GLOBAL_NODE_SECRET_PATH") %}

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"testnet_traces",
        "sql_limit" :"2000000",
        "producer_batch_size" :"4000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "async_concurrent_requests" :"1",
        "exploded_key": tojson(["result"]) }
    ),
    tags = ['streamline_testnet_history']
) }}

WITH to_do AS (
    SELECT block_number
    FROM {{ ref("streamline__testnet_blocks") }}
    EXCEPT
    SELECT block_number
    FROM {{ ref("streamline__testnet_traces_complete") }}
),
ready_blocks AS (
    SELECT block_number
    FROM to_do
    where block_number < (select block_number from {{ ref("_testnet_block_lookback") }})
)
SELECT
    block_number,
    ROUND(block_number, -3) AS partition_key,
    live.udf_api(
        'POST',
        'https://lb.drpc.org/ogrpc?network=mezo-testnet&dkey={API_KEY}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'streamline'
        ),
        OBJECT_CONSTRUCT(
            'id', block_number,
            'jsonrpc', '2.0',
            'method', 'debug_traceBlockByNumber',
            'params', ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), OBJECT_CONSTRUCT('tracer', 'callTracer', 'timeout', '120s'))
        ),
        '{{ node_secret_path }}'
    ) AS request
FROM
    ready_blocks
    
ORDER BY block_number desc

LIMIT 
    2000000