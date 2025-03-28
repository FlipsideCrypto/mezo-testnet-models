{{ config (
    materialized = 'table',
    tags = ['streamline_testnet_complete','chainhead']
) }}

SELECT
    live.udf_api(
        'POST',
        'https://lb.drpc.org/ogrpc?network=mezo-testnet&dkey={API_KEY}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'LiveQuery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        '{{ var('GLOBAL_NODE_SECRET_PATH') }}'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number