-- depends_on: {{ ref('bronze_testnet__receipts_by_hash') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "tx_hash",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(tx_hash)",
    tags = ['silver_testnet']
) }}

WITH bronze_receipts AS (
    SELECT 
        block_number,
        partition_key, 
        tx_hash,
        DATA:result AS receipts_json,
        _inserted_timestamp
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze_testnet__receipts_by_hash') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND DATA:result IS NOT NULL
    {% else %}
    {{ ref('bronze_testnet__receipts_by_hash_fr') }}
    WHERE DATA:result IS NOT NULL
    {% endif %}
)

SELECT 
    block_number,
    partition_key,
    tx_hash,
    receipts_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_hash']) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_receipts
QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY block_number DESC, _inserted_timestamp DESC) = 1