with dedup_query AS 
(
    select
        *,
        row_number() over(partition by id order by updateDate desc) as deduplication_id
    from {{ source('source', 'items') }}
)

select 
    id,
    name,
    category,
    updateDate
from dedup_query
where deduplication_id=1