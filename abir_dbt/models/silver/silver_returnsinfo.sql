with returns AS
(
    select
        sales_id,
        product_sk,
        returned_qty,
        return_reason,
        refund_amount
    from 
        {{ ref('bronze_returns') }}
),


product AS
(
    select
        product_sk,
        category,
        product_name
    from 
        {{ ref('bronze_product') }}
),

joined_query AS
(
    select
        returns.sales_id,
        product.category,
        product.product_name,
        returns.returned_qty,
        returns.return_reason,
        returns.refund_amount

    from
        returns
        JOIn product ON returns.product_sk = product.product_sk
)

select
    category,
    return_reason, 
    sum(refund_amount) as total_refunds,
    sum(returned_qty) as total_returned_quantity
from 
    joined_query
group by category, return_reason
order by total_refunds desc