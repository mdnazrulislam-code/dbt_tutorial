with sales AS 
(
    select
        sales_id,
        product_sk,
        customer_sk,
        {{ multiply('quantity', 'unit_price') }} as calculated_gross_amount,
        gross_amount,
        payment_method
    from 
        {{ ref('bronze_sales') }}
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

customer AS
(
    select
        customer_sk,
        gender,
        loyalty_tier
    from 
        {{ ref('bronze_customer') }}
),
joined_query AS
(
    select
        sales.sales_id,
        sales.calculated_gross_amount,
        sales.gross_amount,
        sales.payment_method,
        product.category,
        product.product_name,
        customer.gender,
        customer.loyalty_tier
    from
        sales
        JOIN product ON sales.product_sk = product.product_sk
        JOIN customer ON sales.customer_sk = customer.customer_sk
)

select
    category,
    gender, 
    sum(gross_amount) as total_sales
from 
    joined_query
group by category, gender
order by total_sales desc