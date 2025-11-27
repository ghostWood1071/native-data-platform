with order_count as (
    select customer_id, count(1) order_quant from (
        select *
        from orders
        where updated_at >= {$run_date}
         and created_at
           < {$run_date}
         and status == 'Completed'
    ) group by customer_id
),
top_count as (
    select * from order_count order by order_quant limit 10
)
select a.* from customers a
join top_count b
on a.id = b.customer_id