drop view analysis.orders;

create view analysis.orders as 
with last_status as (
select order_id, max(dttm) last_date
from production.OrderStatusLog
group by order_id),
all_statuses as (
select *
from production.OrderStatusLog
),
all_orders as (select *
from production.orders)
select all_orders.order_id,
all_orders.order_ts,
all_orders.user_id,
all_orders.bonus_payment, 
all_orders.payment,
all_orders.cost,
all_orders.bonus_grant,
all_statuses.status_id as status
from all_statuses
left join last_status on last_status.order_id = all_statuses.order_id 
and last_status.last_date = all_statuses.dttm
left join all_orders on all_orders.order_id = all_statuses.order_id
where last_status.order_id is not null
order by all_statuses.order_id