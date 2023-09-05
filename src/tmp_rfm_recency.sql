insert into analysis.tmp_rfm_recency
with cte as (select user_id, max(date(order_ts)) last_order
from orders 
where status = 4
group by user_id
order by user_id)
select o.user_id, 
ntile(5) over (order by
	(case 
		when max(last_order) is null then (select min(order_ts)::date
from orders as min_date)
		else max(last_order)
	end)) AS recency
from orders o
left join cte c on o.user_id = c.user_id
group by o.user_id 
order by recency desc

