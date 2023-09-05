INSERT INTO mart.f_customer_retention(
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
)
WITH all_distinct AS (
	SELECT DISTINCT 
			uol.item_id, 
			extract(week FROM date_time::date) period_id
	FROM staging.user_order_log uol
	ORDER BY period_id, item_id),
new_customers AS(
	SELECT 	item_id, 
			period_id, 
			count(customer_id) new_customers_count, 
			sum(sum) new_customers_revenue
		FROM (
			SELECT 	item_id,
					customer_id, 
					extract(week FROM date_time::date) period_id,
					sum(payment_amount) sum, 
					count(payment_amount) --extract(week FROM date_time::date) week, customer_id, item_id, payment_amount, status
			FROM staging.user_order_log uol 
			WHERE status = 'shipped' 
			GROUP BY customer_id, period_id, item_id) AS cou
	WHERE count = 1
	GROUP BY period_id, item_id
	ORDER BY period_id, item_id),
returning_customers AS (
	SELECT 	item_id, 
			period_id, 
			count(customer_id) returning_customers_count, 
			sum(sum) returning_customers_revenue
		FROM (
			SELECT 	item_id,
					customer_id, 
					extract(week FROM date_time::date) period_id,
					sum(payment_amount) sum, 
					count(payment_amount) --extract(week FROM date_time::date) week, customer_id, item_id, payment_amount, status
			FROM staging.user_order_log uol 
			WHERE status = 'shipped' 
			GROUP BY customer_id, period_id, item_id) AS cou
	WHERE count > 1
	GROUP BY period_id, item_id
	ORDER BY period_id, item_id),
refunded_customers AS (
	SELECT 	item_id, 
			period_id, 
			count(customer_id) refunded_customer_count, 
			sum(sum) customers_refunded
		FROM (
			SELECT 	item_id,
				customer_id, 
				extract(week FROM date_time::date) period_id,
				sum(payment_amount) sum, 
				count(payment_amount) --extract(week FROM date_time::date) week, customer_id, item_id, payment_amount, status
			FROM staging.user_order_log uol 
			WHERE status = 'refunded' 
			GROUP BY customer_id, period_id, item_id) AS cou
	GROUP BY period_id, item_id
	ORDER BY period_id, item_id)
SELECT 	nc.new_customers_count, 
		rtc.returning_customers_count,
		rfc.refunded_customer_count,
		ad.period_id,
		ad.item_id, 
		nc.new_customers_revenue,
		rtc.returning_customers_revenue,
		rfc.customers_refunded
	FROM all_distinct ad
	LEFT JOIN new_customers nc ON ad.item_id = nc.item_id AND ad.period_id = nc.period_id
	LEFT JOIN returning_customers rtc ON ad.item_id = rtc.item_id AND ad.period_id = rtc.period_id
	LEFT JOIN refunded_customers rfc ON ad.item_id = rfc.item_id AND ad.period_id = rfc.period_id