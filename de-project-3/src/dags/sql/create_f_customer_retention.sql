CREATE TABLE IF NOT EXISTS mart.f_customer_retention(
	new_customers_count 		int,
	returning_customers_count 	int, 
	refunded_customer_count		int,
	period_name					varchar(10) DEFAULT 'weekly',
	period_id					int, 
	item_id						int REFERENCES mart.d_item(item_id),
	new_customers_revenue		NUMERIC(10,2),
	returning_customers_revenue NUMERIC(10,2),
	customers_refunded			NUMERIC(10,2)
)