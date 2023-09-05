-- drop tables if exists
DROP TABLE IF EXISTS shipping_country_rates CASCADE;
DROP TABLE IF EXISTS shipping_agreement CASCADE;
DROP TABLE IF EXISTS shipping_transfer CASCADE;
DROP TABLE IF EXISTS shipping_info CASCADE;
DROP TABLE IF EXISTS shipping_status CASCADE;
DROP VIEW IF EXISTS shipping_datamart;


-- create tables 
CREATE TABLE shipping_country_rates(
	id 								serial PRIMARY KEY,
	shipping_country				varchar(30),
	shipping_country_base_rate		numeric(14,2)
)


CREATE TABLE shipping_agreement(
	agreement_id 					bigint PRIMARY KEY,
	agreement_number				varchar(20),
	agreement_rate 					numeric(14,2),
	agreement_commission			numeric(14,2)
)

CREATE TABLE shipping_transfer(
	id 								serial PRIMARY KEY,
	transfer_type					varchar(20),
	transfer_model					varchar(20),
	shipping_transfer_rate			numeric(14,3)
)

CREATE TABLE shipping_info(
	shipping_id						bigint	PRIMARY KEY,
	vendor_id						bigint,
	payment_amount					numeric(14,2),
	shipping_plan_datetime			timestamp,
	shipping_transfer_id			bigint REFERENCES shipping_transfer(id),
	shipping_agreement_id			bigint REFERENCES shipping_agreement(agreement_id),
	shipping_country_rate_id 		bigint REFERENCES shipping_country_rates(id)
)

CREATE TABLE shipping_status(
	shipping_id						bigint	PRIMARY KEY,
	status							TEXT,
	state 							TEXT,
	shipping_start_fact_datetime 	TIMESTAMP,
	shipping_end_fact_datetime		TIMESTAMP
)

CREATE VIEW shipping_datamart AS 
SELECT 	si.shipping_id,
		si.vendor_id,
		st.transfer_type,
		date_part('day', age(ss.shipping_end_fact_datetime, 
			ss.shipping_start_fact_datetime)) 	full_day_at_shipping,
		CASE 
			WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime
			THEN 1
			ELSE 0
		END is_delay,
		CASE 
			WHEN ss.status = 'finished'
			THEN 1
			ELSE 0
		END is_shipping_finish,
		CASE 
			WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
			THEN EXTRACT(DAY FROM (ss.shipping_end_fact_datetime - si.shipping_plan_datetime))
			ELSE NULL
		END delay_day_at_shipping,
		si.payment_amount,
		si.payment_amount * (scr.shipping_country_base_rate 
							+ sa.agreement_rate 
							+ st.shipping_transfer_rate) vat,
		sa.agreement_commission * si.payment_amount profit
	FROM shipping_info si
	LEFT JOIN shipping_transfer st
		ON st.id = si.shipping_transfer_id
	LEFT JOIN shipping_status ss
		ON ss.shipping_id = si.shipping_id
	LEFT JOIN shipping_country_rates scr 
		ON scr.id = si.shipping_country_rate_id
	LEFT JOIN shipping_agreement sa 
		ON sa.agreement_id = si.shipping_agreement_id