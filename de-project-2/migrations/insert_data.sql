-- insert into tables

-- shipping_country_rates
INSERT INTO shipping_country_rates
(shipping_country, shipping_country_base_rate)
SELECT DISTINCT shipping_country, shipping_country_base_rate
FROM shipping s 

SELECT *
FROM shipping_country_rates

-- vendor_agreement_description
INSERT INTO shipping_agreement
SELECT 	DISTINCT 
		(regexp_split_to_array(vendor_agreement_description,':'))[1]::int 			AS agreement_id,
		(regexp_split_to_array(vendor_agreement_description,':'))[2]::varchar(20) 	AS agreement_number,
		(regexp_split_to_array(vendor_agreement_description,':'))[3]::numeric(14,2) AS agreement_rate,
		(regexp_split_to_array(vendor_agreement_description,':'))[4]::numeric(14,2) AS agreement_commission
FROM  shipping 
ORDER BY agreement_id

SELECT *
FROM shipping_agreement

-- shipping_transfer
INSERT INTO shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
SELECT DISTINCT 
	   (regexp_split_to_array(shipping_transfer_description, ':'))[1],
	   (regexp_split_to_array(shipping_transfer_description, ':'))[2],
	   shipping_transfer_rate 
FROM shipping s 

SELECT *
FROM shipping_transfer

-- shipping_info
INSERT INTO shipping_info
SELECT 	shippingid 					shipping_id,
		max(vendorid) 				vendor_id,
		max(payment_amount) 		payment_amount,
		max(shipping_plan_datetime) shipping_plan_datetime,
		max(st.id)					shipping_transfer_id,
		max((regexp_split_to_array(vendor_agreement_description,':'))[1]::int) shipping_agreement_id,
		max(scr.id)					shipping_country_rate_id
FROM shipping s
LEFT JOIN shipping_transfer st ON st.shipping_transfer_rate = s.shipping_transfer_rate 
LEFT JOIN shipping_country_rates scr ON scr.shipping_country = s.shipping_country 
GROUP BY shippingid 

SELECT *
FROM shipping_info

-- shipping_status
INSERT INTO shipping_status
WITH last_dt AS(
	SELECT 	
		DISTINCT
			shippingid,
			max(state_datetime) OVER(PARTITION BY shippingid) state_datetime
	FROM shipping),
fact_dt AS (
	SELECT 	shippingid,
			min(CASE WHEN state = 'booked' THEN state_datetime END) shipping_start_fact_datetime,
			max(CASE WHEN state = 'recieved' THEN state_datetime END)  shipping_end_fact_datetime 
	FROM shipping
	GROUP BY shippingid
)
SELECT 	
		s.shippingid			 	shipping_id,
		status						status,
	  	state						state,
	  	fact_dt.shipping_start_fact_datetime		shipping_start_fact_datetime,
	  	fact_dt.shipping_end_fact_datetime 		shipping_end_fact_datetime
	FROM last_dt
	LEFT JOIN shipping s ON s.shippingid = last_dt.shippingid 
	AND s.state_datetime = last_dt.state_datetime
	LEFT JOIN fact_dt ON fact_dt.shippingid = last_dt.shippingid
	ORDER BY shipping_id
