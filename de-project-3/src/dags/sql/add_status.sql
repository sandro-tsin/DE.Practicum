ALTER TABLE staging.user_order_log
ADD COLUMN IF NOT EXISTS status varchar(100);
UPDATE staging.user_order_log
SET status = 'shipped'
WHERE status IS NULL;