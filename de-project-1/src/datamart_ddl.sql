create table if not exists analysis.dm_rfm_segments (
user_id int4 primary key,
recency int4 not null,
frequency int4 not null,
monetary_value int4 not null)