DROP TABLE IF EXISTS STV230529__STAGING.users CASCADE;

CREATE TABLE STV230529__STAGING.users(
	id int PRIMARY KEY,
	chat_name varchar(200), 
	registration_dt timestamp, 
	country varchar(200), 
	age int 
)
ORDER BY id;


DROP TABLE IF EXISTS STV230529__STAGING.groups CASCADE;

CREATE TABLE STV230529__STAGING.groups(
	id int PRIMARY KEY,
	admin_id int,
	group_name VARCHAR(100),
	registration_dt timestamp, 
	is_private BOOLEAN,
	FOREIGN KEY (admin_id) REFERENCES STV230529__STAGING.users(id)
)
order by id, admin_id
PARTITION BY registration_dt::date
GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);



DROP TABLE IF EXISTS STV230529__STAGING.dialogs CASCADE;

CREATE TABLE STV230529__STAGING.dialogs(
	message_id int PRIMARY KEY,
	message_ts timestamp, 
	message_from int, 
	message_to int, 
	message  VARCHAR(1000), 
	message_group int,
	FOREIGN KEY (message_from) REFERENCES STV230529__STAGING.users(id),
	FOREIGN KEY (message_to) REFERENCES STV230529__STAGING.users(id) 
)
ORDER BY message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

DROP TABLE IF EXISTS STV230529__STAGING.group_log CASCADE;

CREATE TABLE STV230529__STAGING.group_log(
	group_id int PRIMARY KEY,
	user_id int, 
	user_id_from int, 
	event varchar,
	datetime timestamp,
	FOREIGN KEY (group_id) REFERENCES STV230529__STAGING.groups(id), 
	FOREIGN KEY (user_id) REFERENCES STV230529__STAGING.users(id), 
	FOREIGN KEY (user_id_from) REFERENCES STV230529__STAGING.users(id)
)
ORDER BY group_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);