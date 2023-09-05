drop table if exists STV230529__DWH.s_admins;

create table STV230529__DWH.s_admins
(
	hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV230529__DWH.l_admins (hk_l_admin_id),
	is_admin boolean,
	admin_from datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

DROP TABLE IF EXISTS STV230529__DWH.s_group_name;

CREATE TABLE STV230529__DWH.s_group_name(
	hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_name_groups REFERENCES STV230529__DWH.h_groups (hk_group_id),
	group_name varchar(100),
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::date 
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


DROP TABLE IF EXISTS STV230529__DWH.s_group_private_status;

CREATE TABLE STV230529__DWH.s_group_private_status(
		hk_group_id bigint NOT NULL CONSTRAINT fk_s_group_name_groups REFERENCES STV230529__DWH.h_groups (hk_group_id),
		is_privat boolean,
		load_dt datetime,
		load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE 
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)


DROP TABLE IF EXISTS STV230529__DWH.s_dialog_info;

CREATE TABLE STV230529__DWH.s_dialog_info(
		hk_message_id bigint NOT NULL CONSTRAINT fk_s_s_dialog_info_dialogs REFERENCES STV230529__DWH.h_dialogs (hk_message_id),
		message_ts timestamp,
		message_from int,
		message_to int,
		load_dt datetime,
		load_src varchar(20)	
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::DATE 
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)


DROP TABLE IF EXISTS STV230529__DWH.s_user_socdem;

CREATE TABLE STV230529__DWH.s_user_socdem(
		hk_user_id bigint NOT NULL CONSTRAINT fk_s_s_user_chat_info_users REFERENCES STV230529__DWH.h_users (hk_user_id),
		country varchar(200),
		age int,
		load_dt datetime,
		load_src varchar(20)	
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE 
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)


DROP TABLE IF EXISTS STV230529__DWH.s_user_chatinfo;

CREATE TABLE STV230529__DWH.s_user_chatinfo(
	hk_user_id bigint NOT NULL CONSTRAINT fk_s_s_user_chat_info_users REFERENCES STV230529__DWH.h_users (hk_user_id),
	chat_name varchar(100) NOT NULL,
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
segmented BY hk_user_id all nodes 
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)


DROP TABLE IF EXISTS STV230529__DWH.s_auth_history;

CREATE TABLE STV230529__DWH.s_auth_history(
	hk_l_user_group_activity bigint NOT NULL CONSTRAINT fk_s_l_auth_user_group_activity REFERENCES STV230529__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar, 
	event_dt timestamp,
	load_dt datetime,
	load_src varchar(20)
)
ORDER BY load_dt
segmented BY hk_l_user_group_activity all nodes 
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)