INSERT INTO STV230529__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV230529__DWH.l_admins as la
left join STV230529__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

INSERT INTO STV230529__DWH.s_group_name
SELECT 
		hg.hk_group_id,
		g.group_name,
		now() as load_dt,
		's3' as load_src
FROM STV230529__DWH.h_groups hg 
LEFT JOIN STV230529__STAGING.groups g ON g.id = hg.group_id 

INSERT INTO STV230529__DWH.s_group_private_status
SELECT 
		hg.hk_group_id,
		g.is_private,
		now() as load_dt,
		's3' as load_src
FROM STV230529__DWH.h_groups hg 
LEFT JOIN STV230529__STAGING.groups g ON g.id = hg.group_id 


INSERT INTO STV230529__DWH.s_dialog_info
SELECT 
		hd.hk_message_id,
		d.message_ts,
		d.message_from,
		d.message_to,
		now() AS load_dt,
		's3' AS load_src
FROM STV230529__DWH.h_dialogs hd 
LEFT JOIN STV230529__STAGING.dialogs d 
	ON hd.message_id = d.message_id 


INSERT INTO  STV230529__DWH.s_user_socdem
SELECT  hu.hk_user_id,
		u.country,
		u.age,
		now() AS load_dt,
		's3' AS load_src
FROM STV230529__DWH.h_users hu 
LEFT JOIN STV230529__STAGING.users u 
	ON hu.user_id = u.id 


INSERT INTO STV230529__DWH.s_user_chatinfo
SELECT  hu.hk_user_id,
		u.chat_name,
		now() AS load_dt,
		's3' AS load_src
FROM STV230529__DWH.h_users hu 
LEFT JOIN STV230529__STAGING.users u 
	ON hu.user_id = u.id 
	

INSERT INTO STV230529__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT 	luga.hk_l_user_group_activity,
		gl.user_id_from,
		gl.event,
		gl.datetime event_dt,
		now() AS load_dt,
		's3' AS load_src
FROM STV230529__STAGING.group_log gl 
LEFT JOIN STV230529__DWH.h_groups hg  ON gl.group_id = hg.group_id 
LEFT JOIN STV230529__DWH.h_users hu ON gl.user_id = hu.user_id 
LEFT JOIN STV230529__DWH.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id AND hu.hk_user_id = luga.hk_user_id 
