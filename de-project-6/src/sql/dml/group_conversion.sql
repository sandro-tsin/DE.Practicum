WITH user_group_log AS (
	SELECT luga.hk_group_id, count(DISTINCT luga.hk_user_id) cnt_added_users
	FROM STV230529__DWH.s_auth_history hg 
	LEFT JOIN STV230529__DWH.l_user_group_activity luga ON hg.hk_l_user_group_activity = luga.hk_l_user_group_activity 
	WHERE hg.event = 'add' AND luga.hk_group_id IN (SELECT hk_group_id 
													FROM STV230529__DWH.h_groups hg 
													ORDER BY registration_dt 
													LIMIT 10)
	GROUP BY luga.hk_group_id
	ORDER BY luga.hk_group_id),
user_group_messages AS (
	SELECT lgd.hk_group_id, count(DISTINCT hk_user_id) cnt_users_in_group_with_messages
	FROM STV230529__DWH.l_groups_dialogs lgd 
	LEFT JOIN STV230529__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id 
	WHERE lgd.hk_group_id IN (SELECT hk_group_id 
								FROM STV230529__DWH.h_groups hg 
								ORDER BY registration_dt 
								LIMIT 10)
	GROUP BY lgd.hk_group_id 
	ORDER BY lgd.hk_group_id
)
SELECT 	ugl.hk_group_id, 
		cnt_added_users, 
		cnt_users_in_group_with_messages, 
		(cnt_users_in_group_with_messages/cnt_added_users) group_conversion
FROM user_group_log ugl
LEFT JOIN user_group_messages ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion desc