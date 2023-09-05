insert into analysis.dm_rfm_segments
select rr.user_id, recency, frequency, monetary_value
from analysis.tmp_rfm_recency rr
left join analysis.tmp_rfm_frequency rf on rr.user_id = rf.user_id
left join analysis.tmp_rfm_monetary_value rfv on rr.user_id = rfv.user_id
order by rr.user_id