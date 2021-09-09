/*
Purpose:
Determine if all dates (days) were flattened as expected.
Instructions:
Replace "<gcp project>.<dataset>" with actual values for GCP project and dataset

Expected results:
ZERO rows.  Any rows returned have unbalanced results.
*/

with sessions as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_sessions_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
), flat_sessions as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_flat_sessions_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
), hits as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_sessions_*`
 left join unnest(hits) as hits
 where _TABLE_SUFFIX like '2%'
 group by 1
), flat_hits as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_flat_hits_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
), products as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_sessions_*`
 left join UNNEST(hits) AS hit
 inner join UNNEST(hit.product) AS hitProduct
 where _TABLE_SUFFIX like '2%'
 group by 1
), flat_products as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_flat_products_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
), promotions as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_sessions_*`
 left join UNNEST(hits) AS hit
 inner join UNNEST(hit.promotion) AS hitPromotion
 where _TABLE_SUFFIX like '2%'
 group by 1
), flat_promotions as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_flat_promotions_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
), experiments as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_sessions_*`
 left join UNNEST(hits) AS hit
 inner join UNNEST(hit.experiment) AS hitExperiment
 where _TABLE_SUFFIX like '2%'
 group by 1
), flat_experiments as (
 SELECT _TABLE_SUFFIX as day, count(*) as cnt
 FROM `<gcp project>.<dataset>.ga_flat_experiments_*`
 where _TABLE_SUFFIX like '2%'
 group by 1
)
select 
s.day, s.cnt as sessions, fs.cnt as flat_sessions, IFNULL(abs(s.cnt - fs.cnt),s.cnt) as delta_sessions, 
h.cnt as hits, fh.cnt as flat_hits, IFNULL(abs(h.cnt - fh.cnt), h.cnt) as delta_hits,
p.cnt as products, fp.cnt as flat_products, ifnull(abs(p.cnt - fp.cnt), p.cnt) as delta_products,
pr.cnt as promotions, fpr.cnt as flat_promotions, ifnull(abs(pr.cnt - fpr.cnt), pr.cnt) as delta_promotions,
e.cnt as experiments, fe.cnt as flat_experiments, ifnull(abs(e.cnt - fe.cnt), e.cnt) as delta_experiments
from sessions s
left join flat_sessions fs using (day)
left join hits h using (day)
left join flat_hits fh using (day)
left join products p using (day)
left join flat_products fp using (day)
left join promotions pr using (day)
left join flat_promotions fpr using (day)
left join experiments e using (day)
left join flat_experiments fe using (day)
where IFNULL(abs(s.cnt - fs.cnt), s.cnt) != 0 
or IFNULL(abs(h.cnt - fh.cnt), h.cnt) != 0
or IFNULL(abs(p.cnt - fp.cnt), p.cnt) != 0
or IFNULL(abs(pr.cnt - fpr.cnt), pr.cnt) != 0
or IFNULL(abs(e.cnt - fe.cnt), e.cnt) != 0
order by 1 desc