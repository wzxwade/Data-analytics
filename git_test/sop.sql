SELECT
    COUNT(*)
FROM
    frfapp.COLLECTIONS_OVERVIEW07@E1MARSPD@e1marspd
    
    
SELECT table_name, column_name
FROM all_tab_columns@e1marspd
WHERE table_name = 'COLLECTIONS_OVERVIEW07'
ORDER BY COLUMN_NAME

SELECT 
CUSTOMER_NUMBER,
AG_0_TO_30,
AG_31_TO_60,
AG_61_TO_90,
AG_91_TO_120,
AG_121_TO_150,
AG_151_TO_180,
AG_181_TO_365

SELECT COUNT(*) FROM(
select renewal_cst,MPR_MONTH,SUM(units),round(max(net_price))FROM FRFAPP.REP_SNAPSHOT4@E1MARSPD
WHERE service_type_id not in (5,6,82,84,56,356)
group by renewal_cst,MPR_MONTH)


SELECT CST_ID,MPR_MONTH,rep_count, net_price
FROM (
select CST_ID,MPR_MONTH,rep_count ,net_price  from E1_SBMM_MONTHLY 
LEFT JOIN
(
select renewal_cst,MPR_MONTH,SUM(units) as rep_count,round(max(net_price)) as net_price FROM FRFAPP.REP_SNAPSHOT4@E1MARSPD
WHERE service_type_id not in (5,6,82,84,56,356)
group by renewal_cst,MPR_MONTH) repsnap
on E1_SBMM_MONTHLY.CST_ID = repsnap.renewal_cst) 



select * FROM FRFAPP.REP_SNAPSHOT4@E1MARSPD
SELECT renewal_cst,MPR_MONTH_AS_DATE,COUNT(*),round(max(net_price)) FROM FRFAPP.REP_SNAPSHOT4@E1MARSPD
WHERE  Extract (Year from MPR_MONTH_AS_DATE)  in (2016,2017,2018,2019,2020)  
and service_type_id not in (5,6,82,84,56,356)
group by renewal_cst,MPR_MONTH_AS_DATE
ORDER BY renewal_cst,MPR_MONTH_AS_DATE DESC


select * from E1_SBMM_MONTHLY

select *FROM   arrow.tsop_worksheet@ct1rpt

drop table sop00

select * from CST_INFORMATION@FIN_REPORT_RS 
where customer_number=1425200

select count(customer_id) from(
select customer_id,business_name, affiliation_id, affiliation_name, GL_CATEGORY, SUM(REV2019_TTM) from stat_pack@fin_report_rs 
group by customer_id,business_name, affiliation_id, affiliation_name, GL_CATEGORY)

select * from erpdata.t_all_rep_with_state_id 
SELECT *  
           FROM   arrow.tbusiness_name@ct1rpt where bus_name_id = 19046500000
           
           
SELECT *  
           FROM FRFAPP.REP_SNAPSHOT4@E1MARSPD where entity_id = 8004654714
           
SELECT *
           FROM   arrowuser.trepresentation@e1marspd
           where rep_id = 210206366
           
SELECT *  
           FROM arrowuser.tentity@e1marspd      
where entity_id = 9200103795

drop table sop00

create table sop00 AS(      
 SELECT
          rep_id,
          w.entity_id,
          w.renewal_cst,
          w.cst_name,
          w.rep_juris_id,  
          w.dom_juris_id,  
          w.lawsuit_type_cd,    
          Trunc(w.received_date, 'MONTH') received_month,  
          Count(*)                        SOP  
   FROM   (
           SELECT received_date,reject_reason_cd,is_attempted,arr.rep_id,entity_id,rep_juris_id, dom_juris_id, lawsuit_type_cd,renewal_cst,cst_name
           FROM   arrow.tsop_worksheet@ct1rpt arr
           LEFT JOIN 
           (SELECT rep_id,renewal_cst,cst_name from FRFAPP.REP_SNAPSHOT4@E1MARSPD 
           GROUP BY rep_id,renewal_cst,cst_name) era
           ON arr.rep_id = era.rep_id
           left join 
           (SELECT rep_id, entity_id 
           FROM   arrowuser.trepresentation@e1marspd) ent
           on arr.rep_id = ent.rep_id) w  
   WHERE  w.received_date >= '01-jan-2019'  
          AND w.reject_reason_cd IS NULL  
          AND w.is_attempted <> 'Y' 
          AND rep_id IS NOT NULL
   GROUP  BY 
             w.rep_id,
             w.entity_id,
             w.renewal_cst,
             w.cst_name,
             w.rep_juris_id,  
             w.dom_juris_id,  
             w.lawsuit_type_cd, 
             Trunc(w.received_date, 'MONTH'))          
             
create table sop01 as (
select rep_id,
          sop.entity_id,
          renewal_cst,
          cst_name,
          rep_juris_id,  
          dom_juris_id,  
          lawsuit_type_cd,    
          received_month,  
          SOP from (             
select           rep_id,
          entity_id,
          rep_juris_id,  
          dom_juris_id,  
          lawsuit_type_cd,    
          received_month,  
          SOP   from sop00     
where renewal_cst is null) sop
LEFT JOIN 
           (SELECT entity_id,renewal_cst,cst_name from FRFAPP.REP_SNAPSHOT4@E1MARSPD 
           GROUP BY entity_id,renewal_cst,cst_name) era
on sop.entity_id =  era.entity_id

union all

select * from sop00     
where renewal_cst is not null)


select sop01.*,trim(cst.cst_state) cst_state from sop01
left join 
(select customer_number as renewal_cst, cst_state from CST_INFORMATION@FIN_REPORT_RS) cst
on sop01.renewal_cst = cst.renewal_cst


select * from sop01
where entity_id = 8004654714


select * from erpdata.t_all_rep_with_state_id
where RENEWAL_BILL_CST	= 8843600






select service_name, count(affl_or_acct) from HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
WHERE Extract (Year from MPR_MONTH_AS_DATE) =2019
group by service_name


select distinct service_type_id from HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
where lower(service_name) like '%special agency%'

select qqq,count(*) from(
select  
case when instr(lower(service_name),'special agency') > 0 then 'Special Agency'
when instr(lower(service_name),'fmca') > 0 then 'FMCA'
when service_type_id in ( 5 , 6 , 82 , 84 )  then 'Independent Director'
when service_type_id in ( 56 , 356  )  then 'Contract Agency'
when service_type_id = 22  then 'Assumed Name'
else 'Standard Rep' end service_name
from HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
WHERE Extract (Year from MPR_MONTH_AS_DATE) =2019) aaa
group by qqq


select distinct service_name from HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
where service_type_id in ( 5 , 6 , 82 , 84 ) 
and Extract (Year from MPR_MONTH_AS_DATE) =2019





SELECT CST_ID,MPR_MONTH,rep_count, net_price
FROM (
select CST_ID,MPR_MONTH,rep_count ,net_price from E1_SBMM_MONTHLY 
LEFT JOIN
(
select renewal_cst,MPR_MONTH,SUM(units) as rep_count,round(avg(net_price)) as net_price FROM HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
WHERE (Extract (Year from MPR_MONTH_AS_DATE) IN (2017,2018,2019,2020) AND  
Extract (MONTH from MPR_MONTH_AS_DATE)=(select extract(month from Last_Day(ADD_MONTHS(trunc(sysdate,'mm'),-2))+1) AS LAST_MONTH from dual) 
AND service_type_id not in (5,6,82,84,56,356))
group by renewal_cst,MPR_MONTH) repsnap
on E1_SBMM_MONTHLY.CST_ID = repsnap.renewal_cst) 



select count(cst_id) from(
select CST_ID,MPR_MONTH,service_name,rep_count ,net_price from E1_SBMM_MONTHLY 
LEFT JOIN
(select renewal_cst,MPR_MONTH,service_name,
SUM(units) as rep_count,round(avg(net_price)) as net_price from
(
select renewal_cst,MPR_MONTH,units,net_price,
case when instr(lower(service_name),'special agency') > 0 then 'Special Agency'
when instr(lower(service_name),'fmca') > 0 then 'FMCA'
when service_type_id in ( 5 , 6 , 82 , 84 )  then 'Independent Director'
when service_type_id in ( 56 , 356  )  then 'Contract Agency'
when service_type_id = 22  then 'Assumed Name'
else 'Standard Rep' end service_name
FROM HESLINM.REP_ARROW_SNAPSHOTS@E1MARSPD
WHERE (Extract (Year from MPR_MONTH_AS_DATE) IN (2017,2018,2019,2020) 
)) group by renewal_cst,MPR_MONTH,service_name) repsnap
on E1_SBMM_MONTHLY.CST_ID = repsnap.renewal_cst)





WITH rep_lvl
     AS (SELECT rep_id,
                state_identifier,
                juris_id,
                jurisdiction,
                rep_status,
                entity_id,
                true_name,
                active_units,
                inactive_units,
                affiliation_id,
                affiliation_name,
                renewal_bill_cst,
                renewal_bill_cst_name,
                renewal_customer_segment,
                renewal_email_address,
                renewal_first_name,
                renewal_last_name,
                renewal_mailing_addr,
                renewal_mailing_city,
                renewal_mailing_state,
                renewal_mailing_zip_code,
                renewal_middle_initial,
                renewal_phone_number,
                renewal_prefix,
                renewal_title,
                comms_cst,
                comms_cst_name,
                comms_customer_segment,
                comms_email_address,
                comms_first_name,
                comms_last_name,
                comms_mailing_addr,
                comms_mailing_city,
                comms_mailing_state,
                comms_mailing_zip_code,
                comms_middle_initial,
                comms_phone_number,
                comms_prefix,
                comms_title
         FROM   erpdata.t_all_rep_with_state_id),
     aff_lfa
     AS (SELECT *
         FROM   arrowuser.taffiliation@e1marspd),
     entity_cnt
     AS (SELECT entity_id,
                Count (DISTINCT rep_id)    AS active_ent_units,
                Count(DISTINCT juris_name) AS active_ent_juris
         FROM   frfapp.rep_snapshot4@e1marspd
         GROUP  BY entity_id),
     affl_cnt
     AS (SELECT affl_or_acct,
                Count (DISTINCT rep_id)    AS active_affl_units,
                Count(DISTINCT juris_name) AS active_affl_juris
         FROM   frfapp.rep_snapshot4@e1marspd
         WHERE  affl_or_acct <> entity_id
         GROUP  BY affl_or_acct),
     dnb_match
     AS (SELECT cust_id AS cst_dun,
                verified_dunsnumber,
                confidence_code
         FROM   jdemars.dun_and_bradstreet_matched@e1marspd
         WHERE  confidence_code IN ( '08', '09', '10' )),
     dnb_table
     AS (SELECT d.duns_number                 AS DUNS,
                d.business_name               AS DUNS_NAME,
                d.global_ult_duns_number      AS GDUNS,
                d.global_ult_business_name    AS GUDNS_NAME,
                --d.US_1987_SIC_1 AS SIC,
                Substr(d.us_1987_sic_1, 0, 2) AS SIC2,
                Substr(d.us_1987_sic_1, 0, 4) AS SIC4,
                d.sales_volume_us_dollars     AS DUNS_Sales,
                d.number_of_family_members    AS DUNS_Mmbrs,
                d.employees_total             AS DUNS_Emplys,
                Substr(g.us_1987_sic_1, 0, 2) AS GDUNS_SIC2,
                Substr(g.us_1987_sic_1, 0, 4) AS GDUNS_SIC4,
                g.sales_volume_us_dollars     AS GDUNS_Sales,
                g.number_of_family_members    AS GDUNS_Mmbrs,
                g.employees_total             AS GDUNS_Emplys
         FROM   jdemars.dun_and_bradstreet@e1marspd d
                LEFT OUTER JOIN jdemars.dun_and_bradstreet@e1marspd g
                             ON d.global_ult_duns_number = G.duns_number),
     sales_table
     AS (SELECT customer_number AS cst,
                salesforce_id   AS CUSTOMER_SFID,
                sales_channel,
                sales_person,
                sales_tu_fullname,
                sales_territory,
                region,
                sales_type,
                sales_manager,
                sales_director,
                sales_vp_name,
                sales_group,
                CST_STATUS,
                CUSTOMER_SEGMENT,
                CUSTOMER_SUBSEGMENT
         FROM   erpdata.cst_information),
     duns_lookup
     AS (SELECT Trim(scy55dbsic1) AS SIC,
                scds01            AS SIC_DESC
         FROM   jdedta.f55sic01@e1marspd)

SELECT a.*,
       is_lawfirm,
       active_ent_units,
       active_ent_juris,
       active_affl_units,
       active_affl_juris,
       e.*,
       f.*,
       G.*,
       H.sic_desc AS SIC2_DESC,
       I.sic_desc AS SIC4_DESC,
       J.sic_desc AS GDUNS_SIC2_DESC,
       K.sic_desc AS GDUNS_SIC4_desc
FROM   rep_lvl a
       LEFT OUTER JOIN aff_lfa b
                    ON a.affiliation_id = b.affl_id
       LEFT OUTER JOIN entity_cnt c
                    ON a.entity_id = c.entity_id
       LEFT OUTER JOIN affl_cnt d
                    ON a.affiliation_id = d.affl_or_acct
       LEFT OUTER JOIN dnb_match e
                    ON a.renewal_bill_cst = e.cst_dun
       LEFT OUTER JOIN dnb_table f
                    ON e.verified_dunsnumber = f.duns
       LEFT OUTER JOIN sales_table g
                    ON a.renewal_bill_cst = g.cst
       LEFT OUTER JOIN duns_lookup h
                    ON f.sic2 = H.sic
       LEFT OUTER JOIN duns_lookup i
                    ON f.sic4 = I.sic
       LEFT OUTER JOIN duns_lookup j
                    ON f.gduns_sic2 = J.sic
       LEFT OUTER JOIN duns_lookup k
                    ON f.gduns_sic4 = K.sic
                    
                    
select * from CST_INFORMATION@FIN_REPORT_RS                    
