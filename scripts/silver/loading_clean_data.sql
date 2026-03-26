-- check for null and duplicates in primary key//
/*
select CST_FIRSTNAME from bronze.crm_cust_info where cst_id is null;
after running query
- results there are nulls in primary key

select cst_id, count(*) from bronze.crm_cust_info group by cst_id having count(*) >1 ;
--we have duplicates too 

--check unwanted spaces
select CST_FIRSTNAME from bronze.crm_cust_info where cst_firstname !=TRIM(cst_firstname)
*/
truncate table  SILVER.crm_prd_info;
Insert into SILVER.crm_prd_info(
    prd_id,
    prd_key,
   prd_nm ,
    prd_cost ,
    prd_line ,
    prd_start_dt
   ,prd_end_dt
)
-- prd_line fix prd_cost fix prd_end
SELECT
prd_id,
SUBSTRING(PRD_KEY,7,LEN(PRD_KEY))AS PRD_KEY,

prd_nm,
ISNULL(prd_cost,0) as prd_cost,
 case  UPPER(TRIM(PRD_LINE))
when 'R' then 'Road' 
when 'S' then 'Other Sale'
  when 'M'then 'Mountain' 
  when 'T'then 'Touring' 

  else 
  'N/A'
end
  PRD_LINE,
  Cast(prd_start_dt as date) as prd_start_dt,
  DateAdd(day,-1,Cast(LEAD(PRD_START_DT) OVER(Partition BY PRD_KEY ORDER BY PRD_START_DT) as Date))   AS PRD_END_DT

  
  from [BRONZE].[CRM_PRD_INFO]





