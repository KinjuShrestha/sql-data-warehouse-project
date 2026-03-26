-- check if primary key is duplicate or null 

select prd_id, count(*) from BRONZE.CRM_PRD_INFO 
group by prd_id
 having count(*) >1 or  prd_id is null

-- check if null for all columns
select prd_line from BRONZE.CRM_PRD_INFO  where prd_line is null or prd_line!=trim(prd_line)

-- check if start date is null, format of date and too old if condtitions apply, start time is > now
select PRD_START_DT from BRONZE.CRM_PRD_INFO where
  len(prd_start_dt) >10 
  or prd_start_dt is null
   or prd_start_dt > getdate()

   -- check if end date is null, format of date and before start time and > then getdate()
select PRD_end_DT from BRONZE.CRM_PRD_INFO where
  len(prd_end_dt) >10 
  or prd_end_dt is null
   or prd_end_dt > getdate()
   or prd_end_dt < prd_start_dt

   -- check for null or negative values
select PRD_cost from BRONZE.CRM_PRD_INFO where
prd_cost is null or prd_cost <0

-- prd_line fix prd_cost fix prd_end
