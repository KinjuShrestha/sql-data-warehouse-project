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
    cat_id,
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
SUBSTRING(PRD_KEY,1,5) as cat_id,
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





--fix ord num ,sls__ord_dt - 0 value,sls_ship_dt ship dt > orderdt,due dt fine , 
--sls sales (-ve value, 0,null), slsprice (-ve,null)


Truncate table  SILVER.sales_details;
Insert into SILVER.sales_details(
     sls_ord_num,
     sls_cat,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select
 sls_ord_num,
SUBSTRING(TRIM(sls_prd_key),0,6) as sls_cat,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or len(sls_order_dt)!=8 then null 
else 
cast(cast(sls_order_dt as varchar)as date) 
end as sls_order_dt,
case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null 
else 
cast(cast(sls_ship_dt as varchar)as date) 
end as sls_ship_dt,
case when sls_due_dt=0 or len(sls_due_dt)!=8 then null 
else 
cast(cast(sls_due_dt as varchar)as date) 
end as sls_due_dt,
case 
when sls_sales <=0 or sls_sales is null or sls_sales!= ABS(sls_price) * sls_quantity then ABS(sls_price) * sls_quantity
else sls_sales 
end as sls_sales,
sls_quantity,

case when sls_price <=0 or sls_price is null  then sls_sales/NULLIF(sls_quantity,0)
else sls_price 
end as sls_price
 from
(Select 
*,
ROW_NUMBER() Over(partition by sls_ord_num order by sls_ord_num desc ) as rank

 from BRONZE.CRM_Sales_details)t where rank =1



Truncate table Silver.erp_cust_az12;
Insert into Silver.erp_cust_az12(
    cid,
    bdate,
    gen
)

select 

case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
else cid 
end 
as cid ,
case when bdate > getdate() or len(bdate)!=10 then null
 else bdate
 end as bdate,
case   when UPPER(TRIM(GEN)) like 'M%' then 'Male' 
 when UPPER(TRIM(GEN)) like 'F%' then 'Female' 
 else 
 'n/a'
 end  as gen


 from bronze.erp_cust_az12


Truncate table silver.erp_cat_g1v2;
Insert into silver.erp_cat_g1v2(
    id,cat,subcat,maintenance
)
SELECT  
replace(id,'_','-') as id,
      cat
      ,subcat
      ,
      case when maintenance like 'Y%' THEN 'YES'
      ELSE 'NO'
      END AS
      maintenance
  FROM [DATAWAREHOUSE].[BRONZE].[erp_cat_g1v2]

    

truncate table  silver.erp_loc_a101;
Insert into  silver.erp_loc_a101(
cid,
cntry
)



select 
REPLACE(CID,'-','') AS CID,
case when 
len(Replace(Trim(cntry),CHAR(13),''))=0 then 'N/A'
when Replace(Trim(cntry),CHAR(13),'') in ('USA','UNITED STATES','US') then 'United States'
 when Replace(Trim(cntry),CHAR(13),'') in ('Germany','DE') then 'Germany'
 else Replace(Trim(cntry),CHAR(13),'')
 end as cntry
 from bronze.erp_loc_a101




