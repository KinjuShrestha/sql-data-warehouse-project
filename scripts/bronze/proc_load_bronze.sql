
/*
====================================================================
Procedure Name : bronze.load_bronze
Description    : This procedure loads raw data into the Bronze layer
                 of the data warehouse from source CSV files.

Details        :
- Truncates existing data in Bronze tables
- Performs BULK INSERT operations from CSV files
- Loads CRM and ERP datasets into respective tables
- Captures basic load duration for monitoring

Data Sources   :
- cust_info.csv
- prd_info.csv
- sales_details.csv
- CUST_AZ12.csv
- loc_a101.csv
- px_cat_g1v2.csv

Layer Purpose  :
The Bronze layer stores raw, unprocessed data exactly as received
from source systems. No transformations or data cleansing are applied
at this stage.

Notes          :
- Ensure source files are available in the container path:
  /var/opt/mssql/data/
- Column structure in tables must match CSV format
- Data type mismatches (e.g., dates) may cause load errors


====================================================================
*/


create or alter procedure  bronze.load_bronze as 
BEGIN
declare 
@startime_bronze datetime,
@endtime_bronze datetime,
@start_time datetime, @end_time datetime;
begin try
print'==================================================================';

  print 'loading bronze layer';
print'==================================================================';
print'-------------------------------------------------------------------';
  print 'loading crm tables ';

print'-------------------------------------------------------------------';


  print '>>>truncating table bronze.crm_cust_info ';
set @start_time = GETDATE();
set @startime_bronze = Getdate();

truncate table bronze.crm_cust_info ;
  print '>>>inserting data into table bronze.crm_cust_info ';

bulk insert bronze.crm_cust_info 
from '/var/opt/mssql/data/cust_info.csv'
with(
    firstrow=2,
    fieldterminator=',',
    tablock
);
set @end_time = GETDATE();
print '>> load duration ' + Cast(Datediff(second, @end_time,@start_time) as nVARCHAR)


  print '>>>truncating table bronze.crm_prd_info ';

truncate table bronze.crm_prd_info;
  print '>>>inserting data into table bronze.crm_prd_info ';

bulk insert bronze.crm_prd_info
from '/var/opt/mssql/data/prd_info.csv'
with(
firstrow=2,
fieldterminator=',',
tablock
)


TRUNCATE table bronze.crm_sales_details;
bulk insert  bronze.crm_sales_details FROM
'/var/opt/mssql/data/sales_details.csv'
with(
    firstrow=2,
    fieldterminator=',',
    tablock
)
print'-------------------------------------------------------------------';
  print 'loading erp tables ';

print'-------------------------------------------------------------------';

truncate table bronze.erp_cust_az12
bulk insert bronze.erp_cust_az12 FROM
'/var/opt/mssql/data/CUST_AZ12.csv'
with(
    firstrow=2,
    fieldterminator=',',
    tablock
);

truncate table bronze.erp_loc_a101
bulk insert bronze.erp_loc_a101 FROM
'/var/opt/mssql/data/loc_a101.csv'
with(
    firstrow=2,
    fieldterminator=',',
    tablock
);

truncate table bronze.erp_cat_g1v2
bulk insert bronze.erp_cat_g1v2 FROM
'/var/opt/mssql/data/px_cat_g1v2.csv'
with(
    firstrow=2,
    fieldterminator=',',
    tablock
);
end try

begin catch 

end catch 

set @endtime_bronze = Getdate();
print 'duration taken for whole batch ---- ' + Cast(DATEDIFF(second,@endtime_bronze,@startime_bronze) as NVARCHAR)
end     
// exec bronze.load_bronze;
