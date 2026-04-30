
-- which 5 products generate hightest revenue
select top(5) p.product_name, 
sum(s.sales_amount) as top_five_highest_revenue

from gold.fact_sales s
left join 
gold.dim_products p
on p.product_key = s.product_key
group by p.product_name 
order by top_five_highest_revenue desc

-- which 5 worst products generate 
select top(5) p.product_name, 
sum(s.sales_amount) as top_five_lowest_revenue

from gold.fact_sales s
left join 
gold.dim_products p
on p.product_key = s.product_key
group by p.product_name 
order by top_five_lowest_revenue asc

-- find top 10 customers who have genereaed highest revenue and 3 customers with the fewest orders placed 

Select top(10) 
s.customer_key,
c.first_name customers_with_highest_reveune,
sum(s.sales_amount) rn from gold.fact_sales s
left join gold.dim_customers c on 
s.customer_key = c.customer_key
 group by s.customer_key, c.first_name order by rn desc
;

Select top(3) 
c.customer_key,
c.first_name customers_with_lowest_reveune,
count(distinct s.order_number) rn from 
gold.fact_sales s
left join gold.dim_customers c on 
s.customer_key = c.customer_key
 group by c.customer_key, c.first_name 
 order by rn
;


-- sales over time

select 
Month(order_date) year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date  is not null 
group by Month(order_date)
order by year

-- cumualtive analysis 

-- cal total sales per month and running otal of  sales overtime 


-- analyze the yearly perfromance of products by
-- comparing each products sales to both its avg sales
-- and the previous years sales
With cte_prd_sales  as (
    Select 
    p.product_name ,
    sum(sales_amount) as current_sales,
    Year(s.order_date) as year
    from gold.fact_sales s

    left join gold.dim_products p 
    on 
    s.product_key=p.product_key
    where s.order_date is not null

    group by Year(s.order_date)
    ,p.product_name
    
    )


Select product_name,
current_sales,
avg(current_sales) Over(Partition by product_name) as avg_sales,
current_sales-Lag(current_sales) Over(Partition by product_name Order by year) as diff,
year,
case

 when avg(current_sales) Over(Partition by product_name) > current_sales  then 'Below Average'
when avg(current_sales) Over(Partition by product_name) < current_sales then 'Above Average'
else 'Equal to average'
end as compare_current_to_avg_sales,
case when current_sales-Lag(current_sales) Over(Partition by product_name Order by year) > 0 then 'increase'
when current_sales-Lag(current_sales) Over(Partition by product_name Order by year) < 0 then 'decreases'
ELSE 'no change'
end 
 from cte_prd_sales order by product_name




-- which categories contrihue most to overall sale

With cte_category_sale as (
    Select p.category,
    sum(s.sales_amount) total_sales
    from gold.dim_products p left join  gold.fact_sales s 
    
    on p.product_key = s.product_key
    where category is not null
    group by category
)
select category,sum(total_sales ) over() sum
, CAST(total_sales as float) /(sum(total_sales ) over())*100 as percentage
 from cte_category_sale 
