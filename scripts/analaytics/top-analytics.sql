
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
