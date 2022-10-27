use ejmcdbstg;
select count(*) from channels;
select count(*) from customers;  
select count(*) from countries;
select count(*) from products;
select count(*) from promotions;
select count(*) from sales;
select count(*) from times;

use ejmcdbsor;
select count(*) from dim_channel;
select count(*) from dim_countries;
select count(*) from dim_customers;
select count(*) from dim_products;
select count(*) from dim_promotions;
select count(*) from dim_sales;
select count(*) from dim_times;