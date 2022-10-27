use ejmcdbstg;
select * from tra_channel;
select * from tra_products;
select * from tra_sales;
select * from tra_countries;
select * from tra_customers;
select * from tra_promotions;
select * from tra_times;
SELECT CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID from channels;

use ejmcdbsor;
select * from dim_channel;
select * from dim_times;
select * from dim_promotions;
select * from dim_countries;

use ejmcdbstg;
select count(*) from tra_channel;
select count(*) from tra_products;
select count(*) from tra_sales;
select count(*) from tra_countries;
select count(*) from tra_customers;
select count(*) from tra_promotions;
select count(*) from tra_times;