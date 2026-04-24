
SELECT name 
FROM sys.databases;
USE swiggy_data;
GO
select * from swiggy_data 

select count(*) AS TOTAL_ROWS from swiggy_data 
 --there are above swiggy data total  197401 rows 

---Data validation & cleaning
--null check
select
	
	sum(case when state IS NULL THEN 1 ELSE 0 END) as null_state,
	sum(case when 1 IS NULL THEN 1 ELSE 0 END) as null_city,
	sum(case when order_Date IS NULL THEN 1 ELSE 0 END) as null_order_date,
	sum(case when Restaurant_Name IS NULL THEN 1 ELSE 0 END) as null_restaurant,
	sum(case when location IS NULL THEN 1 ELSE 0 END) as null_location,
	sum(case when category IS NULL THEN 1 ELSE 0 END) as null_category,
	sum(case when Dish_Name IS NULL THEN 1 ELSE 0 END) as null_dish,
	sum(case when price_INR IS NULL THEN 1 ELSE 0 END) as null_price,
	sum(case when Rating IS NULL THEN 1 ELSE 0 END) as null_rating,
	sum(case when Rating_count IS NULL THEN 1 ELSE 0 END) as null_rating_count
from swiggy_data;



--Blank or empty strings
select * from swiggy_data 
where state='' or '1' ='' or Restaurant_Name='' or loCation ='' or Category=''

or Dish_Name='' or Price_INR='' or Rating='' or Rating_count=''

--Duplicate Detection
select
state,
city,
order_date,restaurant_name,location,category, dish_name,price_INR,rating,
rating_count,count(*) as CNT
from swiggy_data
group by
state,
city,
order_date,restaurant_name,location,category,
dish_name,price_INR,rating,rating_count
having count(*)>1

---delete duplication

with cte as (
select *,row_number() over(
partition by state,
city,
order_date,restaurant_name,location,category, dish_name,price_INR,rating,
rating_count
order by (select null)
) as rn
from swiggy_data )
delete from cte where rn>1

---creating schema
--Dimension tables
--date table
create table dim_date(
date_id int identity(1,1) primary key,
full_date Date,
year int,
month int,
month_name varchar(20),
quarter int,
day int,
week int
)
select* from dim_date

 




---dim_location
create table dim_location(
	location_id int identity(1,1) primary key,
	state varchar(100),
	city varchar(100),
	location varchar(200)
	);
	select * from dim_location;
--dim__restaurant
create table dim_restaurant(
		restaurant_id  int identity(1,1) primary key,
		restaurant_name varchar(200));

select * from dim_restaurant
--dim_category
create table dim_category(
	category_id int identity(1,1) primary key,
	category varchar(200) );
select * from dim_category;
--dim_dish
create table dim_dish(
dish_id  int identity(1,1) primary key,
dish_name varchar(200))

select * from dim_dish
--FACT TABLE

create table fect_swiggy_order(
	order_id int identity(1,1) primary key,
	date_id int,
	price_INR decimal(10,2),
	Rating decimal(4,2),
	Rating_count int,

	location_id int,
	Restaurant_id int,
	category_id int,
	dish_id int,

	foreign key(date_id) references dim_date(date_id),
	foreign key(location_id) references dim_location(location_id),
	foreign key(restaurant_id) references dim_restaurant(restaurant_id),
	foreign key(category_id) references dim_category(category_id),
	foreign key(dish_id) references dim_dish(dish_id)
	);

select * from fect_swiggy_order

---insert data in tables
--dim date
insert into dim_date(full_date,year,month,month_name,quarter,dat,week)
select distinct
order_date,
year(order_date),
month(order_date),
datename(month,order_date),
datepart(quarter,order_date),
day(order_date),
datepart(week,order_date)
from swiggy_data 
where order_date is not null;

select * from dim_date

---dim_location
insert into dim_location(state,city,location)
select distinct
state,
city,
location from swiggy_data
select * from dim_location

--dim _restaurant
insert into dim_restaurant(restaurant_name)
select distinct
restaurant_name from swiggy_data

select * from dim_restaurant

--dim_category
insert into dim_category(category)
select distinct
category
from swiggy_data

select * from dim_category

--dim dish
insert into dim_dish(dish_name)
select distinct
dish_name from swiggy_data

select * from dim_dish


--fact table

insert into fect_swiggy_order(
	date_id,
	price_INR,
	Rating,
	Rating_count,
	location_id,
	Restaurant_id,
	category_id,
	dish_id
)
select
    dd.date_id,
	s.price_INR,
	s.Rating,
	s.Rating_count,

	dl.location_id,
	dr.Restaurant_id,
	dc.category_id,
	dsh.dish_id
	from swiggy_data  s
join dim_date dd on dd.full_date=s.Order_Date
join dim_location dl 
on dl.state=s.state
and dl.city=s.city
and dl.location=s.location
join dim_restaurant  dr
on dr.restaurant_name=s.Restaurant_Name
join dim_category dc
on dc.category=s.category
join dim_dish dsh
on dsh.dish_name=s.Dish_Name;

select * from fect_swiggy_order

select * from fect_swiggy_order f
join dim_date d on f.date_id=d.date_id
join dim_location l on f.location_id=l.location_id
join dim_restaurant r on f.restaurant_id=r.restaurant_id
join dim_category c on f.category_id=c.category_id
join dim_dish di on f.dish_id=di.dish_id

--kpI's
--total orders
select count(*) as total_order
from fect_swiggy_order
--total fact table order in 394802

--total revenue(INR million)
select sum(price_INR) as total_revenue from fect_swiggy_order

select
format(sum(convert(float,price_INR))/1000000,'N2')+'INR million' 
as total_revenue from fect_swiggy_order


--average dish price

select
format(avg(convert(float,price_INR)),'N2')+'INR ' 
as total_revenue from fect_swiggy_order
---average rating
select 
avg(rating) as avg_rating 
from fect_swiggy_order


--Deep-Dive business Analysis
--monthly orders

select
d.year,
d.month,
d.month_name,
count(*) as total_orders
from fect_swiggy_order f join dim_date d 
on f.date_id=d.date_id
group by d.year,
d.month,
d.month_name
order by count(*) desc
--total revenue
select
d.year,
d.month,
d.month_name,
sum(price_INR) as total_revenue
from fect_swiggy_order f join dim_date d 
on f.date_id=d.date_id
group by d.year,
d.month,
d.month_name
order by sum(price_INR) desc


--Quaterly trend
select
d.year,
d.quarter,
count(*) as total_orders
from fect_swiggy_order f join dim_date d 
on f.date_id=d.date_id
group by d.year,
d.quarter
order by count(*) desc

--Yearly Trend

select
d.year,
count(*) as total_orders
from fect_swiggy_order f join dim_date d 
on f.date_id=d.date_id
group by d.year
order by count(*) desc

-- orders by day of week(mon-sun)

select 
	datename(weekday,d.full_date) as day_name,
	count(*) as total_orders
from fect_swiggy_order f join dim_date d 
on f.date_id=d.date_id
group by datename(weekday,d.full_date)
,datepart(weekday,d.full_date)
order by datename(weekday,d.full_date) desc

---top 10 cities by order volume
select 
 top 10 l.city,
count(*) as total_orders from fect_swiggy_order f
join dim_location l
on l.location_id=f.location_id
group by l.city 
order by count(*) desc

--total revenue
select 
 top 10 l.city,
sum(price_INR) as total_revenue from fect_swiggy_order f
join dim_location l
on l.location_id=f.location_id
group by l.city 
order by sum(price_INR) ASC

--REVENUE CONTRIBUTION BY STATES
select 
  l.state,
sum(price_INR) as total_revenue from fect_swiggy_order f
join dim_location l
on l.location_id=f.location_id
group by l.state 
order by sum(price_INR) desc

--total 10 restaurants by orders
select 
 top 10 r.restaurant_name,
sum(price_INR) as total_revenue from fect_swiggy_order f
join dim_restaurant r
on r.restaurant_id=f.restaurant_id
group by  r.restaurant_name
order by sum(price_INR) desc

--top categories by order volume
select
c.category,
count(*) as total_orders
from fect_swiggy_order f
join dim_category c
on f.category_id=c.category_id
group by c.category
order by total_orders desc

--most ordered dishes
select 
top 10 d.dish_name,
count(*) as total_orders
from fect_swiggy_order f
join dim_dish d
on f.dish_id=d.dish_id
group by d.dish_name
order by total_orders desc

--cuisine performance(order + avg rating)
select
c.category,
count(*) as total_orders,
avg(f.rating) as avg_rating
from fect_swiggy_order f
join dim_category c
on f.category_id=c.category_id
group by c.category
order by total_orders desc



select
	case
		when convert(float,price_inr)<100 then 'under 100'
		when convert(float,price_inr) between 100 and 199 then '100-199'
		when convert(float,price_inr) between 200 and 299 then '200-299'
		when convert(float,price_inr) between 300 and 499 then '300-499'
		else '500+'
	end as price_range,
	count(*) as total_orders
	from fect_swiggy_order
	group by
		case	
			when convert(float,price_inr)<100 then 'under 100'
			when convert(float,price_inr) between 100 and 199 then '100-199'
			when convert(float,price_inr) between 200 and 299 then '200-299'
			when convert(float,price_inr) between 300 and 499 then '300-499'
			else '500+'
		end 
		order by total_orders desc;
--rating count distributtion
select
rating,
count(*) as rating_count
from fect_swiggy_order
group by rating
order by  count(*) desc;