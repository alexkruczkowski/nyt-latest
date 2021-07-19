
--populate movies staging table

insert into staging.movies_data (display_title, critic, description, recommend,
	opening_date, publication_date, mpaa_rating, nyt_review_url)

select 
	display_title, 
	critic, 
	description, 
	recommend,
	opening_date, 
	publication_date, 
	mpaa_rating, 
	nyt_review_url
from raw.movies_data;


--populate bestsellers staging table

insert into staging.bestsellers_data (titles, authors, descriptions,
	ranks, last_week_ranks, weeks_on_list, bestseller_date, amazon_url)

select 
	titles,
	authors,
	descriptions,
	ranks,
	last_week_ranks,
	weeks_on_list,
	bestseller_date,
	amazon_url
from raw.bestsellers_data;

-- populate staging ratings table

insert into staging.dim_ratings(rating, description, audience, date_updated)

select distinct mpaa_rating as rating,
	case when mpaa_rating = 'G' then 'General audiences'
	when mpaa_rating = 'GP' then 'Approved for general audiences' 
	when mpaa_rating = 'Approved' then 'Approved for all audiences'
	when mpaa_rating = 'PG' then 'Parental guidance suggested, movie'
	when mpaa_rating = 'TV-PG' then 'Parental guidance suggested, TV'
	when mpaa_rating = 'TV-14' then 'Parents strongly cautioned, TV'
	when mpaa_rating = 'PG-13' then 'Parents strongly cautioned, movie'
	when mpaa_rating = 'Not Rated' then 'Not rated'
	when mpaa_rating = 'R' then 'Restricted'
	when mpaa_rating = 'TV-MA' then 'TV mature audiences only'
	when mpaa_rating = 'Unrated' then 'Not rated'
	else '' end as description,
	case when mpaa_rating = 'G' then 'All audiences' 
	when mpaa_rating = 'GP' then 'General audiences' 
	when mpaa_rating = 'Approved' then 'Approved for all audiences'
	when mpaa_rating = 'PG' then 'Some material may not be suitable for children'
	when mpaa_rating = 'TV-PG' then 'Some material may not be suitable for children'
	when mpaa_rating = 'TV-14' then 'May be unsuitable for children under 14 years of age'
	when mpaa_rating = 'PG-13' then 'Not suitable for audiences under 13'
	when mpaa_rating = 'Not Rated' then 'Unknown'
	when mpaa_rating = 'R' then 'For audiences 18 and above'
	when mpaa_rating = 'TV-MA' then 'For audiences 18 and above'
	when mpaa_rating = 'Unrated' then 'Unknown'
	else '' end as audience,
	CURRENT_DATE as date_updated
from raw.movies_data;

-- populate rim_date staging table

insert into staging.dim_date(date, year, year_quarter, month, day, weekend)

select 
	datum as date,
	EXTRACT(YEAR FROM datum) as year,
	TO_CHAR(datum, 'yyyy/"Q"Q') as year_quarter,
	EXTRACT(MONTH FROM datum) as month,
	EXTRACT(DAY FROM datum) as day,
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend
FROM (
	SELECT '2020-01-01'::date + SEQUENCE.DAY as datum
	FROM generate_series(0,1095) as SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ;

-- populate movies prod table

insert into prod.movies_data (display_title, critic, description,
	recommend, opening_date, publication_date, mpaa_rating, nyt_review_url)

select 
	display_title,
	critic,
	description,
	recommend,
	opening_date,
	publication_date,
	mpaa_rating,
	nyt_review_url
from staging.movies_data;

-- populate bestsellers prod table

insert into prod.bestsellers_data (titles, authors, descriptions,
	ranks, last_week_ranks, weeks_on_list, bestseller_date, amazon_url)

select 
	titles,
	authors,
	descriptions,
	ranks,
	last_week_ranks,
	weeks_on_list,
	bestseller_date,
	amazon_url
from staging.bestsellers_data;

-- populate ratings prod table

insert into prod.dim_ratings (rating, description, audience, date_updated)

select 
	rating,
	description,
	audience,
	date_updated
from staging.dim_ratings;

-- populate date prod table

insert into prod.dim_date(date, year, year_quarter, month, day, weekend)

select 
	date,
	year,
	year_quarter,
	month,
	day,
	weekend
from staging.dim_date;