
-- to do: add staging and prod load for dim_date and dim_ratings

--populate movies staging table

insert into staging.movies_data (display_title, critic, description, recommend,
	opening_date, publication_date, mpaa_rating, nyt_review_url);

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
	ranks, last_week_ranks, weeks_on_list, bestseller_date, amazon_url);

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


-- populate movies prod table

insert into prod.movies_data (display_title, critic, description,
	recommend, opening_date, publication_date, mpaa_rating, nyt_review_url);

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
	ranks, last_week_ranks, weeks_on_list, bestseller_date, amazon_url);

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