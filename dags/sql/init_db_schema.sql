
CREATE SCHEMA if not exists raw;
CREATE SCHEMA if not exists staging;
CREATE SCHEMA if not exists prod;

--DROP TABLE raw.movies_data;
CREATE TABLE if not exists raw.movies_data (
	display_title text,
	critic text,
	description text,
	recommend text,
	opening_date timestamp,
	publication_date timestamp,
	mpaa_rating text,
	nyt_review_url text
);

--DROP TABLE raw.bestsellers_data;
CREATE table if not exists raw.bestsellers_data (
	titles text,
	authors text,
	descriptions text,
	ranks text,
	last_week_ranks text,
	weeks_on_list text,
	bestseller_date timestamp,
	amazon_url text
);

--DROP TABLE staging.movies_data;
CREATE TABLE if not exists staging.movies_data (
	display_title text,
	critic text,
	description text,
	recommend text,
	opening_date timestamp,
	publication_date timestamp,
	mpaa_rating text,
	nyt_review_url text
);

--DROP TABLE staging.bestsellers_data;
CREATE table if not exists staging.bestsellers_data (
	titles text,
	authors text,
	descriptions text,
	ranks text,
	last_week_ranks text,
	weeks_on_list text,
	bestseller_date timestamp,
	amazon_url text
);

--DROP TABLE staging.dim_date;
CREATE TABLE if not exists staging.dim_date(
	date date,
	year int,
	year_quarter int,
	month int,
	day int,
	weekend text
);

--DROP TABLE staging.dim_ratings;
CREATE TABLE if not exists staging.dim_ratings(
	rating text,
	description text,
	audience text,
	date_updated date
);

--DROP TABLE prod.movies_data;
CREATE TABLE if not exists prod.movies_data (
	display_title text,
	critic text,
	description text,
	recommend text,
	opening_date timestamp,
	publication_date timestamp,
	mpaa_rating text,
	nyt_review_url text
);

--DROP TABLE prod.bestsellers_data;
CREATE table if not exists prod.bestsellers_data (
	titles text,
	authors text,
	descriptions text,
	ranks text,
	last_week_ranks text,
	weeks_on_list text,
	bestseller_date timestamp,
	amazon_url text
);

--DROP TABLE prod.dim_date;
CREATE TABLE if not exists prod.dim_date(
	date date,
	year int,
	year_quarter int,
	month int,
	day int,
	weekend text
);

--DROP TABLE prod.dim_ratings;
CREATE TABLE if not exists prod.dim_ratings(
	rating text,
	description text,
	audience text,
	date_updated date
);