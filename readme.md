# NYT Summary Tool
A tool to discover the latest NYT bestsellers and new movie reviews. 

## Contents

1. [Overview](#overview)
1. [The architecture](#the-architecture)
1. [Built with](#built-with)
1. [Next steps](#next-steps)
1. [Author](#author)

## Overview
Build an end to end process to process, transform, store and visualize the latest NYT bestseller and movie review data. 

### What's the problem?
Would like to be able to discover new books, top stories, and movies as they are released. 

### The Idea
Leverage the New York Times APIs to summarize interesting book and movie information to find new titles to explore.

### Bringing the idea to life
Create an ETL process to store and summarize data in AWS - build a dashboard on top of this data for some fun stats. 

## The Architecture
Insert image here

### ETL flow
1. Load data from NYT bestsellers and movie reviews API into AWS S3 bucket
2. Create base tables in AWS RDS
3. Load and summarize data from S3 to RDS (Postgres)
4. Use an analytics tool to create a dashboard with data from the RDS tables

### Infrastructure
The project is self-contained within a docker-compose container and hosted using the AWS ecosystem. An RDS postgres database (t2.micro) contains all the data, a docker-compose container is hosted on ECS and a metabase dashboard is hosted on elastic beanstalk. 

See below for a snapshot of the resources used:

### Dashboard
A publically accessible dashboard can be found here:

With screenshots below:

## Built With
Python, SQL, Docker, AWS S3, AWS RDS (Postgres), AWS ECS

## Next Steps
<s>Develop ETLs and tests for NYT bestsellers and movie reviews APIs</s>
<s>Create a process to load into S3</s>
<s>Schedule using airflow</s>
<s>Move all proceses to a local docker-compose container</s>
<s>Add SQL logic to create schemas/tables and read from postgres db</s>
<s>Create process to load csv files from S3 into postgres raw tables</s>
<s>Finalize table staging and prod table structure and add logic to load</s>
Add historical data and connect metabase to dashboard
Time permitting, add additional data and fields to the existing tables for a richer dataset.

## Author
:wave:
Alex Kruczkowski