# NYT Summary Tool
A tool to discover the latest NYT bestsellers, top articles, and new movie reviews. 

## Contents

1. [Overview](#overview)
1. [The architecture](#the-architecture)
1. [Project demo](#project-demo)
1. [Built with](#built-with)
1. [Next steps](#next-steps)
1. [Author](#author)

## Overview

### What's the problem?
Would like to be able to discover new books, top stories, and movies as they are released. 

### The Idea
Leverage the New York Times APIs to summarize interesting book and movie information to find new titles to explore.

### Bringing the idea to life
Create an ETL process to store and summarize data in AWS - build a dashboard on top of this data for some fun stats. 

## The Architecture
1. Load data from NYT bestsellers and movie reviews API into AWS S3 bucket
2. Create base tables in AWS RDS
3. Load and summarize data from S3 to RDS
4. Use an analytics tool to create a dashboard with data from the RDS tables

## Project Demo

## Built With
Docker, AWS S3, AWS RDS, Python, SQL (tbd)

## Next Steps

## Author
:wave:
Alex Kruczkowski