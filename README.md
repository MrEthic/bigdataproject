# Big Data Project

## Introduction

We decided here to study the tweets about the french parliements election of 2022. Our goal is to
extract and load tweets talking about those elections in near real-time. To process them and visualize them
on a dashboard.


We've decided to use the following stack :
- Python as our developpement language
- Kafka for Stream handling
- Spark (pyspark) for in-memory data processing
- Airflow for ETL orchestration
- MongoDB Atlas as our database
- Mongo Charts as our visualization tool
- Local Filesystem as our raw datalake

## Solution Architecture

![Solution](/solution.png)
