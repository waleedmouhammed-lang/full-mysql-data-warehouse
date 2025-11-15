-- This is how we are going to start building the new data warehouse using the medallian data architecture
-- Creating the bronze layer database

CREATE DATABASE IF NOT EXISTS dw_bronze; 

CREATE DATABASE IF NOT EXISTS dw_sliver;

CREATE DATABASE IF NOT EXISTS dw_gold;