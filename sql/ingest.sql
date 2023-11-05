-- Author: Sreemoyee Mukherjee (sreemoym)
-- Turning command echo on
.echo on
-- Specifying the file search path for all project data
SET file_search_path TO 'C:\Users\sreem\Downloads\Data_Warehousing\project_data\data';
-- An extra filename column should be included in each table as filename is set to true for each table creation command
-- reading .csv.gz compressed file
CREATE TABLE bike_data AS
    SELECT * FROM read_csv_auto('citibike-tripdata.csv.gz',
    filename=true, all_varchar=1, union_by_name=True);
-- reading .csv files
CREATE TABLE central_park_weather AS
    SELECT * FROM read_csv_auto('central_park_weather.csv',
    filename=true, all_varchar=1, union_by_name=True);
CREATE TABLE fhv_bases AS
    SELECT * FROM read_csv_auto('fhv_bases.csv',
    HEADER=true, filename=true, all_varchar=1, union_by_name=True);
-- reading .parquet files
CREATE TABLE fhv_tripdata AS
    SELECT * FROM read_parquet('taxi\fhv_tripdata.parquet',
    filename=true, union_by_name=True);
CREATE TABLE fhvhv_tripdata AS
    SELECT * FROM read_parquet('taxi\fhvhv_tripdata.parquet',
    filename=true, union_by_name=True);
CREATE TABLE green_tripdata AS
    SELECT * FROM read_parquet('taxi\green_tripdata.parquet',
    filename=true, union_by_name=True);
CREATE TABLE yellow_tripdata AS
    SELECT * FROM read_parquet('taxi\yellow_tripdata.parquet',
    filename=true, union_by_name=True);