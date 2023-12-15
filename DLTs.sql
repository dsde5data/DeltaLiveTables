-- Databricks notebook source
create or refresh streaming live table br_countries
as
select * from cloud_files('/Volumes/main/default/vol_ext/Countries', 'csv')

-- COMMAND ----------

create or refresh live table silv_countries
(constraint valid_id expect (countryid is not null) on violation drop row )
as select * from (live.br_countries)

-- COMMAND ----------

create or refresh live table countries_agg
as select region, count(*) count_regional from live.silv_countries
group by region

-- COMMAND ----------


