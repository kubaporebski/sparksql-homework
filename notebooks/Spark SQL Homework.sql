-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Spark SQL Homework notebook
-- MAGIC ## author: Jakub Porebski 
-- MAGIC
-- MAGIC Welcome to this notebook. Here I will present my solution to the following homework task.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task description
-- MAGIC
-- MAGIC * Create delta tables based on data in storage account.
-- MAGIC * Using Spark SQL calculate and visualize in Databricks Notebooks (for queries use hotel_id - join key, srch_ci- checkin, srch_co - checkout:
-- MAGIC   *  Top 10 hotels with max absolute temperature difference by month.
-- MAGIC   * Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
-- MAGIC   * For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

-- COMMAND ----------

-- DBTITLE 1,Here you have to set the google bucket name. It will be a result of executing a `terraform apply`
-- MAGIC %python
-- MAGIC # without a single slash at the end
-- MAGIC spark.conf.set("ob.BUCKET_PATH", "gs://storage-bucket-large-hedgehog")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's go to our Google Cloud bucket and see input data.
-- MAGIC
-- MAGIC ![](https://github.com/kubaporebski/sparksql-homework/blob/master/docs/input_data.png?raw=true)

-- COMMAND ----------

-- DBTITLE 1,Creation of a delta table `expedia`
create or replace table expedia
as
select * from 
avro.`${ob.BUCKET_PATH}/expedia/`;

describe detail expedia;

-- COMMAND ----------

-- DBTITLE 1,Creation of a delta table `hotel_weather`
create or replace table hotel_weather
as
select * from 
parquet.`${ob.BUCKET_PATH}/hotel-weather`;

describe detail hotel_weather;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's move on to the sub-tasks.

-- COMMAND ----------

-- DBTITLE 1,1. Top 10 hotels with max absolute temperature difference by month.
create or replace temp view v_top10_hotels_temp as 
select id as hotel_id, year, month, temp_diff, hotel_address
from (
	SELECT 
		id, year, month, round(abs(max(avg_tmpr_c) - min(avg_tmpr_c)), 2) as temp_diff, max(address) as hotel_address,
		ROW_NUMBER() over (partition by id order by abs(max(avg_tmpr_c) - min(avg_tmpr_c)) desc) as rn
	from hotel_weather 
	group by id, year, month
) T
where temp_diff > 0 and rn=1
order by temp_diff desc
limit 10
;

select * from v_top10_hotels_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt

-- COMMAND ----------

-- DBTITLE 1,Temperature difference visualized 
-- MAGIC %python
-- MAGIC # let's create pandas datasets for Seaborn plotting library
-- MAGIC pd_top10hotels = spark.sql("select hotel_address, temp_diff from v_top10_hotels_temp").toPandas()
-- MAGIC sns.barplot(y=pd_top10hotels["hotel_address"], x=pd_top10hotels["temp_diff"], orient="h")
-- MAGIC plt.xlabel('Temperature difference')
-- MAGIC plt.ylabel('Hotel')

-- COMMAND ----------

-- DBTITLE 1,2. Top 10 busy (e.g., with the biggest visits count) hotels for each month
-- first temporary view, which stores count of check-ins of a given hotel for each month
create or replace temp view v_tmp_expedia_checkins as
  with cte as (
    select *,
      concat(year(srch_ci), '/', right(concat('0', month(srch_ci)), 2))  as check_in_date
    from expedia
    where srch_ci is not null
  )
  select hotel_id, check_in_date, count(*) as check_ins, row_number() over (partition by check_in_date order by count(*) desc) as rn
  from cte
  group by hotel_id, check_in_date 
  order by check_in_date desc, check_ins desc
  ;

-- second temporary view for storing addresses of hotels, which are in the hotel_weather table
create or replace temp view v_tmp_hotel_address as
  select id as hotel_id, max(address) as address
    from hotel_weather
    group by id
  ;

-- combine two views together to achieve final result of this sub-task
create or replace temp view v_top10_busy_hotels as
select ci.hotel_id, check_in_date as check_in_year_month, check_ins, coalesce(ad.address, 'n/a') as hotel_address
from 
  v_tmp_expedia_checkins as ci 
  left join v_tmp_hotel_address as ad on ci.hotel_id = ad.hotel_id
where rn <= 10;

select * from v_top10_busy_hotels

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pd_top10busyhotels = spark.sql("select check_in_year_month as year_month, check_ins, hotel_id, hotel_address from v_top10_busy_hotels").toPandas()
-- MAGIC max_y, min_y = pd_top10busyhotels["check_ins"].max(), pd_top10busyhotels["check_ins"].min()
-- MAGIC
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC sns.barplot(x='year_month', y=pd_top10busyhotels['check_ins'], hue='hotel_address', data=pd_top10busyhotels)
-- MAGIC plt.title('Check ins by year/month and hotel')
-- MAGIC plt.xlabel('Year/month')
-- MAGIC plt.ylabel('Total check ins')
-- MAGIC plt.legend(title='Hotel ID',  bbox_to_anchor=(1.1, 1))
-- MAGIC plt.ylim(min_y - (0.01 * min_y), max_y + (0.01 * max_y))
-- MAGIC plt.show()

-- COMMAND ----------

-- DBTITLE 1,3. Weather trend for visits with extended stay
-- "long/extended stay" is defined as: above 7 days (8 and more days)

-- records with a long stay
create or replace temp view v_expedia_ext_stay_days as
select
  *, date_diff(srch_co, srch_ci) as stay_days, srch_ci as first_day, srch_co as last_day
from expedia
where date_diff(srch_co, srch_ci) > 7
;

-- weather for hotel stays on a first day of stay
create or replace temp view v_first_days as
select ex.id as expedia_id, hw.id as hotel_id, ex.first_day, hw.avg_tmpr_c 
from hotel_weather as hw join v_expedia_ext_stay_days as ex on hw.id=ex.hotel_id and hw.wthr_date=ex.first_day
order by hotel_id, first_day
;

-- weather for hotel stays on a last day of stay
create or replace temp view v_last_days as
select ex.id as expedia_id, hw.id as hotel_id, ex.last_day, hw.avg_tmpr_c 
from hotel_weather as hw join v_expedia_ext_stay_days as ex on hw.id=ex.hotel_id and hw.wthr_date=ex.last_day
order by hotel_id, last_day
;

-- calculating a temperature trend
create or replace temp view v_hotel_temp_trend as
select a.hotel_id, a.first_day, b.last_day, round(b.avg_tmpr_c - a.avg_tmpr_c, 2) as temp_trend
from v_first_days as a join v_last_days as b on a.expedia_id=b.expedia_id
;

-- final view for this task
create or replace temp view v_weather_trends as
select *
from 
  v_hotel_temp_trend as tr, 
  lateral (
    select round(avg(hw.avg_tmpr_c), 2) as avg_temp 
    from hotel_weather as hw 
    where 
      hw.id=tr.hotel_id 
      and hw.wthr_date between tr.first_day and tr.last_day
    ) 
order by tr.hotel_id, first_day
;

select * from v_weather_trends
;

-- COMMAND ----------

-- Following is my first version of above query
-- I didn't know then about LATERAL keyword (which simplifies thing and probably speeds them up also)

-- here it is: CROSS JOIN
-- calculating average temperature during a stay
create or replace temp view v_hotel_avg_temp as
select tr.hotel_id, round(avg(hw.avg_tmpr_c), 2) as avg_temp
from v_hotel_temp_trend as tr cross join hotel_weather as hw
where hw.id=tr.hotel_id and hw.wthr_date between tr.first_day and tr.last_day
group by tr.hotel_id
order by tr.hotel_id
;

-- view for this task
create or replace temp view v_weather_trends_alpha as
select distinct tr.*,  av.avg_temp as average_temp_during_stay
from v_hotel_temp_trend as tr join v_hotel_avg_temp as av on tr.hotel_id=av.hotel_id
order by tr.hotel_id, first_day
;

select * from v_weather_trends_alpha

-- COMMAND ----------

-- MAGIC %md
-- MAGIC And saving into DBFS directly. Just create tables, and that's it.
-- MAGIC Results will be persisted in tables which names begin with prefix `result_`.
-- MAGIC
-- MAGIC Temporary views won't be persisted at all, so after terminating the cluster, they're gone.

-- COMMAND ----------

create or replace table result_top10_hotels_temperature LOCATION '${ob.BUCKET_PATH}/result/top10_hotels_temperature' as select * from v_top10_hotels_temp;
create or replace table result_top10_busy_hotels LOCATION '${ob.BUCKET_PATH}/result/top10_busy_hotels' as select * from v_top10_busy_hotels;
create or replace table result_hotels_weather_trend LOCATION '${ob.BUCKET_PATH}/result/hotels_weather_trend' as select * from v_weather_trends;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View on the results in Google Cloud bucket:
-- MAGIC
-- MAGIC ![](https://github.com/kubaporebski/sparksql-homework/blob/master/docs/result_tables.png?raw=true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analysis of queries
-- MAGIC
-- MAGIC Now, we can perform analysis of each query. `explain` keyword will tell us what is going on inside the engine.
-- MAGIC

-- COMMAND ----------

explain select * from v_top10_hotels_temp

-- COMMAND ----------

explain select * from v_top10_busy_hotels

-- COMMAND ----------

explain select * from v_weather_trends

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `v_weather_trends` is a view that is build from query containing `lateral` joins. 
-- MAGIC Following however, is explain result of query `v_weather_trends_alpha`, which contains `cross join`. 
-- MAGIC
-- MAGIC Those two execution plans cleary are different from each other.

-- COMMAND ----------

explain select * from v_weather_trends_alpha

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I have Photon support enabled on the cluster thus my `explain` output contains everything Photon. Anyway, results here are physical plans.
-- MAGIC Starting from the very bottom, going upwards, we can see how each operation is applied after each.
-- MAGIC
-- MAGIC There can be also tree-like structure. It is seen in output of `explain select * from v_weather_trends`. 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Good job! 😎
