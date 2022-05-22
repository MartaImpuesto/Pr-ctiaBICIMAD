import json
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext


spark = SparkSession.builder.getOrCreate()


def main():
    files = ["20"+str(i)+"_movements.json" for i in [1908, 1909,
                                                     1910, 1911, 1912, 2001, 2002, 2003, 2004, 2005, 2006, 2007]]
    df = spark.read.json(files)
    df.createGlobalTempView("data")
    spark.sql("select * from global_temp.data")
    spark.sql("select count(*),substr(unplug_hourTime,1,7) as fecha from global_temp.data group by fecha ").show()
    spark.sql(
        "select distinct user_type from global_temp.data  ")
    spark.sql(
        "select substr(unplug_hourTime,1,7) as fecha, sum(case when user_type = 1 then 1 else 0 end)*100 / count(*) as porcentaje1, sum(case when user_type = 2 then 1 else 0 end)*100 / count(*) as porcentaje2, sum(case when user_type = 3 then 1 else 0 end)*100 / count(*)   as porcentaje3 from global_temp.data group by fecha order by fecha").show()
    spark.sql(
        "select  mean(travel_time), substr(unplug_hourTime,1,7) as fecha from global_temp.data  where travel_time < 10000 group by fecha order by fecha  ").show()
    spark.sql(
        "select  mean(travel_time), user_type, substr(unplug_hourTime,1,7) as fecha from global_temp.data  where travel_time < 10000 group by fecha, user_type order by fecha,user_type  ").show(36)


if __name__ == "__main__":
    main()
