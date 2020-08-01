#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 20:54:15 2020

@author: peterfang
"""

import os
import configparser
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, year, month, dayofweek, hour
from pyspark.sql.types import IntegerType
from pyspark.sql.types import *


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_demographics_data(spark, path):
    
    """
    1. split demographc data that were originally stored in a wide column of a csv file.
    2. stringType need be converted into IntergerType for age, population et.al data
    3. The table need be normalized as redundancy caused by race data that have 5 categories.
        To normazlize the table, first create a pivot table that convert 5 categories of race into 5 columns.
        Then join the pivot table with the rest columns of demographics. 
        After normalization, the demographic table size reduces from more than 2000 rows into ~500 rows
    4. Save the data into S3 bucket with a format parquet 
    """
    
    df=spark.read.csv(path + "us_cities_demographics.csv", header=True)
    
    split_col = split(df['City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count'], ';')
    
    df=df.withColumn('city', split_col.getItem(0))\
                 .withColumn('state', split_col.getItem(1))\
                 .withColumn('median_age', split_col.getItem(2))\
                 .withColumn('male_population', split_col.getItem(3))\
                 .withColumn('female_population', split_col.getItem(4))\
                 .withColumn('total_population', split_col.getItem(5))\
                 .withColumn('number_of_veterans', split_col.getItem(6))\
                 .withColumn('foreign_born', split_col.getItem(7))\
                 .withColumn('average_household_size', split_col.getItem(8))\
                 .withColumn('state_code', split_col.getItem(9))\
                 .withColumn('race', split_col.getItem(10))\
                 .withColumn('count', split_col.getItem(11))
                 
    df=df.drop('City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count')
    
    df=df.withColumn("median_age", df["median_age"].cast(IntegerType()))\
                .withColumn("male_population", df["male_population"].cast(IntegerType()))\
                .withColumn("female_population", df["female_population"].cast(IntegerType()))\
                .withColumn("total_population", df["total_population"].cast(IntegerType()))\
                .withColumn("number_of_veterans", df["number_of_veterans"].cast(IntegerType()))\
                .withColumn("foreign_born", df["foreign_born"].cast(IntegerType()))\
                .withColumn("average_household_size", df["average_household_size"].cast(IntegerType()))\
                .withColumn("count", df["count"].cast(IntegerType()))
    
    df_for_pivot=df.select('city','race','count')
    df_for_pivot=df_for_pivot.groupBy('city').pivot('race', ['Hispanic or Latino','White','Black or African-American','American Indian and Alaska Native','Asian']).sum('count')
    
    df_split_join=df.select('city','state','median_age','male_population','female_population',
                              'total_population','number_of_veterans','foreign_born','average_household_size','state_code')\
                             .dropDuplicates()
    
    df_join=df_for_pivot.join(df_split_join, on=['city'], how='left')
    
    df_join_save=df_join.select(df_join.city, df_join.state, df_join.state_code, df_join.median_age,
                                df_join.male_population, df_join.female_population, 
                                df_join.total_population, df_join.number_of_veterans, df_join.foreign_born, 
                                df_join.average_household_size,
                                col("Hispanic or Latino").alias("Hispanic_or_Latino"),df_join.White,
                                col("Black or African-American").alias("Black_or_African_American"),
                                col("American Indian and Alaska Native").alias("American_Indian_and_Alaska_Native"),df_join.Asian)
    
    df_join_save.write.parquet(path + "demographics/demographics.parquet", mode="overwrite")
    
    
def process_accident_data(spark, path):
    """
    Process the accident data
    1. parse the Weather_Timestamp data into timestamp format
    2. extract hour,month,weekday, and year from the timestamp data for easy of analytic query
    3. Save the data into S3 bucket with a format parquet
    """
    
    df=spark.read.csv(path + "US_Accidents_June20.csv", header=True)

    df=df.select(col("ID").alias("accident_id"), col("Street").alias("street"), 
                   col("City").alias("city"), col("County").alias("county"),
                   col("State").alias("state"), col("Zipcode").alias("zipcode"), 
                   col("Airport_Code").alias("airport_code_id"), 
                   col("Weather_Timestamp").alias("weather_timestamp"))
    
    
    df=df.withColumn("timestamp", F.to_timestamp("weather_timestamp", "yyyy-MM-dd HH:mm:ss"))\
                   .withColumn("hour", hour("timestamp"))\
                   .withColumn("month", month("timestamp"))\
                   .withColumn("weekday", dayofweek("timestamp"))\
                   .withColumn("year", year("timestamp"))
     
    df.write.parquet(path + "accident/accident.parquet", mode="overwrite")                  
                       
                       
def process_weather_data(spark, path):
    """
    Process the weather data
    """ 
                      
    df=spark.read.csv(path + "US_Accidents_June20.csv", header=True) 
    
    df=df.select(col("ID").alias("accident_id"),col("Temperature(F)").alias("temperature"),
                     col("Wind_Chill(F)").alias("wind_chill"), col("Humidity(%)").alias("humidity"), 
                     col("Pressure(in)").alias("pressure"), col("Visibility(mi)").alias("visibility"), 
                     col("Wind_Direction").alias("wind_detection"), col("Wind_Speed(mph)").alias("wind_speed"),
                     col("Precipitation(in)").alias("precipitation"), col("Weather_Condition").alias("weather_condition"),
                     col("Bump").alias("bump"), col("Crossing").alias("crossing"), col("Sunrise_Sunset").alias("sunrise_sunset"))
    
    df.write.parquet(path + "weather/weather.parquet", mode="overwrite")
    
    
def process_income_data(spark, path):
    """
    Process the income data
    """     
    df=spark.read.csv(path + "Median_income_zip.csv", header=True)
    
    df=df.select(col("Zip").alias("zip"), col("Median_income").alias("median_income"), 
                           col("Mean_income").alias("mean_income"),col("Population").alias("population") )
    
    df.write.parquet(path + "income/income.parquet", mode="overwrite")
                

def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    
    s3_path="s3://*/traffic_accident/" 
    
    process_demographics_data(spark, s3_path)  
    process_accident_data(spark, s3_path)
    process_weather_data(spark, s3_path)
    process_income_data(spark, s3_path)
    
if __name__ == "__main__":
    main()
  