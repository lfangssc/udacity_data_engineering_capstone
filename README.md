# udacity_data_engineering_capstone_datalake

# Project Title: US Annual Traffic Accidents and the Infulence of Weather, Demographic and Household Income

## I. Scope the Project and Gather Data
   ### Scope
   
   1. The project aims to build a datalake that stores and organizes the data of US traffic accidents and the related  weather, demographic and household income data. This datalake will be useful for data scientist and data analyst to gain deep knowlege on which factors are likely to cause traffic accidents so a predicatable model can be built using the data and the latest AI technologies. 
   
   
   2. The datalake will be setup on AWS Elastic MapReduce (EMR), Apache pySpark will be used for data cleaning and data modelling. The data modelling adopts the conventional methods of data normalization and "star schema". The orginal data have CSV format and are stored in AWS S3 buckets. The processed data have parquet format and are stored in AWS S3 buckets, which can be accessed by users via Spark notebook or AWS service Athena for data analysis.
   
   
   
   3. technology stack


        - AWS Elastic MapReduce (EMR): 1 master (m5.xlarge) and 2 workers (m5.xlarge)
        - Apache pySpark SQL and DataFrame API
        - Data lake methodology: S3+EMR+Spark and "Schema on read"
        - Data modelling: normalization and "Star Schema"
        - Data storage: S3 and columnar stoarge parquet
        - Spark job submission in bash
        - Data analysis: EMR jupyter and/or Athena
   
   ### Data Sources
   
   The data are:
   1. US_Accidents_June20.csv 
      This dataset is countrywide car accident data that have details about traffic and weather information. The data come from [Kaggle](https://www.kaggle.com/sobhanmoosavi/us-accidents). The data has 3.5 million records and the size of data is about 1.3G. In this project, the data will be split into two tables, one is the accident information for fact table, another part focus on weather data that will be stored in a dimension table.
      
      
   2. us-cities-demographics.csv
      This dataset comes from Udacity Data Engineering capstone project. The data has geographic and demographic information of more than 500 major US cities, including male/female/immigrant population data. A similar dataset that have more data can be accessed publicly is at [opendatasoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/table/). In this project, the Udacity dataset will be used, partly because that the data need more data engieering, rather than use the clean data from Opendatasoft.
      
      
   3. Median_income_zip.csv
      This dataset comes from the Population Studies Center at the University of Michigan [Mean and Median household Income](https://www.psc.isr.umich.edu/dis/census/Features/tract2zip/).
      
      
## II. Explore and Assess the Data

1. Wide column data:
   The us-cities-demographics.csv from Udacity needs data engineering as all data are stored in a wide column. The wide column needs be splitted and data need be stored in different columns.
   

        Row(City;State;Median Age;Male Population;Female Population;Total Population;Number of Veterans;Foreign-born;Average Household Size;State Code;Race;Count='Silver Spring;Maryland;33.8;40601;41862;82463;1562;30908;2.6;MD;Hispanic or Latino;25924')
    
    
    
    
2. Data normalization:
   The us-cities-demographics.csv also has data redundancy issue as the "race" column has 5 categories of "'Hispanic or Latino','White','Black or African-American','American Indian and Alaska Native','Asian' ". But the rest columns corresponding to the 5 categories, such as female and male populations, are identical. To handle the data redundancy issue, a pivot table is used to convert the 5 categories of race data into 5 different columns. Once a pivot table was created, drop the duplicates data to reduce the orginal 2000 rows into 500 rows.
   
   
   
3. Hour/month/weekday extraction:
   The time data in US_Accidents_June20.csv is a string type, and needs be parsed into timestamp and extract hour/month/weekday data for data analysis convenience. 
   
   
   
## III. Data Model

### Conceptual Data Model

1. The data model adopts the traditional "Star Schema" for easy data query. The data model consists of 1 fact table and 3 dimension tables. The fact table contains all traffic records and keys that are used for join with the dimension tables. The dimension tables are "weather", "demographics" and "income". Each table has key for join with the fact table. Query optimization techniques like partitionBy is omit here as the data set in this project is small.  


Fact table: accident

         accident_id: string  (key)
         street: string 
         city: string         (key)
         county: string 
         state: string 
         zipcode: string      (key)
         airport_code_id: string 
         weather_timestamp: string 
         timestamp: timestamp 
         hour: integer 
         month: integer 
         weekday: integer 
         year: integer 
         
         
 Dimensional table: demographics

        city: string        (key)
        state: string 
        state_code: string 
        median_age: integer 
        male_population: integer 
        female_population: integer 
        total_population: integer 
        number_of_veterans: integer 
        foreign_born: integer 
        average_household_size: integer 
        Hispanic_or_Latino: long 
        White: long 
        Black_or_African_American: long 
        American_Indian_and_Alaska_Native: long 
        Asian: long 
        
 Dimensional table: weather
 
        accident_id: string    (key)
        temperature: string 
        wind_chill: string 
        humidity: string 
        pressure: string 
        visibility: string 
        wind_detection: string 
        wind_speed: string 
        precipitation: string 
        weather_condition: string 
        bump: string 
        crossing: string 
        sunrise_sunset: string 
        
 Dimensional table: income
 
        zip: string         (key)
        median_income: integer 
        mean_income: integer 
        population: integer 
        
        
###  Mapping Out Data Pipelines


1. Load csv data from S3 into EMR
    - US_Accidents_June20.csv
    - us-cities-demographics.csv
    - Median_income_zip.csv
    
    
2. Transform us-cities-demographics.csv into "demographics" table


3. Transform/extract US_Accidents_June20.csv into "accident" table


4. Transform US_Accidents_June20.csv into "weather" table


5. Transform Median_income_zip.csv into "income" table


6. write tables into parquet and saved into S3 bucket
    - demographics.parquet
    - accident.parquet
    - weather.parquet
    - income.parquet
    
    
## IV. Run Pipelines to Model the Data


1. Create the data model by submit the traffic_spark_lake.py in EMR


2. Data Quality Checks: To see the capstone_QA.ipynb, which read the parquet data, and count, join, sort the data. showing succesful data processing has been conducted by Spark.


3. Data dictionary: To see the Traffic_accident_data_dictionary.csv

## V. File descriptions

 - traffic_spark_lake.py : py file for spark job submission.
 - dl.cfg : configure file of AWS secret id and secret key.
 - capstone_QA.ipynb: data quality check file to ensure all parquet files are right and can be joined.
 - US_Accidents_June20.zip: original data of traffic accident.
 - Median_income_zip.csv: original income data.
 - us-cities-demographics.csv: original demographic data.
 - Traffic_accident_data_dictionary.csv: data dictionary.

## VI. Spark Job Deployment


1. Copy traffic_spark_lake.py and dl.cfg from local into EMR master


        $ scp -i /path/*.pem /path/traffic_spark_lake.py hadoop@<emr endpoint>:~/.
        
        $ scp -i /path/*.pem /path/dl.cfg hadoop@<emr endpoint>:~/.
        

2. Deploy in emr bash. It takes few minutes for data processing of the project.


        $ /usr/bin/spark-submit --master yarn ./traffic_spark_lake.py
        
        
## VII. Complete Project Write Up

1.  The rationale for the choice of tools and technologies for the project.


- EMR+Spark is state-of-the-art technology for big data processing, analysis and machine leanring model development. Spark is mermory-based computation, and therefore is purported as one of the most fast tools for big data. 


- Data modeling adpots the traditional data warehouse methods such as star-schema and data normalization, therefore is very straightforward for SQL query.


- The processed data were saved as a Parquet format which uses columnar storage technique that can saves lots of disk storage space and also speed up query. For example, the original traffic accident data is 1.3G, after stored in parquet, the total size is only 100Mb, which is 10 times smaller than the original csv file.


2. Data dalily update 7:00 am every day.

- Depends on business need, the spark job can be scheduled every day.
- The most strightfoward method is setup a cron task in EMR master.
- Use airflow to schedule job.


3. Scale up.


- The data was increased by 100x. Need use Kubernate to deploy the EMR cluster and data processing.
        
    
- The database needed to be accessed by 100+ people. Because the processed data has been stored in S3, Athena is a right tool for many users to query the data concurrently. The Athena is a serverless lamda applications, it is very economic.


![ ](lake_structure.png?raw=true "Title")
