# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)



```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(spark.version)
```

The output was 3.5.5.





## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB


```python
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df = df.repartition(4)
df.write.parquet('yellow/2024/10')
```

The parquet files were 22.4 mb each.


## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

```python
from pyspark.sql.functions import col, to_timestamp

# Convert the 'lpep_pickup_datetime' to timestam if it is not already
df = df.withColumn("lpep_pickup_datetime", to_timestamp("tpep_picku_datetime", "yyyy-MM-dd HH:mm:ss"))


oct_15_trips = df.filter(col("lpep_pickup_datetime").between("2024-10-15 00:00:00", "2024-10-15 23:59:59"))

oct_15_count = oct_15_trips.count()

print(f"Number of taxi trips on the 15th of October : {oct_15_count}")

```

Number of taxi trips on the 15th of October: 125,567


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

```python
df.registerTempTable('trips_data')
spark.sql("""
select MAX(timestampdiff(HOUR, tpep_pickup_datetime, tpep_dropoff_datetime)) from trips_data
""").show()
```

The output was 162.


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Spark UI is available at localhost:4040.

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay


```python
zone = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

from pyspark.sql.functions import col
zone = zone.withColumn("LocationID", col("LocationID").cast("int"))

zone.registerTempTable('zones')

spark.sql("""
select t2.Zone, count(*) as num_trips from trips_data t1 inner join zones t2 on t1.PULocationID = t2.LocationID
group by t2.Zone
order by num_trips
LIMIT 1
""").show(truncate=False)
```

The output shows "Governor's Island/Ellis Island/Liberty Island".



## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website