

```python
#Creates a DataFrame from an RDD of tuple/list, list or pandas.DataFrame.
#https://spark.apache.org/docs/1.6.2/api/python/pyspark.sql.html

from pyspark.sql.types import *

schemaPerson = StructType([
    StructField("id",IntegerType(), True), 
    StructField("name",StringType(), True), 
    StructField("cityId",IntegerType(), True)
])

schemaCity = StructType([
    StructField("cityId",IntegerType(), True), 
    StructField("name",StringType(), True)
])

```


```python
# create family dataframe
listPerson = [(0, "Agata", 0), (1, "Iweta", 0), (2, "Patryk", 2),(3, "Maksym", 0),(4, "Maksym", 4)]
dfFamily = sqlContext.createDataFrame(listPerson,schemaPerson)
dfFamily.collect()
dfFamily.show()
```

    +---+------+------+
    | id|  name|cityId|
    +---+------+------+
    |  0| Agata|     0|
    |  1| Iweta|     0|
    |  2|Patryk|     2|
    |  3|Maksym|     0|
    |  4|Maksym|     4|
    +---+------+------+
    



```python
# create city dataframe
listCity = [(0, "Warsaw"),(1, "Washington"),(2, "Sopot")]
dfCity=sqlContext.createDataFrame(listCity,schemaCity)
dfCity.collect()
dfCity.show()
```

    +------+----------+
    |cityId|      name|
    +------+----------+
    |     0|    Warsaw|
    |     1|Washington|
    |     2|     Sopot|
    +------+----------+
    



```python
# convert the dataframe to table and perform join operation
sqlDfFamily = dfFamily.createOrReplaceTempView("family")
sqlDfCity = dfCity.createOrReplaceTempView("city")
```


```python
#inner join
sqlQuery1 = spark.sql("""
                      select f.id, f.name, c.name from family f inner join city c on f.cityId = c.cityId
                      """)
sqlQuery1.show()
```

    +---+------+------+
    | id|  name|  name|
    +---+------+------+
    |  2|Patryk| Sopot|
    |  0| Agata|Warsaw|
    |  1| Iweta|Warsaw|
    |  3|Maksym|Warsaw|
    +---+------+------+
    



```python
#left join
sqlQuery1 = spark.sql("""
                      select f.id, f.name, c.name from family f left join city c on f.cityId = c.cityId
                      """)
sqlQuery1.show()
```

    +---+------+------+
    | id|  name|  name|
    +---+------+------+
    |  4|Maksym|  null|
    |  2|Patryk| Sopot|
    |  0| Agata|Warsaw|
    |  1| Iweta|Warsaw|
    |  3|Maksym|Warsaw|
    +---+------+------+
    



```python
#right join
sqlQuery1 = spark.sql("""
                      select f.id, f.name, c.name from family f right join city c on f.cityId = c.cityId
                      """)
sqlQuery1.show()
```

    +----+------+----------+
    |  id|  name|      name|
    +----+------+----------+
    |null|  null|Washington|
    |   2|Patryk|     Sopot|
    |   0| Agata|    Warsaw|
    |   1| Iweta|    Warsaw|
    |   3|Maksym|    Warsaw|
    +----+------+----------+
    



```python
#full outer join
sqlQuery1 = spark.sql("""
                      select f.id, f.name, c.name from family f full outer join city c on f.cityId = c.cityId
                      """)
sqlQuery1.show()
```

    +----+------+----------+
    |  id|  name|      name|
    +----+------+----------+
    |null|  null|Washington|
    |   4|Maksym|      null|
    |   2|Patryk|     Sopot|
    |   0| Agata|    Warsaw|
    |   1| Iweta|    Warsaw|
    |   3|Maksym|    Warsaw|
    +----+------+----------+
    



```python
#not exists
sqlQuery1 = spark.sql("""
                      select * from family f where not exists (select 1 from city c where f.cityId = c.cityId)
                      """)
sqlQuery1.show()
```

    +---+------+------+
    | id|  name|cityId|
    +---+------+------+
    |  4|Maksym|     4|
    +---+------+------+
    

