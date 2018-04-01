

```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row

employeeSchema = StructType([
    StructField("name", StringType(), True), 
    StructField("job", StringType(), True),
    StructField("department", StringType(), True),
    StructField("annual_salary", StringType(), True),
    StructField("estimated_salary", StringType(), True)
])

sqlContext = SQLContext(sc)
df = sqlContext.read.format("csv").schema(employeeSchema).option("header","true").option("delimiter", ",").load("/home/krish/Learning/pySpark/Employees.csv")
df.show()
```

    +--------------------+--------------------+----------------+-------------+----------------+
    |                name|                 job|      department|annual_salary|estimated_salary|
    +--------------------+--------------------+----------------+-------------+----------------+
    |     AARON,  ELVIA J|    WATER RATE TAKER|     WATER MGMNT|    $81000.00|       $73862.00|
    |   AARON,  JEFFERY M|      POLICE OFFICER|          POLICE|    $74628.00|       $74628.00|
    | AARON,  KIMBERLEI R|CHIEF CONTRACT EX...| FLEET MANAGEMNT|    $77280.00|       $70174.00|
    | ABAD JR,  VICENTE M|   CIVIL ENGINEER IV|     WATER MGMNT|    $96276.00|       $96276.00|
    |ABBATACOLA,  ROBE...| ELECTRICAL MECHANIC|     WATER MGMNT|    $84032.00|       $76627.00|
    |    ABBATE,  TERRY M|      POLICE OFFICER|          POLICE|    $79926.00|       $79926.00|
    |ABBATEMARCO,  JAM...|         FIREFIGHTER|            FIRE|    $77238.00|       $77238.00|
    |    ABBOTT,  BETTY L|  FOSTER GRANDPARENT|FAMILY & SUPPORT|     $2756.00|        $2756.00|
    |   ABBOTT,  LYNISE M|           CLERK III|FAMILY & SUPPORT|    $38568.00|       $38568.00|
    |      ABBOTT,  SAM J| ELECTRICAL MECHANIC|      TRANSPORTN|    $84032.00|       $76627.00|
    |ABDELHADI,  ABDAL...|      POLICE OFFICER|          POLICE|    $71040.00|       $71040.00|
    | ABDELLATIF,  AREF R|FIREFIGHTER (PER ...|            FIRE|    $86922.00|       $86922.00|
    |  ABDELMAJEID,  AZIZ|      POLICE OFFICER|          POLICE|    $67704.00|       $67704.00|
    | ABDOLLAHZADEH,  ALI|           PARAMEDIC|            FIRE|    $64374.00|       $64374.00|
    |ABDUL-KARIM,  MUH...|ENGINEERING TECHN...|     WATER MGMNT|    $81000.00|       $81000.00|
    |    ABDULLAH,  KEVIN|         FIREFIGHTER|            FIRE|    $83148.00|       $83148.00|
    |ABDULLAH,  LAKENYA N|      CROSSING GUARD|          POLICE|    $15319.00|       $15319.00|
    | ABDULLAH,  MOULAY R|      POLICE OFFICER|          POLICE|    $82878.00|       $82878.00|
    | ABDULLAH,  RASHAD J|ELECTRICAL MECHAN...| FLEET MANAGEMNT|    $84032.00|       $76627.00|
    |   ABEJERO,  JASON V|      POLICE OFFICER|          POLICE|    $77238.00|       $77238.00|
    +--------------------+--------------------+----------------+-------------+----------------+
    only showing top 20 rows
    



```python
df.printSchema()
```

    root
     |-- name: string (nullable = true)
     |-- job: string (nullable = true)
     |-- department: string (nullable = true)
     |-- annual_salary: string (nullable = true)
     |-- estimated_salary: string (nullable = true)
    



```python
sqlDF = df.createOrReplaceTempView("emp")
sqlDF = spark.sql("""
                  select name 
                          ,job 
                          ,department 
                          ,float(substr(annual_salary,2)) annual_salary 
                          ,float(substr(estimated_salary,2)) estimated_salary 
                  from emp 
                  """)
sqlDF = sqlDF.createOrReplaceTempView("emp")
sqlDF = spark.sql("""
                  select department, sum(annual_salary) total_salary from emp group by department
                  """)
sqlDF.show()
sqlDF.printSchema()
```

    +-----------------+------------+
    |       department|total_salary|
    +-----------------+------------+
    |  HUMAN RELATIONS|   2485344.0|
    | BUSINESS AFFAIRS| 1.3272819E7|
    |          REVENUE| 2.5017405E7|
    | CULTURAL AFFAIRS|   5253000.0|
    |     CITY COUNCIL| 2.1975599E7|
    |LICENSE APPL COMM|     63276.0|
    |BOARD OF ELECTION|   6503736.0|
    |    BUDGET & MGMT|   4443428.0|
    |             IPRA|   6755328.0|
    | GENERAL SERVICES| 3.4528777E7|
    |       COMPLIANCE|   1793292.0|
    |      WATER MGMNT|1.45017269E8|
    |        BUILDINGS| 2.5194636E7|
    |      ENVIRONMENT|   4899336.0|
    |  BOARD OF ETHICS|    562800.0|
    |    STREETS & SAN|1.47859259E8|
    |         AVIATION| 8.7098768E7|
    |           POLICE|1.07557067E9|
    | FAMILY & SUPPORT| 3.7239766E7|
    |    ANIMAL CONTRL|   3811291.0|
    +-----------------+------------+
    only showing top 20 rows
    
    root
     |-- department: string (nullable = true)
     |-- total_salary: double (nullable = true)
    



```python
# Dropping and 
spark.catalog.dropTempView("emp1")

# list alll the temp tables
spark.catalog.listTables()
```




    [Table(name='emp', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]




```python
sqlDF.write.format("csv").mode("overwrite").save("employee_new.csv") 
sqlDF.write.format("parquet").mode("overwrite").save("employee_new.parquet") 
```


```python
newdf = sqlContext.read.format("parquet").option("header","true").option("delimiter", ",").load("employee_new.parquet")
newdf.show()
newdf.printSchema()
```

    +--------------------+------------+
    |          department|total_salary|
    +--------------------+------------+
    |          TRANSPORTN| 9.5238846E7|
    |COMMUNITY DEVELOP...| 1.8676702E7|
    |   BOARD OF ELECTION|   6503736.0|
    |   LICENSE APPL COMM|     63276.0|
    |    BUSINESS AFFAIRS| 1.3272819E7|
    |    FAMILY & SUPPORT| 3.7239766E7|
    |    GENERAL SERVICES| 3.4528777E7|
    |    CULTURAL AFFAIRS|   5253000.0|
    |        ADMIN HEARNG|   2695164.0|
    |           TREASURER|   1850395.0|
    |     BOARD OF ETHICS|    562800.0|
    |     FLEET MANAGEMNT| 4.7155125E7|
    |     HUMAN RESOURCES|   4867798.0|
    |     HUMAN RELATIONS|   2485344.0|
    |      MAYOR'S OFFICE|   6196699.0|
    |      PUBLIC LIBRARY|  5.405776E7|
    |       BUDGET & MGMT|   4443428.0|
    |       STREETS & SAN|1.47859259E8|
    |       INSPECTOR GEN|   4189926.0|
    |       ANIMAL CONTRL|   3811291.0|
    +--------------------+------------+
    only showing top 20 rows
    
    root
     |-- department: string (nullable = true)
     |-- total_salary: double (nullable = true)
    

