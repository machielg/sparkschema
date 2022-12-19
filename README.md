# Spark Schema

Analyse a DataFrame to determine its schema using PySpark. Mainly geared towards CSV files, since spark doesn't do a great job at schema inference and doesn't support Decimal types. 

### Usage
```
from sparkschema import SparkSchema

# read the file as-is
spark = SparkSession.getActiveSession()
df = spark.read.csv('myfile.csv', header=True, inferSchema=False, sep='|')
df.printSchema() # all strings!

# read it again but with its detected schema
schema = SparkSchema().get_schema(df)
df = spark.read.csv('myfile.csv', header=True, inferSchema=False, sep='|', schema=schema)

df.printSchema() # look decimals and everything!
```

