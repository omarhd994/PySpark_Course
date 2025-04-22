dbutils.fs.ls('/FileStore/tables/drivers.json/')

df_json = spark.read.format('json').option('inferSchema',True)\
    .option('header', True)\
    .option('multiLine', False)\
    .load('/FileStore/tables/drivers.json/')

df_json.show()

dbutils.fs.ls('/FileStore/tables/BigMart_Sales.csv')

df = spark.read.format('csv').option('interschema', True). option('header', True). load('/FileStore/tables/BigMart_Sales.csv')

df.display()

df.printSchema()

my_ddl_schema = '''     
                Item_Identifier STRING,
                Item_Weight DOUBLE, 
                Item_Fat_Content STRING, 
                Item_Visibility DOUBLE, 
                Item_Type STRING, 
                Item_MRP STRING, 
                Outlet_Identifier STRING, 
                Outlet_Establishment_Year INT, 
                Outlet_Size STRING, 
                Outlet_Location_Type STRING, 
                Outlet_Type STRING, 
                Item_Outlet_Sales DOUBLE
                '''

df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

df.display()

df.printSchema()

df.display()

df_sel = df.select('Item_Identifier','Item_weight','Item_Fat_Content').display()

df.display()

from pyspark.sql.functions import col

df.select(col('Item_Identifier').alias('Identifiant')).display()

df.filter(col('Item_Fat_Content') == 'Regular').display()

df.filter((col('Item_Type') == 'Dairy') & (col('Item_Weight') < 8)).display()

df.filter((col('Outlet_size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

df.withColumnRenamed('Item_Weight','Item_Wt').display()

df.withColumn('multiplied', col('Item_Weight') * col('Item_MRP')).display()

from pyspark.sql.functions import regexp_replace

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Regular', 'Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat', 'LF')).display()

df.printSchema()

from pyspark.sql.types import StructType, StructField, StringType


df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).printSchema()

df.printSchema()

from pyspark.sql.functions import desc, asc

df.sort(asc('Item_Weight'), desc('Item_Visibility')).display()

df.limit(5).display()
