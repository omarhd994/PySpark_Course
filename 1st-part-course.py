# List files in the given path to check the JSON file
dbutils.fs.ls('/FileStore/tables/drivers.json/')

# Read a JSON file with automatic schema inference
df_json = spark.read.format('json').option('inferSchema', True)\
    .option('header', True)\
    .option('multiLine', False)\
    .load('/FileStore/tables/drivers.json/')

# Display the content of the JSON DataFrame
df_json.show()

# List files in the path to check the CSV file
dbutils.fs.ls('/FileStore/tables/BigMart_Sales.csv')

# Read the CSV file with incorrect schema option (should be inferSchema not interschema)
df = spark.read.format('csv').option('interschema', True).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# Display the content of the DataFrame
df.display()

# Print the inferred schema of the DataFrame
df.printSchema()

# Define a DDL schema manually as a string
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

# Read the CSV file again using the manually defined DDL schema
df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')

# Display the DataFrame
df.display()

# Print the schema to confirm the DDL-based structure
df.printSchema()

# Display the full DataFrame again
df.display()

# Select specific columns and display (this assigns result to df_sel but it's already displayed)
df_sel = df.select('Item_Identifier', 'Item_weight', 'Item_Fat_Content').display()

# Display the entire DataFrame again
df.display()

# Import the col function for column references
from pyspark.sql.functions import col

# Rename column with alias while selecting
df.select(col('Item_Identifier').alias('Identifiant')).display()

# Filter rows where 'Item_Fat_Content' is 'Regular'
df.filter(col('Item_Fat_Content') == 'Regular').display()

# Filter rows where type is 'Dairy' and weight is less than 8
df.filter((col('Item_Type') == 'Dairy') & (col('Item_Weight') < 8)).display()

# Filter rows with null Outlet_Size and where location is Tier 1 or Tier 2
df.filter((col('Outlet_size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# Rename column permanently with withColumnRenamed
df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# Create a new column from the multiplication of two existing columns
df.withColumn('multiplied', col('Item_Weight') * col('Item_MRP')).display()

# Import regexp_replace for string replacement using regular expressions
from pyspark.sql.functions import regexp_replace

# Replace values in 'Item_Fat_Content' using chained regexp_replace
df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'Reg'))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF')).display()

# Print the current schema
df.printSchema()

# Import StringType to cast columns
from pyspark.sql.types import StructType, StructField, StringType

# Cast 'Item_Weight' column to string and print the schema of the result
df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).printSchema()

# Print original schema again
df.printSchema()

# Import asc and desc for sorting
from pyspark.sql.functions import desc, asc

# Sort by Item_Weight ascending and Item_Visibility descending
df.sort(asc('Item_Weight'), desc('Item_Visibility')).display()

# Limit the result to 5 rows
df.limit(5).display()
