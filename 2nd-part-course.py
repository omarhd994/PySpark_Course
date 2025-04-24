#!/usr/bin/env python
# coding: utf-8

# In[ ]:


dbutils.fs.ls('/FileStore/tables/BigMart_Sales.csv')


# In[ ]:


df = spark.read.format('csv').option('interschema', True). option('header', True). load('/FileStore/tables/BigMart_Sales.csv')


# In[ ]:


df.display()


# ## Review of last lesson: LIMIT

# In[ ]:


df.limit(17).display()


# ## Drop_Duplicates

# In[ ]:


df.dropDuplicates().display()


# # Scenario - 2

# In[ ]:


df.dropDuplicates(subset=['Item_Type']).display()


# DISTINCT : Does same thing that dropDuplicates() but only for all df

# In[ ]:


df.distinct().display()


# # UNION and UNION BY NAME

# Preparing df

# In[ ]:


data1 = [('1', 'omar'),
         ('2', 'philippe')]
schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1, schema1)

data2 = [('1', 'hugues'),
         ('2', 'maria')]
schema2 = 'id STRING, name STRING'

df2 = spark.createDataFrame(data2, schema2)


# In[ ]:


df1.display()


# In[ ]:


df2.display()


# # Union

# In[ ]:


df1.union(df2).display()


# In[ ]:


df2.union(df1).display()


# Messed up data eg

# In[ ]:


data3 = [('Cedric','3'),
         ('Laurent', '4')]
schema3 = 'name STRING, id STRING'

df3 = spark.createDataFrame(data3, schema3)


# In[ ]:


df1.union(df3).display()


# # Union By Name

# In[ ]:


df1.unionByName(df3).display()


# # Sting f(x)

# INITCAP

# In[ ]:


from pyspark.sql.functions import initcap, upper, lower


# In[ ]:


df.select(initcap('Item_Type').alias('InitCap_Item_Type')).display()


# UPPER

# In[ ]:


df.select(upper('Item_Type').alias('Upper_Item_Type')).display()


# LOWER

# In[ ]:


df.select(lower('Item_Type').alias('Lower_Item_Type')).display()


# # CURRENT_DATE, DATE_ADD, DATE_SUB

# Current_Date

# In[ ]:


from pyspark.sql.functions import current_date, date_add, date_sub


# In[ ]:


df.withColumn('current_date', current_date()).display()


# Date_Add

# In[ ]:


df.withColumn('week_after', date_add('current_date', 7)).display()


# Date_Sub

# In[ ]:


df.withColumn('week_before', date_sub('current_date', 7)).display()


# 
