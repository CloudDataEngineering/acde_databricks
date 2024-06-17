# Databricks notebook source
dbutils.fs.ls('dbfs:/FileStore/files/babynames')

# COMMAND ----------

# Read the CSV file into a DataFrame and specify that the first row is the header
baby_names = spark.read.format('csv').option('inferSchema', True).option('header', True).option('sep',',').load('dbfs:/FileStore/files/babynames/baby_names.csv')
display(baby_names)

# COMMAND ----------

baby_names = baby_names.withColumnRenamed('First Name','Name')
display(baby_names)

# COMMAND ----------

baby_names.write.format('delta').saveAsTable('baby_names')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from baby_names limit 5;

# COMMAND ----------

totla_count_of_baby_names = baby_names.count()
total_count = f'Total Count of Baby Names Records = {totla_count_of_baby_names}'
print(total_count)
# display(total_count)

# COMMAND ----------

baby_names_1960 = baby_names.filter(baby_names.Year == '1960')
display(baby_names_1960.count())

# COMMAND ----------

baby_names_1960.write.format('delta').saveAsTable('baby_names_1960')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from baby_names_1960;
# MAGIC -- select * from baby_names_1960 limit 5;
# MAGIC -- select * from baby_names_1960 where sex = 'M';
# MAGIC -- select count(*) from baby_names_1960 where sex = 'F';

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customer(
# MAGIC   custid int,
# MAGIC   custname varchar(20)
# MAGIC ) using delta;
# MAGIC
# MAGIC create table orders(
# MAGIC   orderid int,
# MAGIC   custid int,
# MAGIC   order_date date,
# MAGIC   order_amount int 
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customer values (1, 'Vinay');
# MAGIC insert into customer values (2, 'Alice');
# MAGIC insert into customer values (3, 'Roopa');
# MAGIC insert into customer values (4, 'Honey');
# MAGIC insert into customer values (5, 'MMM');
# MAGIC
# MAGIC insert into orders values 
# MAGIC (101, 1, '2024-05-01', 50),
# MAGIC (102, 2, '2024-05-02', 75),
# MAGIC (103, 1, '2024-05-03', 100),
# MAGIC (104, 3, '2024-05-04', 120),
# MAGIC (105, 4, '2024-05-05', 80),
# MAGIC (106, 5, '2024-05-06', 90),
# MAGIC (107, 1, '2024-05-07', 110),
# MAGIC (108, 2, '2024-05-08', 95),
# MAGIC (109, 3, '2024-05-09', 130),
# MAGIC (110, 4, '2024-05-10', 85);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer order by custid asc;
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from customer c, orders o
# MAGIC -- where c.custid = o.custid;
# MAGIC
# MAGIC -- select count(*) from
# MAGIC -- customer c
# MAGIC -- inner join
# MAGIC -- orders o on c.custid = o.custid;
# MAGIC
# MAGIC -- select * from 
# MAGIC -- customer c
# MAGIC -- inner join
# MAGIC -- orders o on c.custid = o.custid;
# MAGIC
# MAGIC -- with order_value as (select  c.custid,
# MAGIC -- c.custname,
# MAGIC -- avg(o.order_amount) avarage_order_amount,
# MAGIC -- sum(o.order_amount) total_order_amount,
# MAGIC -- dense_rank() over(order by sum(o.order_amount) desc) as order_rank
# MAGIC --  from 
# MAGIC -- customer c
# MAGIC -- inner join 
# MAGIC -- orders o on c.custid = o.custid
# MAGIC -- group by c.custid, c.custname)
# MAGIC -- select * from order_value;
# MAGIC
# MAGIC with order_value as(
# MAGIC   select 
# MAGIC   c.custid,c.custname,
# MAGIC   avg(o.order_amount) as average_order_amount,
# MAGIC   sum(o.order_amount) as total_order_amount
# MAGIC   from
# MAGIC   customer c
# MAGIC   inner join
# MAGIC   orders o on c.custid = o.custid
# MAGIC   group by c.custid, c.custname
# MAGIC )
# MAGIC select *,
# MAGIC rank() over(order by average_order_amount desc) as order_rank
# MAGIC  from order_value;

# COMMAND ----------

df_customer = sql('select * from customer')
display(df_customer)
df_orders = sql('select * from orders')
display(df_orders)

# COMMAND ----------

from pyspark.sql.functions import avg, sum
from pyspark.sql import functions as F
from pyspark.sql.window import Window

order_value = df_customer.join(df_orders, df_customer['custid'] == df_orders['custid'], 'inner') \
                         .groupBy(df_customer['custid'], df_customer['custname']) \
                         .agg(avg('order_amount').alias('average_order_amount'), sum('order_amount').alias('total_order_amount'))

order_value_rank = order_value.withColumn('order_rank', F.rank().over(Window.orderBy(F.desc('average_order_amount'))))

display(order_value_rank)

# COMMAND ----------

from pyspark.sql.functions import avg, sum
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Create DataFrame with average and total order amounts per customer
order_value = df_customer.join(df_orders, df_customer['custid'] == df_orders['custid'], 'inner') \
                        .groupBy(df_customer['custid'], df_customer['custname']) \
                        .agg(avg('order_amount').alias('average_order_amount'), sum('order_amount').alias('total_ouder_amount'))
# display(order_value)

# # Calculate order rank using window function
windowSpec = Window.orderBy(F.desc("average_order_amount"))
order_value = order_value.withColumn("order_rank", F.rank().over(windowSpec))

# Show the result
# order_value.show()
display(order_value)

