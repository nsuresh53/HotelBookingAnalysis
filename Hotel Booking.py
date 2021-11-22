# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This Notebook covers the step by step EDA of Hotel Bookings. The anlysis is divided into 4 sections
# MAGIC 1. Average Daily Rate
# MAGIC 2. % Stayed vs Canceled
# MAGIC 3. Average Length Of Stay
# MAGIC 4. Lead Time
# MAGIC 5. Reserved Room vs Assigned Room Type
# MAGIC 6. Repeated Customers

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/hotel_bookings-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd
def is_canceledstr(value):
  if value == '0':
    return 'No'
  else:
    return 'Yes'

udfis_canceledstr=udf(is_canceledstr,StringType())
df=df.withColumn("canceled",udfis_canceledstr("is_canceled"))
#df=df.drop('is_canceled')
display(df)

# COMMAND ----------

#The function creates a new column which says if the rooms assigned is the same as the reserved type.
def is_roomtypestr(value1,value2):
  if value1==value2:
    return 'Same'
  else:
    return 'NotSame'

udfis_roomtypestr=udf(is_roomtypestr,StringType())
df=df.withColumn("RoomType",udfis_roomtypestr("reserved_room_type","assigned_room_type"))
display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "hotel"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `hotel`

# COMMAND ----------

df.filter(df['hotel'].isNull()).count()

# COMMAND ----------

# DBTITLE 1,Number of Total Bookings
#Types of Hotels
display(df.select('hotel').groupby('hotel').count())

# COMMAND ----------

# MAGIC %md
# MAGIC **From the above graph it is obvious that City hotel is in more demand compared to Resort hotel.**

# COMMAND ----------

@pandas_udf('double')

def monthnumber(v:pd.Series) -> pd.Series:
  return datetime.strptime(v,"%B").month

df.withColumn('mnthnumber',monthnumber(df.arrival_date_month))

# COMMAND ----------

result_df=df.select("*").toPandas()
def monthnumber(v):
  return datetime.strptime(v,"%B").month

spark.udf.register("monthnumber",monthnumber,LongType())

result_df['month']=result_df['arrival_date_month'].apply(lambda v:monthnumber(v))

# COMMAND ----------

result_df['dateInt']=result_df['arrival_date_year'].astype(str) + result_df['month'].astype(str).str.zfill(2)+ result_df['arrival_date_day_of_month'].astype(str).str.zfill(2)
result_df['ArrivalDate'] = pd.to_datetime(result_df['dateInt'], format='%Y%m%d')


# COMMAND ----------

result_df=result_df.drop(columns=['month','dateInt'])
df=spark.createDataFrame(result_df)
#df=df.withColumn("TotalReserved",df.groupBy('Hotel').count('ArrivalDate'))

# COMMAND ----------

df.createOrReplaceTempView('hotel')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Reservations by Year
#Number of Arrival Entry gives us the booking information
display(df.groupby(['Hotel','arrival_date_year']).count().orderBy('arrival_date_year',ascending=True))

# COMMAND ----------

#Highest Occupied month
#When is ADR highest
#Number of people visiting from each country

# COMMAND ----------

# DBTITLE 1,Total reservations 
display(df.groupby(['Hotel','arrival_date_month']).count().dropna())

# COMMAND ----------

# MAGIC %md
# MAGIC It is clearly visible that the percentage of booking is higher during the summer months from May - August.

# COMMAND ----------

# DBTITLE 1,Average Daily Rate by Month
# MAGIC %sql
# MAGIC 
# MAGIC select avg(adr) ADR,hotel,arrival_date_month from hotel group by hotel,arrival_date_month

# COMMAND ----------

# DBTITLE 1,Average Daily Rate (ADR) by Market Segment for Stayed Guests
display(df.filter(df['canceled']=='No').groupby(['hotel','market_segment']).agg({'adr':'avg'}))

# COMMAND ----------

# DBTITLE 1,Average Daily Rate (ADR) by Customer Type for Stayed Guests
display(df.filter(df['canceled']=='No').groupby(['hotel','customer_type']).agg({'adr':'avg'}))

# COMMAND ----------

# DBTITLE 1,ADR by Region for Stayed Guest
display(df.filter(df['canceled']=='No').groupby(['hotel','country']).agg({'adr':'mean','ArrivalDate':'count'}))

# COMMAND ----------

# MAGIC %md 
# MAGIC **More number of bookings are from Portugal.** 

# COMMAND ----------

# DBTITLE 1,Stayed vs Cancelled
# MAGIC %sql
# MAGIC select hotel,canceled,count(ArrivalDate) from hotel group by hotel,canceled

# COMMAND ----------

#Percentage of Cancellation between Resort and City
#Market Segment
#Out of the booking how many cancelled and how manys stayed
#Country
#Out of the booking how many cancelled and how many stayed


# COMMAND ----------

# DBTITLE 1,% Of Cancellation 
# MAGIC %sql
# MAGIC Select h.Hotel,h.canceled,count(h.ArrivalDate)/a.Total as Percentage from hotel h join (select Hotel,count(ArrivalDate) as Total from hotel group by Hotel) a on h.Hotel=a.Hotel group by h.Hotel,h.canceled,a.Total

# COMMAND ----------

# MAGIC %md
# MAGIC **% of Cancellation is high in City Hotels**

# COMMAND ----------

# DBTITLE 1,% of Stayed vs Canceled by Market Segments
# MAGIC %sql
# MAGIC select Hotel,market_segment,canceled,count(ArrivalDate) from hotel where group by Hotel,canceled,market_segment

# COMMAND ----------

# DBTITLE 1,% of Stayed vs Canceled by Market Segments
# MAGIC %sql
# MAGIC select Hotel,customer_type,canceled,count(ArrivalDate) Count from hotel where group by Hotel,canceled,customer_type

# COMMAND ----------

# DBTITLE 1,% of Stayed vs Canceled by country
# MAGIC %sql
# MAGIC Select h.Hotel,h.canceled,h.country,round((count(h.ArrivalDate)/a.Total)*100,2) as Percentage from hotel h join (select Hotel,count(ArrivalDate) as Total from hotel group by Hotel) a on h.Hotel=a.Hotel where country != 'NULL' group by h.Hotel,h.canceled,h.country,a.Total

# COMMAND ----------

# DBTITLE 1,Number of People stayed in the hotel by age group
#Find the number of adults, childer and babies stayed in resort and city
df.filter(df['canceled']=='No').groupBy(['hotel']).agg(sum('adults').alias('Adult'),sum('children').alias('Children'),sum('babies').alias('Babies')).display()

# COMMAND ----------

#Percent of people with more family members that cancelled.
#Did people with babies and children cancel more often
#Create a table with percentage cancellation statistics,by market segment,country,hotel. And connect this to powerbi for a visulaization.
#Average length of stay for people with families and without families.
#Lead time where the chances of cancellation is high.
#What's the lead time of people with families
#Average weekend stay and average weeknight stay
#May be create two tables for each hotel type and create two pages of dashboard.


# COMMAND ----------

# DBTITLE 1,Number of people stayed by family size
# MAGIC %sql
# MAGIC 
# MAGIC select hotel,count(ArrivalDate),adults,children,babies from hotel where canceled = 'No' group by adults,children,babies,hotel

# COMMAND ----------

# DBTITLE 1,Hotel guest's without Adults
# MAGIC %sql
# MAGIC -- from the above analysis there are few entries where the occupants were only children.Lets check the segments of those and see if it is a valid entry
# MAGIC select hotel,count(ArrivalDate),adults,children,babies,market_segment from hotel where canceled = 'No' and adults=0 group by adults,children,babies,market_segment,hotel

# COMMAND ----------

# MAGIC %md 
# MAGIC **The above booking had no adults accompanied during their stay, which is not allowed by law. Hence I think these are non valid entries and should be removed from analysis.**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select h.Hotel,h.canceled,h.adults,h.children,h.babies,round(((count(h.ArrivalDate)/a.Total)*100),2) as Percentage from hotel h join (select Hotel,count(ArrivalDate) as Total from hotel group by Hotel) a on h.Hotel=a.Hotel where country != 'NULL' group by h.Hotel,h.canceled,a.Total,h.adults,h.children,h.babies

# COMMAND ----------

# MAGIC %md
# MAGIC The percentage of cancellation is very less for families. More cancellations are done by booking with 2 adults.

# COMMAND ----------

# DBTITLE 1,Average length of time for people with family
display(df.filter(((df.stays_in_weekend_nights>0) | (df.stays_in_week_nights>0)) & (df.canceled =='No') ).groupBy(['hotel','adults','children','babies']).agg((ceil(avg('stays_in_weekend_nights'))).alias('Weekend'),(ceil(avg('stays_in_week_nights'))).alias('Weekday')))

# COMMAND ----------

# DBTITLE 1,Avg Lead Time
display(df.groupBy(['hotel','canceled']).agg(avg('lead_time').alias('AvgLeadTime')))

# COMMAND ----------

# DBTITLE 1,Reserved vs Assigned Room Type
# MAGIC %sql
# MAGIC select count(RoomType),hotel,RoomType from hotel where canceled = 'No' group by RoomType,hotel

# COMMAND ----------

# MAGIC %md
# MAGIC **In Resort Hotel 25% of the booking were not assigned the correct room type which were reserved at the time of booking.**

# COMMAND ----------

# DBTITLE 1,Agents with Highest number of Bookings
df.filter(df['canceled']=='No').groupby(['Agent']).agg(count('ArrivalDate').alias('bookings')).display()

# COMMAND ----------

# DBTITLE 1,Repeated Guests?
df.filter(df['is_repeated_guest']>0).groupby(['hotel','country']).agg(sum('is_repeated_guest').alias('RepeatedGuest')).display()

# COMMAND ----------

hoteldf=df.filter(df['adults']>0)

# COMMAND ----------

hoteldf = hoteldf.withColumn("adr", hoteldf["adr"].cast(IntegerType()))
hoteldf = hoteldf.withColumn("ArrivalDate", hoteldf["ArrivalDate"].cast(DateType()))
permanent_table_name = "hoteldata_csv"
#hoteldf.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------


