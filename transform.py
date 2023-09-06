import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, count, split, coalesce, max
from pyspark.sql.functions import col, max, row_number,lit,months_between,StringType,udf
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

 
#path

df_street = spark.read.parquet("s3://grp6-datalakexxx/street/",header=True,inferSchema=True)


#Dropping or filling null values

df_street=df_street.drop('Context','LSOA code','LSOA name')

df_with_null=df_street.filter(col('Crime ID').isNull())
df_not_null=df_street.filter(col('Crime ID').isNotNull())

df_not_null=df_not_null.dropDuplicates(['Crime ID'])
df_with_null=df_with_null.na.fill('Not Defined',['Crime ID'])

df_street=df_with_null.union(df_not_null)

df_street=df_street.na.fill('Status update unavailable',['Last outcome category'])

df_street=df_street.na.drop(subset=['Longitude','Latitude'])

#fetching only location of police force --------------------------

police_forces = {
    "Metropolitan Police Service": "London",
    "Wiltshire Police": "Wiltshire",
    "West Yorkshire Police": "West Yorkshire",
    "Leicestershire Police": "Leicestershire",
    "Humberside Police": "Humberside",
    "Suffolk Constabulary": "Suffolk",
    "West Midlands Police": "West Midlands",
    "Norfolk Constabulary": "Norfolk",
    "Lancashire Constabulary": "Lancashire",
    "Cheshire Constabulary": "Cheshire",
    "Bedfordshire Police": "Bedfordshire",
    "Hertfordshire Constabulary": "Hertfordshire",
    "Essex Police": "Essex",
    "North Wales Police": "North Wales",
    "Derbyshire Constabulary": "Derbyshire",
    "Durham Constabulary": "Durham",
    "Avon and Somerset Constabulary": "Avon and Somerset",
    "Northamptonshire Police": "Northamptonshire",
    "Surrey Police": "Surrey",
    "Sussex Police": "Sussex",
    "Merseyside Police": "Merseyside",
    "Cleveland Police": "Cleveland",
    "Dyfed-Powys Police": "Dyfed-Powys",
    "Hampshire Constabulary": "Hampshire",
    "South Yorkshire Police": "South Yorkshire",
    "Thames Valley Police": "Thames Valley",
    "Dorset Police": "Dorset",
    "North Yorkshire Police": "North Yorkshire",
    "Cumbria Constabulary": "Cumbria",
    "Greater Manchester Police": "Greater Manchester",
    "West Mercia Police": "West Mercia",
    "South Wales Police": "South Wales",
    "City of London Police": "London",
    "Devon & Cornwall Police": "Devon & Cornwall",
    "Kent Police": "Kent",
    "Cambridgeshire Constabulary": "Cambridgeshire",
    "Nottinghamshire Police": "Nottinghamshire",
    "Northumbria Police": "Northumbria",
    "Gwent Police": "Gwent",
    "Staffordshire Police": "Staffordshire",
    "Lincolnshire Police": "Lincolnshire",
    "Gloucestershire Constabulary": "Gloucestershire",
    "Police Service of Northern Ireland": "Northern Ireland",
    "Warwickshire Police": "Warwickshire",
}

def location(val):
	return police_forces.get(val,'Other')

get_location = udf(location, StringType())

df_street = df_street.withColumn("Police Force location", get_location(col("Falls within")))

#Dividing police forces as per countries in UK (England, Scotland,wales, Northern Ireland)----------------------------

conditions = [
(col("falls within").isin('Metropolitan Police Service',
'West Yorkshire Police','West Midlands Police','Greater Manchester Police','Kent Police','Lancashire Constabulary',
'Thames Valley Police','Essex Police','Hampshire Constabulary','South Yorkshire Police','Merseyside Police',
'Avon and Somerset Constabulary','Sussex Police','Nottinghamshire Police','Devon & Cornwall Police',
'West Mercia Police','Staffordshire Police','Derbyshire Constabulary','Hertfordshire Constabulary',
'Humberside Police','Cheshire Constabulary','Leicestershire Police','Cleveland Police','Surrey Police',
'Cambridgeshire Constabulary','Northamptonshire Police','Durham Constabulary','Norfolk Constabulary',
'Dorset Police','North Yorkshire Police','Lincolnshire Police','Bedfordshire Police','Suffolk Constabulary',
'Gloucestershire Constabulary','Wiltshire Police','Warwickshire Police','Cumbria Constabulary',
'City of London Police'), 'England'),

    (col("falls within").isin('Northumbria Police'), 'Scotland'),

    (col("falls within").isin('Dyfed-Powys Police','Gwent Police','North Wales Police','South Wales Police'), 'wales'),

    (col("falls within").isin('Police Service of Northern Ireland'), 'Northern Ireland')
]

df_street = df_street.withColumn("Country", when(conditions[0][0], conditions[0][1]).when(conditions[1][0], conditions[1][1]).when(conditions[2][0], conditions[2][1]).when(conditions[3][0], conditions[3][1]).otherwise('Other'))

# Dividing crime type into 4 category to get better visualization ---------------------------------------------

df_street=df_street.withColumnRenamed("Crime type", "Sub Crime type")

conditions = [
     (col("Sub Crime type").isin('Violence and sexual offences','Robbery','Possession of weapons'
     ), 'Violent Crimes'),

     (col("Sub Crime type").isin('Burglary','Criminal damage and arson','Other theft','Vehicle crime','Bicycle theft','Theft from the person'
     ), 'Property Crimes'),

     (col("Sub Crime type").isin('Public order','Anti-social behaviour'

     ), 'Public Order Crimes'),

     (col("Sub Crime type").isin('Shoplifting','Other crime','Drugs'
     ), 'Miscellaneous Crimes')
 ]
 

df_street= df_street.withColumn("Crime type", when(conditions[0][0], conditions[0][1]).when(conditions[1][0], conditions[1][1]).when(conditions[2][0], conditions[2][1]).when(conditions[3][0], conditions[3][1]).otherwise('Other'))


#----------------------------------------------------------outcome------------------------------------------------------------------------------

df_outcome = spark.read.parquet("s3://grp6-datalakexxx/outcomes/",header=True,inferSchema=True)

#dropping columns which are already present in Street 
temp=['Reported by','Falls within','Longitude','Latitude','Location','LSOA code','LSOA name']

df_outcome=df_outcome.drop(*temp)

#To remove ambiguity
df_outcome=df_outcome.withColumnRenamed("Month", "Month of Outcome")

#we are removing duplicates from Crime ID and considering the latest dates we have

df_outcome = df_outcome.withColumn("Date", col("Month of Outcome").cast("timestamp"))
df_outcome=df_outcome.drop('Month of Outcome')

window_spec = Window().partitionBy("Crime ID").orderBy(col("Date").desc())

df_outcome = df_outcome.withColumn("row_num", row_number().over(window_spec))
df_outcome = df_outcome.filter(col("row_num") == 1)
df_outcome = df_outcome.drop("row_num")

#--------------------------------------------------------------JOIN---------------------------------------------------------------------------

joined_df=df_street.join(df_outcome,on="Crime ID",how="left")

#Now remaining outcome will be present in upcoming outcome files hence currently result are Pending.
joined_df=joined_df.na.fill('Result Pending',['Outcome type'])

# Filling null values in outcome because we will never get outcome for the cases where crime ID not available.
joined_df = joined_df.withColumn("Final_outcome",when(joined_df["Crime ID"] == 'Not Defined','Crime ID not available').otherwise(joined_df["Outcome type"]))

joined_df=joined_df.drop("Outcome type")

#Due to joining null values created in outcome date also so, filling "1960-01-01 00:00:00" to null.
joined_df_filled = joined_df.withColumn("Date", F.to_timestamp(
    when(col("Date").isNull(), lit("1960-01-01 00:00:00")).otherwise(col("Date")),
    format="yyyy-MM-dd HH:mm:ss"
))

#calculating difference between month of case & month of outcome
joined_df_filled = joined_df_filled.withColumn("months_between", months_between(col("Date"), col("Month")))

joined_df_filled.repartition(1).write.parquet('s3://grp6-datawarexxx/master')

#========================================================Stop and search====================================================

from pyspark.sql.functions import col, to_timestamp, date_format,concat,lit, to_timestamp, from_utc_timestamp,when

df =spark.read.parquet("s3://grp6-datalakexxx/stop_n_search",header=True,inferSchema=True)

df=df.drop(*['Part of a policing operation','Policing operation','Officer-defined ethnicity','Outcome linked to object of search','Removal of more than just outer clothing'])

df = df.fillna('Details not available', subset=['Outcome'])

df = df.fillna('Not mentioned', subset=['Object of search'])

df= df.fillna('Not mentioned', subset=['Legislation'])

df= df.fillna('Other ethnic group - Not stated', subset=['Self-defined ethnicity'])

df= df.fillna('0.0',subset=['Latitude'])

df= df.fillna('0.0',subset=['Longitude'])

df= df.fillna('Not mentioned', subset=['Age range'])

df= df.fillna('Other', subset=['Gender'])

df = df.withColumn("DateTime", from_utc_timestamp(to_timestamp("Date", "yyyy-MM-dd'T'HH:mm:ssXXX"), "UTC"))

df = df.drop('Date')

conditions = [
(col("Outcome").isin('Suspect arrested','Suspect summonsed to court','Offender given penalty notice','Offender cautioned','Suspected psychoactive substances seized - No further action'), 'Found guilty'),
(col("Outcome").isin('Nothing found - no further action','Details not available','Article found - Detailed outcome unavailable','Offender given drugs possession warning','Local resolution','Details not available'), 'Not Found guilty')
]

df= df.withColumn("Outcome type", when(conditions[0][0], conditions[0][1]).when(conditions[1][0], conditions[1][1]).otherwise('Other'))

df.repartition(1).write.parquet("s3://grp6-datawarexxx/stop")


job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()