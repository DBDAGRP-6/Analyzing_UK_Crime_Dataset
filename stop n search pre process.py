import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session



from pyspark.sql.functions import col, to_timestamp, date_format,concat,lit, to_timestamp, from_utc_timestamp,when

df =spark.read.parquet("s3://grp6-datalake/stop_n_search/",header=True,inferSchema=True)

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

df.repartition(1).write.parquet("s3://grp6-dataware/stop")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()