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

jdbc_url="jdbc:mysql://database-1.cxiq7pdjnjhk.us-east-1.rds.amazonaws.com:3306/crime_database"

connection_properties = {
    "user": "admin",
    "password": "wYtCqWxYt3OVMkWafMmk",
}

outcomes_df = spark.read.jdbc(url=jdbc_url, table="Crime_Outcomes", properties=connection_properties)
outcomes_df1 = spark.read.csv("s3://raw-proj-files-19-23/outcomes/")
outcomes = outcomes_df.union(outcomes_df1)
outcomes.repartition(1).write.parquet("s3://grp6-datalakexxx/outcomes")

street_df = spark.read.jdbc(url=jdbc_url, table="Street_Crimes", properties=connection_properties)
street_df1 = spark.read.csv("s3://raw-proj-files-19-23/street/")
street = street_df.union(street_df1)
street.repartition(1).write.parquet("s3://grp6-datalakexxx/street")


stop_n_search_df = spark.read.jdbc(url=jdbc_url, table="Stop_And_Search", properties=connection_properties)
stop_n_search_df1 = spark.read.csv("s3://raw-proj-files-19-23/stop_and_search/")
stop_n_search = stop_n_search_df.union(stop_n_search_df1)
stop_n_search.repartition(1).write.parquet("s3://grp6-datalakexxx/stop_n_search")


job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()