from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import datetime
from datetime import datetime

infer_schema = StructType([
StructField("EventName", StringType(), True),\
more than 30 fields
......
])

from azure.storage.blob import BlobClient

# configuration: landing area of the Data Platform storage
storage_account_name = dbutils.secrets.get(company="azure-xxx", key="storage-account-name") 
storage_account_access_key = dbutils.secrets.get(company="azure-xxx", key="storage-account-access-key")


# variables for automating the data reading from the storage acccount above
storage_account_path = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
spark.conf.set(storage_account_path,storage_account_access_key)
blob_connection = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_access_key};EndpointSuffix=core.windows.net"

blob_name = "logs-active"
blob = BlobClient.from_connection_string(blob_connection, container_name=blob_container_name, blob_name=blob_name)


    
now = datetime.now() 
day = now.date().day-1
#print(day)
#You can use :02d with formatting to allocate two spaces for a digit, and fill it up with 0 if the space remains empty.

def construct_path(day):
    return f'{day:02d}'

construct_path (day)
 
month = now.date().month

def construct_path1(month):
    return f'{month:02d}'

#hours json files to one csv file
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)
df_jsona=(spark.read.format("json")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter","*.json")
    .option('append', "true")
    .schema(infer_schema)
    .load(f"abfss://{blob_container_name}@{storage_account_name}.dfs.core.windows.net/folders/
    y=2026/m={construct_path1(month)}/d={construct_path(day)}/")
    .cache())

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

for column in df_jsona.schema:
    if isinstance(column.dataType, T.TimestampType):
        df_jsona = df_jsona.withColumn(
            column.name,
            F.when(
                F.col(column.name).between(pd.Timestamp.min, pd.Timestamp.max),
                F.col(column.name),
            ),
        )
cols = ['col1','col2', 'col3', 'col4', 'COL5', 'col6', 'start',   'time']
df_jsona = df_jsona.select(*cols)
df_jsona.show()
pdf = df_jsona.toPandas()

import datetime
pdf['time_start'] = pd.to_datetime(pdf['start'], format='%Y-%m-%dT%H:%M:%S.%fZ')
pdf['time_end'] = pd.to_datetime(pdf['time'], format='%Y-%m-%dT%H:%M:%S.%fZ')

pdf['delta']=pdf['time_end']-pdf['time_start']
pdf['delta']=pdf['delta'].dt.total_seconds()

pdf.drop(['start', 'time'], axis=1, inplace=True)

date = str(int(datetime.now().strftime('%Y%m%d')) - 1)
blob_file_name = f'a-log_{date}.csv'

pdf.to_csv(blob_file_name, sep=",", index=False)
data_container = f'logs-active/active/m={month}'

# Instantiate a BlobServiceClient using a connection string
connection="DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={access_key};EndpointSuffix=core.windows.net".format(storage_account=storage_account_name, access_key=storage_account_access_key)
blob_service_client = BlobServiceClient.from_connection_string(connection)
# Instantiate a ContainerClient
container_client = blob_service_client.get_container_client(data_container)

# Read the locally saved CSV file
with open(blob_file_name, "rb") as f:
    blob_bytes = f.read()
client = BlobClient.from_connection_string(connection, container_name=data_container, blob_name=blob_file_name)
# save as blob
client.upload_blob(blob_bytes, overwrite=True)