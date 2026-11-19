from azure.storage.blob import BlobClient
from datetime import datetime, timedelta
import pyspark.sql.functions as f
from azure.storage.blob import BlobServiceClient
from dateutil.relativedelta import relativedelta





# configuration: landing area of the Data Platform storage
storage_account_name = dbutils.secrets.get(company="azure-xxx", key="storage-account-name") 
storage_account_access_key = dbutils.secrets.get(company="azure-xxx", key="storage-account-access-key")
blob_container_name = "test-logs-sample"

# variables for automating the data reading from the storage acccount above
storage_account_path = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
spark.conf.set(storage_account_path,storage_account_access_key)
blob_connection = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_access_key};EndpointSuffix=core.windows.net"

blob_name = "active/m=10"
blob = BlobClient.from_connection_string(blob_connection, container_name=blob_container_name, blob_name=blob_name)

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)


# Path containing your 10 CSV files
input_path = f"wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net/active/m=10/*.csv"

# Read all files at once (assuming they share the same schema)
df = spark.read.option("header", "true").csv(input_path)

# Temporary directory where the single partition file will be written
temp_output_path = f"wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net/temp_output/"

# Coalesce to 1 partition and write to blob storage
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)


    
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




blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_access_key)

month_1= f'{(datetime.now() - relativedelta(months=1)).strftime("%m")}'
blob_file_name = f'acivity-{month_1}.csv'

df.to_csv(blob_file_name, sep=",", index=False)
data_container = f'test/active/m={month_1}'

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