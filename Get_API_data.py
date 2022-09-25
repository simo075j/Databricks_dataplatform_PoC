# Databricks notebook source
import requests
import pandas
import azure.storage.blob

# COMMAND ----------

secret_siteCode = dbutils.secrets.get(scope = "azurekeyvault", key = "siteCode")
secret_apiID = dbutils.secrets.get(scope = "azurekeyvault", key = "apiID")
secret_apiPassword = dbutils.secrets.get(scope = "azurekeyvault", key = "apiPassword")
secret_rawconnectionstring = dbutils.secrets.get(scope = "azurekeyvault", key = "rawzone-connectionstring")
secret_baseurl = dbutils.secrets.get(scope = "azurekeyvault", key = "API-baseurl") 

# COMMAND ----------

endpoints =["GetCustomersRaw","GetContactsRaw","GetTasksRaw","GetEmployeesRaw","GetAllocationsRaw","GetWorkUnitsRaw",
            "GetTimeOffRegistrationsRaw","GetProjectsRaw","GetInvoicesRaw","GetInvoiceLinesRaw","GetInvoiceLineDetailsRaw","GetMileageRaw","GetContractsRaw"]

# COMMAND ----------

# API call API and header setup
URL = f'{secret_baseurl}/{endpoints}'
DATA = {'key': 'value'}

# The actual call
r = requests.post(URL, data=DATA)

# COMMAND ----------

# class that takes object and saves it to blobstorage(kinda overkill to do this in a class)
container_n = "timelogapidata"

class pdtoblob():
    def __init__(self, element):
        self.element = element

    def writeToBlob(self):
        blob_block = ContainerClient.from_connection_string(
            conn_str= secret_rawconnectionstring,
            container_name= container_n
            )
        output = io.StringIO()
        partial =self.element
        output = partial.to_csv(encoding='utf-8')  
        name = f"AppleUnits\DailyAppUnitsData{formatted_now_clean}.csv"
        blob_block.upload_blob(name, output, overwrite=True, encoding='utf-8')

# initialization
pdtoblob(df).writeToBlob()


# COMMAND ----------

# design considerations:
# individual requests vs looped requests?lamda function maybe?
# write everything to the same container vs multiple containers?

