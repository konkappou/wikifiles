import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from requests.structures import CaseInsensitiveDict

from dotenv import load_dotenv
import tempfile
#from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from atlassian import Confluence
import pandas as pd
import os
file_name = "wikidata"

def wikidata_export_sharepoint(df, is_export_to_sharepoint, spfilename = "wikidata_reporting_export"):
    # ---------- Sharepoint
    if is_export_to_sharepoint:
        filename_full = '~/European Securities and Markets Authority/CMSReportingDashboard - General/data/jira/' + spfilename + '.csv'
        df.to_csv(filename_full, index=False)
        print("\nExported to sharepoint online file: ")
        print(filename_full,"\n")

    # ---------- Storage account
    connect_str = os.environ["STORAGE_CONTAINER_STRING"]
    # container_name = 'containertimertrigjira2'
    # container_name = 'cmsreportingstorageprem-container'
    container_name = os.environ["STORAGE_CONTAINER_NAME"]
   
    local_file_name = spfilename + '.csv'

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    with tempfile.NamedTemporaryFile(delete=False) as temp:
        # with tempfile.NamedTemporaryFile() as temp:
        file_name_temp = temp.name + '.csv'
        df.to_csv(file_name_temp, index=False)
        with open(file_name_temp, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        temp.close()
    print('Uploaded successfully to Azure Blob Storage %s, file: %s' %(container_name, local_file_name))
    return df



def save_wiki_data_csv(df):   
    filename_full = "wikidata" + '.csv'
    csvfile = df.to_csv(filename_full, index=False)
    print(df)
    print("\nLocal file save code executed !\n")
    return csvfile
  

def get_wiki_data():
    confluence = Confluence(
        url='https://wiki.esma.europa.eu')

    knownerrorsNo = 0
    requestmodelsNo = 0
    userguidesNo = 0
    operationguidesNo = 0
    
    knownerrors = confluence.get_page_child_by_type(86675341, type='page', start=None, limit=None, expand=None)
    for childpage in knownerrors:
        knownerrorsNo += 1
    print(f'The known errors are: {knownerrorsNo}')

    requestmodels = confluence.get_page_child_by_type(86673334, type='page', start=None, limit=None, expand=None)
    for childpage in requestmodels:
        requestmodelsNo += 1
    print(f'The request models are: {requestmodelsNo}')

    userguides = confluence.get_page_child_by_type(86675343, type='page', start=None, limit=None, expand=None)
    for childpage in userguides:
        userguidesNo += 1
    print(f'The user guides are: {userguidesNo}')

    operationguides = confluence.get_page_child_by_type(89195812, type='page', start=None, limit=None, expand=None)
    for childpage in operationguides:
        operationguidesNo += 1
    print(f'The operation guides are: {operationguidesNo}')

    wiki_data = [knownerrorsNo, requestmodelsNo, userguidesNo, operationguidesNo]
    df = pd.DataFrame(columns= ['Known Errors', 'Request models', 'User Guides', 'Operation Guides'])
    df.loc[len(df)] = wiki_data
    print(df)

    return df