import logging
from datetime import datetime, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import tempfile
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.storage.blob import BlobServiceClient

# ----------------- Comon Methods -----------------
def ft_datetime_to_string_hour_min(input_date_datetime):
    # return ('{:%Y-%m-%d-%H:%M}'.format(input_date_datetime))
    return str('{:%H:%M}'.format(input_date_datetime))

def ft_update_check(update_start_string, update_end_string, update_start_string_secondary, update_end_string_secondary):
    # Fortmat update_start_string and update_end_string
    # update_start_string = '19:30'
    # update_end_string = '23:30'
    # update_start_string_secondary = '00:30'
    # update_end_string_secondary = '05:30'
    # note: datetime.now() is GMT time, hence adding 2 hours for Paris time
    now_hour_min = ft_datetime_to_string_hour_min(datetime.now()+ timedelta(hours=2))
    return (((now_hour_min <= update_end_string) and (now_hour_min >= update_start_string))
            or ((now_hour_min <= update_end_string_secondary) and (now_hour_min >= update_start_string_secondary))), now_hour_min

def ft_df_export_csv_to_container(df, connect_str, container_name, file_name):
    # ---------- Storage account
    local_file_name = file_name + '.csv'
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        # with tempfile.NamedTemporaryFile() as temp:
        file_name_temp = temp.name + '.csv'
        df.to_csv(file_name_temp, index=False)
        with open(file_name_temp, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        temp.close()
    logging.info('Uploaded successfully file: %s', local_file_name)
    logging.info('to Azure Blob Storage container: %s', container_name)
    return df

def ft_retrieve_from_keyvault(keyVaultName, secret_name):
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    retrieved_storage_container_string = client.get_secret(secret_name)
    retrieved_secret = retrieved_storage_container_string.value
    logging.info("\nRetrieved from KeyVault : %s", secret_name)
    return retrieved_secret