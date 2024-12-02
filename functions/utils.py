from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import requests
import logging
import json
import pytz

def get_current_date_in_timezone(timezone_str, separator='/'):
    # Set the timezone based on the provided string
    tz = pytz.timezone(timezone_str)
    
    # Get the current time in the specified timezone
    current_datetime = datetime.now(tz)
    
    # Format the date as YYYY<separator>MM<separator>DD
    formatted_date = current_datetime.strftime(f'%Y{separator}%m{separator}%d')
    
    return formatted_date

def orchestrator_post_request(request_url, request_body):
    response = requests.post(url=request_url, json=request_body)
    print(response)
    print(response.text)
    return


def load_request_body(file_path="tournaments_to_ingest.json"):
    with open(file_path, "r") as file:
        tournaments = json.load(file)
    return tournaments

def find_item_in_array_of_objects(array_of_objects, key, value, item_name):
    """
    Searches for an item in a list of dictionaries where a specific key matches a given value.

    :param array_of_objects: The list of dictionaries to search in.
    :param key: The key to check.
    :param value: The value to match.
    :param item_name: The name of the item (for error messages).
    :return: The matching dictionary if found, else raises a ValueError.
    """
    item = next((item for item in array_of_objects if item[key] == value), None)
    if not item:
        raise ValueError(f"{item_name} '{value}' not found.")
    return item


def create_blob_service_client(connection_string: str = None, account_url: str = None):
    """
    Returns a BlobServiceClient based on the provided connection method.
    
    :param connection_string: Connection string for the storage account.
    :param account_url: URL of the storage account for managed identity authentication.
    :return: BlobServiceClient instance.
    """
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    elif account_url:
        credential = DefaultAzureCredential()
        return BlobServiceClient(account_url=account_url, credential=credential)
    else:
        raise ValueError("Either a connection string or an account URL must be provided.")
    

def list_blobs(blob_service_client: BlobServiceClient, container_name: str = None, path: str = ""):
    """
    Lists all blobs in a specific path within a container with flexible authentication.
    
    :param connection_string: Azure Storage account connection string.
    :param account_url: Azure Storage account URL for managed identity authentication.
    :param container_name: Name of the container in the data lake.
    :param path: Path to list blobs from (use an empty string to list all blobs in the container).
    :return: List of blob names.
    """
    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # List blobs in the specified path
        blob_list = container_client.list_blobs(name_starts_with=path)
        blobs = [blob.name for blob in blob_list]
        
        print(f"Blobs in path '{path}': {blobs}")
        return blobs
    
    except Exception as e:
        logging.info("PYLOG: Error in function list_blobs")
        print(f"An error occurred: {e}")
        raise

def read_blob(blob_service_client: BlobServiceClient, container_name: str, blob_name: str):
    """
    Reads the content of a blob from Azure Blob Storage.

    :param container_name (str): The name of the container containing the blob.
    :param blob_name (str): The name of the blob to read.
    :return str: The content of the blob as a string.
    """
    try:
        # Get a client for the specified container
        container_client = blob_service_client.get_container_client(container_name)
        # Get a client for the specified blob
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download the blob's content as a string
        blob_data = blob_client.download_blob().readall()
        blob_content = blob_data.decode('utf-8')  # Decode bytes to string
        
        print(f"Successfully read blob: {blob_name}")
        return blob_content
    except Exception as e:
        print(f"Error trying to read blob {blob_name} from container {container_name}: {e}")
        raise

def upload_blob(blob_service_client: BlobServiceClient, container_name: str, blob_path: str, data: bytes, overwrite: bool = True):
    """
    Uploads data to Azure Data Lake as a blob with flexible authentication.
    
    :param connection_string: Azure Storage account connection string.
    :param account_url: Azure Storage account URL for managed identity authentication.
    :param container_name: Name of the container in the data lake.
    :param blob_path: Path of the blob within the container.
    :param data: Data to upload (bytes).
    """
    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)
        # Get the blob client
        blob_client = container_client.get_blob_client(blob_path)
        
        # Upload the data
        blob_client.upload_blob(data, overwrite=overwrite)
        print(f"Blob uploaded successfully to {blob_path}")

    except Exception as e:
        logging.info("PYLOG: Error in function upload_blob")
        print(f"An error occurred: {e}")
        raise

    return


def logic_app_notificator(logic_app_url: str, chat_id: str, message: str) -> bool:
    """
    Sends a notification to a Logic App that handles Telegram messaging.

    :param logic_app_url: The HTTP endpoint of the Logic App.
    :param chat_id: The chat ID to send the message to.
    :param message: The message text to be sent.
    :return: True if the notification was successfully sent, False otherwise.
    """
    # Prepare the payload for the Logic App
    payload = {
        "chat_id": chat_id,
        "message": message
    }

    try:
        # Make the HTTP POST request to trigger the Logic App
        response = requests.post(logic_app_url, json=payload)
        response.raise_for_status()
        print("Notification sent successfully through Logic Apps!")
        return True
    except requests.exceptions.RequestException as e:
        logging.info("PYLOG: Error in function logic_app_notificator")
        print(f"Error occurred while calling the Logic App: {e}")
        return False
    
    
def notification_message(mode=0, **kwargs):
    if mode == 0:
        return("❌ JOB FAILURE ALERT \n\n"
            "🏷️ Resource: {resource}\n"
            "🔎 Job name: {job_name}\n"
            "🕑 Finished at: {finished_at}\n\n"
            "📋 Error details:\n" 
            "----------\n" 
            "{error_message}\n" 
            "----------\n\n" 
            "⚠️ Action Required: Please check the logs and resolve the issue."
            ).format(**kwargs)
    elif mode == 1:
        return(
            "✅ JOB SUCCESS\n\n"
            "🏷️ Resource: {resource}\n"
            "🔎 Job name: {job_name}\n"
            "🕑 Finished at: {finished_at}"
        ).format(**kwargs)
    
def execute_with_notification(code_callable, logic_app_url, telegram_chat_id, resource, job_name, notificate_success=False):
    """
    Executes a block of code with success and failure notifications.
    :param resource: The resource running the job.
    :param job_name: The name of the job.
    :param logic_app_url: The URL for the Logic App.
    :param telegram_chat_id: The Telegram chat ID for notifications.
    :param code_callable: A callable representing the code to execute.
    :return: The result of the code_callable, if any.
    """
    success = True
    error = None
    try:
        # Execute the provided callable
        code_callable()
    except Exception as e:
        success = False
        error = e
    finally:
        tz = pytz.timezone("America/Sao_Paulo")
        current_datetime = datetime.now(tz).strftime("%d/%m/%Y - %H:%M:%S")
        if success:
            if notificate_success:
                message = notification_message(
                    mode=1,
                    resource=resource,
                    job_name=job_name, 
                    finished_at=current_datetime
                )
            else: 
                return
        else:
            message = notification_message(
                mode=0,
                resource=resource,
                job_name=job_name, 
                finished_at=current_datetime, 
                error_message = error
            )
        
        logic_app_notificator(logic_app_url, telegram_chat_id, message)

        if not success:
            raise error
        
        return





