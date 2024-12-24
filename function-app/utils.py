from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from datetime import datetime
import requests
import logging
import json
import pytz

def get_current_date_in_timezone(timezone_str: str, date_format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Get the current time in a specified format, for a given timezone.
    :param timezone_str: Timezone value. For all possible values, refer to https://api-custom.eyesover.com/public/getZoneNames.
    :param date_format: Format of the date to return. Default is "%Y-%m-%d %H:%M:%S".
    :return: The current date in the specified timezone and format.
    """
    tz = pytz.timezone(timezone_str)
    current_datetime = datetime.now(tz)
    formatted_date = current_datetime.strftime(date_format)
    
    return formatted_date

def http_response_template(func: func, status_code: int, response: str) -> func.HttpResponse:
    """
    Creates an HTTP response object with the provided status code and response.
    :param func: The `azure.functions` module, which contains the `HttpResponse` class.
    :param status_code: The status code to return in the response.
    :parm response: The response body to return.
    :return: func.HttpResponse object.
    """
    if status_code == 200:
        return func.HttpResponse(
                str(response),
                status_code=200
            )
    else:
        return func.HttpResponse(
                str(response),
                status_code=500
            )


def find_item_in_array_of_objects(array_of_objects: list[dict], key: str, value: str, item_name: str) -> dict:
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


def create_blob_service_client(connection_string: str = None, account_url: str = None) -> BlobServiceClient: 
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
    

def list_blobs(blob_service_client: BlobServiceClient, container_name: str = None, path: str = "")  -> list[str]:
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
        
        logging.info(f"PYLOG: Found these blobs in path '{path}': {blobs}")
        return blobs
    
    except Exception as e:
        logging.info("PYLOG: Error in function list_blobs")
        logging.info(f"An error occurred: {e}")
        raise

def read_blob(blob_service_client: BlobServiceClient, container_name: str, blob_name: str) -> str:
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
        
        logging.info(f"PYLOG: Successfully read blob: {blob_name}")
        return blob_content
    except Exception as e:
        logging.info(f"PYLOG: Error trying to read blob {blob_name} from container {container_name}: {e}")
        raise

def upload_blob(blob_service_client: BlobServiceClient, container_name: str, blob_path: str, data: bytes, overwrite: bool = True) -> None:
    """
    Uploads data to Azure Data Lake as a blob.
    :param blob_service_client: The BlobServiceClient to perform the desired operation.
    :param container_name: Name of the container in the data lake.
    :param blob_path: Path of the blob within the container.
    :param data: Data to upload (bytes).
    :param overwrite: Whether to overwrite the blob if it already exists (default is True).
    :return: None
    """
    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)
        # Get the blob client
        blob_client = container_client.get_blob_client(blob_path)
        
        # Upload the data
        blob_client.upload_blob(data, overwrite=overwrite)
        logging.info(f"PYLOG: Blob uploaded successfully to {blob_path}")

    except Exception as e:
        logging.info(f"PYLOG: Error in function upload_blob\n Error: {str(e)}")
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
        logging.info("PYLOG: Notification sent successfully through Logic Apps!")
        return True
    except requests.exceptions.RequestException as e:
        logging.info(f"PYLOG: Error in function logic_app_notificator\nError: {str(e)}")
        return False
    
    
def notification_message(mode: int = 0, **kwargs) -> str:
    """
    Returns a notification message for success or failure of a job.
    :param mode: The mode of the message (0 for failure, 1 for success).
    :param kwargs: The keyword arguments to be formatted into the message.
    :return: The formatted notification message.
    """
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
            "📋 Job details: {job_details}\n"
            "🕑 Finished at: {finished_at}"
        ).format(**kwargs)


def build_notification_message(status_code: int, response: str, resource: str, job_name: str, notificate_success: bool) -> str:
    """
    Builds a notification message based on the status code and response of a job.
    :param status_code: The status code of the job execution.
    :param response: The response message from the job.
    :param resource: The resource running the job.
    :param job_name: The name of the job or, in this case, of the function.
    :param notificate_success: Whether to send a notification for successful jobs.
    :return: The formatted notification message.
    """
    current_datetime = get_current_date_in_timezone("America/Sao_Paulo", "%d/%m/%Y - %H:%M:%S")
    if status_code == 200:
        if notificate_success:
            message = notification_message(
                mode=1,
                resource=resource,
                job_name=job_name,
                job_details=response, 
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
            error_message = response
        )
    return message


def function_notificator(logic_app_url: str, telegram_chat_id: str, status_code: int, response: str, resource: str, job_name: str, notificate_success: bool) -> None:
    """
    Gets a notification message based on the job status and sends it to a Logic Apps for Telegram notification.
    :param logic_app_url: The URL for the Logic App.
    :param telegram_chat_id: The Telegram chat ID for notifications.
    :param status_code: The status code of the job execution.
    :param response: The response message from the job.
    :param resource: The resource running the job.
    :param job_name: The name of the job or, in this case, of the function.
    :param notificate_success: Whether to send a notification for successful jobs.
    :return: None
    """
    message = build_notification_message(status_code, response, resource, job_name, notificate_success)
    if message:    
        logic_app_notificator(logic_app_url, telegram_chat_id, message)
    return


def execute_with_notification(code_callable, logic_app_url, telegram_chat_id, resource, job_name, job_details="No details for this job", notificate_success=False):
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
                    job_details=job_details, 
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
        
        return