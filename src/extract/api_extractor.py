import json
import requests
import logging
from src.utils.file_utils import get_incremental_data

logger = logging.getLogger('pipeline')


def get_data(base_url:str, endpoint:str, data_field:str=None, params:dict=None, headers:dict=None) -> dict | list | None:
    """
    Make a GET request to an API to obtain data.

    Args:
    base_url (str): The base URL of the API.
    endpoint (str): The API endpoint to which the request will be made.
        params (dict): Query parameters to send with the request.
        data_field (str): The name of the field in the JSON that contains the data.
        headers (dict): Headers to send with the request.
    
    Returns:
        dict|list|None: The data obtained from the API in JSON format.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()
        
        logger.debug(f'Status code: {response.status_code}. Request accepted!')
        
        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except:
            logger.error("The response is not valid JSON.")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"The request failed. Error code: {e}")
        
        
def get_incremental_extraction(incremental_file_path: str, base_url: str, endpoint: str, params: dict = None, headers: dict = None) -> dict|None:
    """
    Perform an incremental extraction using 'id' as the incremental field.
    Save the last id in a JSON file for the next execution.
    
    Args:
        incremental_file_path (str): relative path to the .json file with the incremental control variable.
        base_url (str): The base URL of the API.
        endpoint (str): The API endpoint to which the request will be made.
        params (dict): Query parameters to send with the request.
        headers (dict): Headers to send with the request.
    
    Returns:
        dict|None: The data obtained from the API in JSON format.
    
    """
    # Read last saved ID
    incremental_content = get_incremental_data(incremental_file_path)
    if not incremental_content or ('last_value' not in incremental_content and 'previous_value' not in incremental_content):
        logger.error('Incremental file not created or without "last_value" or "previous_value" field.')
        return None

    last_value = incremental_content['last_value']
    previous_value =incremental_content['previous_value']
        
    params = params or {}
    params["fromId"] = last_value + 1

    # API call
    data = get_data(base_url, endpoint, params=params, headers=headers)
    logger.debug(f"Requesting from ID: {last_value + 1}")

    # Filter only IDs greater than the last value
    new_value = max(d['id'] for d in data)

    # Update the last value
    with open(incremental_file_path, "w", encoding="utf-8") as f:
        json.dump({"previous_value":previous_value,"last_value": new_value}, f, indent=4, ensure_ascii=False)

    logger.debug(f"Updated incremental file: {last_value} -> {new_value}")

    return data
