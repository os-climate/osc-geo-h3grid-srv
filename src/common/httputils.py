# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-08 by davis.broda@brodagroupsoftware.com
import logging
from typing import Optional, Dict, Any

import requests

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)
logger = logging.getLogger(__name__)


def httprequest(host: str, port: int, service: str, method: str,
                data: Optional[Any] = None, obj: Optional[Dict] = None,
                files: Optional[Any] = None, params: Optional[Dict] = None,
                log_full_request: bool = True
                ) -> requests.Response | Dict[str, Any]:
    """
    Generic request function using the requests library.
    On success, a True boolean is returned.
    On errors, a log message is issued, but only
    a False boolean will be returned.
    To simplify execution, exceptions are purposely NOT
    caught (but they are logged) since this expects to be
    used in a CLI.

    Parameters:
    - service (str): The URL of the service to which the request is made
    - method (str): The HTTP method to use (e.g., GET, POST, PUT, DELETE)
    - data (any, optional): Data to send in the request body, typically for POST requests
    - obj (dict, optional): JSON object to send in the request body
    - files (any, optional): Files to send in the request body
    - log_full_request (bool, optional): whether to include the
        full request object in debug logs, or whether to only include the
        length of the data and object elements, instead of content. Can be
        used to prevent excessively large requests from clogging log files,
        or to prevent sensitive data form being written to logs.

    Returns:
    - requests.Response: The response object
    """
    if log_full_request:
        logger.debug(
            f"Issue request, host:{host} port:{port} method:{method}"
            f" data:{data} obj:{obj} files:{files} params:{params}")
    else:
        data_length = len(str(data))
        obj_len = len(str(obj))
        logger.debug(
            f"Issue request, host:{host} port:{port} method:{method}"
            f" data length in characters:{data_length}"
            f" obj length in characters:{obj_len}"
            f" files:{files} params:{params}")

    response = None
    try:
        url = f"http://{host}:{port}{service}"
        logger.info(f"Using url:{url}")

        # Convert method to uppercase to match HTTP standard
        method = method.upper()

        # If files are being sent a different header type is required
        if files:
            headers = {}  # Leave headers empty, 'requests' will handle 'multipart/form-data'
        else:
            headers = {"Content-Type": "application/json"}

        logger.info(f"Using headers: {headers}")

        if method not in ["GET", "POST", "PUT", "DELETE"]:
            raise ValueError(f"Invalid HTTP method: {method}")

        # Make the appropriate request based on the method
        if method == "GET":
            response = requests.get(url, params=params, headers=headers)
        elif method == "POST":
            if files:
                # Send files as part of the multipart form data request
                response = requests.post(
                    url, data=data, files=files, headers=headers, params=params)
            else:
                # Send JSON data
                response = requests.post(
                    url, json=obj, data=data, headers=headers, params=params)
        elif method == "PUT":
            response = requests.put(url, json=obj, data=data, headers=headers, params=params)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers, params=params)

        if response.status_code != 200:
            import json
            try:
                text = json.loads(response.text)
            except ValueError:
                text = response.text  # If response is not JSON formatted

            msg = f"HTTP error, code:{response.status_code} text:{text} url:{url}"
            logger.error(msg)
            error = {
                "error": "HTTP error",
                "code": response.status_code,
                "text": text,
                "url": url
            }
            return error

    except requests.exceptions.ConnectionError as e:
        msg = f"Connection error: {e}"
        logger.error(msg)
        return {"error": msg}
    except requests.exceptions.RequestException as e:
        msg = e.args[0] if e.args else "Unknown request error"
        if response:
            msg += f" text:{response.text}"
        logger.error(msg)
        return {"error": msg}
    except Exception as e:
        # Catch any unknown exception, log it, and return the error
        msg = f"Exception: {e}"
        logger.error(msg)
        return {"error": msg}

    try:
        return response.json()
    except ValueError:
        return response  # If the response is not JSON, return the raw response object
