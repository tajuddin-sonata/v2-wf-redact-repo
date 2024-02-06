from typing import Union
import functions_framework
from flask import request, g
from werkzeug.exceptions import InternalServerError, BadRequest, NotFound
from jsonschema import ValidationError
from json import dumps
import logging
from traceback import format_exc
from util_input_validation import Config

###Libraries for Azure
import azure.functions as func
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient, BlobClient
from datetime import datetime, timedelta



def impersonate_account(signing_account: str, lifetime: int):
    impersonated_credential = ManagedIdentityCredential(
        client_id=signing_account,
        lifetime=lifetime,
    )
    return impersonated_credential

# def impersonate_account(signing_account: str, lifetime: int):
#     credential = DefaultAzureCredential()
#     target_scopes = "https://storage.azure.com/.default"
#     token_credential = credential.get_token(target_scopes)
#     lifetime = lifetime
#     impersonated_credential = {
#         "token": token_credential.token,
#     }
#     return impersonated_credential

def create_outgoing_file_ref(file: Union[BlobClient, Config.InputFiles.InputFile]):
    if isinstance(file, BlobClient):
        container_name = file.container_name
        blob_properties = file.get_blob_properties()
        return {
            "bucket_name": str(container_name),
            "full_path": str(file.blob_name),
            "version": str(blob_properties.etag),
            "size": str(blob_properties.size),
            "content_type": str(blob_properties.content_settings.content_type),
            "uploaded": str(blob_properties.last_modified) if blob_properties.last_modified else None,
        }
    elif isinstance(file, Config.InputFiles.InputFile):
        return {
            "bucket_name": str(file.bucket_name),
            "full_path": str(file.full_path),
            "version": str(file.version),
            "size": str(file.size),
            "content_type": str(file.content_type),
            "uploaded": str(file.uploaded.isoformat()) if file.uploaded else None,
        }
    else:
        return {}


# @functions_framework.errorhandler(InternalServerError)
# def handle_exception(e):
#     """Return JSON instead of HTML for HTTP errors."""
#     request_json = request.get_json()
#     context_json = g.context
#     msg = {
#         "code": e.code,
#         "name": e.name,
#         "description": e.description,
#         "trace": format_exc(),
#     }
#     logging.error(dumps({**msg, "context": context_json, "request": request_json}))
#     response = e.get_response()
#     response.data = dumps(msg)
#     response.content_type = "application/json"
#     return response


# @functions_framework.errorhandler(NotFound)
# def handle_not_found(e):
#     request_json = request.get_json()
#     context_json = g.context
#     msg = {"code": e.code, "name": e.name, "description": e.description}
#     logging.error(dumps({**msg, "context": context_json, "request": request_json}))
#     response = e.get_response()
#     response.data = dumps(msg)
#     response.content_type = "application/json"
#     return response


# @functions_framework.errorhandler(BadRequest)
# def handle_bad_request(e):
#     msg = {
#         "code": e.code,
#         "name": e.name,
#         "description": e.description,
#     }
#     try:
#         request_json = request.get_json()
#         context_json = request_json.pop("context")
#         if isinstance(e.description, ValidationError):
#             original_error = e.description
#             msg = {
#                 **msg,
#                 "name": "Validation Error",
#                 "description": original_error.message,
#                 "trace": original_error.__str__(),
#             }
#         else:
#             msg = {**msg, "trace": format_exc()}
#         logging.warning(
#             dumps({**msg, "context": context_json, "request": request_json})
#         )
#     except:
#         request_json = request.data
#         msg = {**msg, "trace": format_exc()}
#         logging.warning(dumps({**msg, "request": str(request_json, "utf-8")}))
#     response = e.get_response()
#     response.data = dumps(msg)
#     response.content_type = "application/json"
#     return response


def handle_exception(req: func.HttpRequest, e) -> func.HttpResponse:
    """Return JSON instead of HTML for HTTP errors."""

    request_json = req.get_json()
    context_json = request_json
    logging.info(f'Input Request Received: {context_json}')

    msg = {
        "status": "error", 
        "code": 500,  # InternalServerError 
        "name": "Internal Server Error",
        "description": str(e),  
        "trace": format_exc(),
    }
    logging.error(dumps({**msg, "context": context_json, "request": request_json}))
    return func.HttpResponse(body=dumps(msg), status_code=500, mimetype='application/json')

def handle_not_found(req: func.HttpRequest, e) -> func.HttpResponse:
    
    request_json = req.get_json()
    context_json = request_json
    logging.info(f'Received request: {context_json}')

    msg = {
        "code": 404,  # NotFound 
        "name": "Not Found",
        "description": str(e),  
    }
    logging.error(dumps({**msg, "context": context_json, "request": request_json}))
    return func.HttpResponse(body=dumps(msg), status_code=404, mimetype='application/json')


def handle_bad_request(req: func.HttpRequest, e: Exception) -> func.HttpResponse:
    msg = {
        "code": 400,  # BadRequest 
        "name": "Bad Request",
        "description": str(e),  
    }
    try:
        request_json = req.get_json()
        context_json = request_json.pop("context")
        if isinstance(e, ValidationError):  
            original_error = e
            msg.update({
                "name": "Validation Error",
                "description": original_error.message,
                "trace": str(original_error),
            }) 
        else:
            msg.update({"trace": format_exc()})
        logging.warning(dumps({**msg, "context": context_json, "request": request_json}))
    except Exception as ex:
        request_json = req.get_body().decode('utf-8')
        msg.update({"trace": format_exc(), "request": str(request_json)})
        logging.warning(dumps(msg))
    return func.HttpResponse(body=dumps(msg), status_code=400, mimetype='application/json')