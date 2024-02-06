from time import time
from typing import Union
import logging
from datetime import datetime, timedelta, timezone
from json import dumps, loads
from pathlib import Path
from uuid import uuid1
import functions_framework
from flask import Request, abort, g
from flask_expects_json import expects_json
from os import environ

import os
import sys
import json
import subprocess

#Azure libraries
import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from werkzeug.exceptions import InternalServerError, BadRequest, NotFound


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

start_time=time()

from util_input_validation import schema, Config
from util_helpers import (
    handle_bad_request,
    handle_exception,
    handle_not_found,
    impersonate_account,
    create_outgoing_file_ref,
)
# from werkzeug.exceptions import BadRequest, NotFound
from redact_text import redact_transcript_and_nlp
from redact_media import redact_media_file

# import ffmpeg
from tempfile import TemporaryDirectory 

# from util_helpers import bad_request, not_found, generic_500, impersonate_account

### GLOBAL Vars
# Env Vars
# service = environ.get("K_SERVICE")

### Instance-wide storage Vars
instance_id = str(uuid1())
run_counter = 0

connection_string = os.environ['StorageAccountConnectionString']
storage_client = BlobServiceClient.from_connection_string(connection_string)

time_cold_start = time() - start_time


### MAIN
# @functions_framework.http
# @expects_json(schema)

app = func.FunctionApp()
@app.function_name(name="wf_redact_HttpTrigger1")
@app.route(route="wf_redact_HttpTrigger1")
def main(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    # Input Variables
    global run_counter
    run_counter += 1
    request_recieved = datetime.now(timezone.utc)
    request_json = req.get_json()
    CONFIG = Config(request_json)
    del request_json
    context = {
        **CONFIG.context.toJson(),
        "instance": instance_id,
        "instance_run": run_counter,
        "request_recieved": request_recieved.isoformat(),
    }

    # Output Variables
    response_json = {
        "staged_files":{},
        "required_redaction":False
    }
    out_files = {}
    # logging.info(dumps({"fail": CONFIG["missing"]}))

    # REQUIRED Input Files:
    # - NLP
    # - Transcript
    # OPTIONAL input Files:
    # - MKA (compressed audio)
    # - Video

    ##### check and Install ffmpeg package #######
    try:
        ## Check if FFmpeg is already installed
        check_ffmpeg_installed = "ffmpeg -version"
        subprocess.run(check_ffmpeg_installed, shell=True, check=True)
        logging.info("FFmpeg is already installed.")
    except subprocess.CalledProcessError:
        ## If FFmpeg is not installed, attempt to install it
        install_command = "apt-get install -y ffmpeg || yum install -y ffmpeg"
        try:
            subprocess.run(install_command, shell=True, check=True)
            logging.info("FFmpeg installed successfully.")
        except subprocess.CalledProcessError as e:
            ## Log the exception details
            logging.exception("Error installing FFmpeg: %s", e)

    ### Get Staging Bucket
    staging_bucket = storage_client.get_container_client(CONFIG.staging_config.bucket_name)
    ### GET Blobs
    nlp_blob = storage_client.get_container_client(CONFIG.input_files.nlp.bucket_name).get_blob_client(
        CONFIG.input_files.nlp.full_path, #version_id=CONFIG.input_files.nlp.version
    )
    transcript_blob = storage_client.get_container_client(
        CONFIG.input_files.transcript.bucket_name
    ).get_blob_client(
        CONFIG.input_files.transcript.full_path,
        # version_id=CONFIG.input_files.transcript.version,
    )
 
    try:
        ### Try to fetch blob properties with the condition that the ETag must match the desired_etag
        nlp_etag_value = nlp_blob.get_blob_properties(if_match=CONFIG.input_files.nlp.version)
        logging.info(f'nlp Blob Name: {nlp_blob.blob_name}')
        logging.info(f'nlp Blob ETag: {nlp_etag_value["etag"]}')

        transcript_etag_value = transcript_blob.get_blob_properties(if_match=CONFIG.input_files.transcript.version)
        logging.info(f'Transcript Blob Name: {transcript_blob.blob_name}')
        logging.info(f'Transcript Blob ETag: {transcript_etag_value["etag"]}')

    except ResourceNotFoundError:
        ### Handle the case where the blob with the specified ETag is not found
        abort(404, "nlp or transcript input_files not found on buckets")

    ###### MAIN ######

    #### Download NLP + Transcript
    nlp_bytes = loads(nlp_blob.download_blob().readall())
    transcript_bytes = loads(transcript_blob.download_blob().readall())
    del nlp_blob
    del transcript_blob
    ### Analyse NLP for NER, Then Redact NLP +Transcript
    (
        redacted_transcript_json,
        redacted_nlp_json,
        redacted_timings,
        required_redaction
    ) = redact_transcript_and_nlp(
        transcript_bytes, nlp_bytes, CONFIG.function_config.redact_config.toJson()
    )
    del nlp_bytes
    del transcript_bytes
    
    if required_redaction==False:
        # return response_json, 200
        return func.HttpResponse(json.dumps(response_json), status_code=200, mimetype='application/json')

    ### Create Staging Paths for Transcript + NLP
    staging_redacted_transcript_path = (
        Path(
            CONFIG.staging_config.folder_path,
            CONFIG.staging_config.file_prefix + "_" + "redacted_transcript",
        )
        .with_suffix(".json")
        .as_posix()
    )
    staging_redacted_nlp_path = (
        Path(
            CONFIG.staging_config.folder_path,
            CONFIG.staging_config.file_prefix + "_" + "redacted_nlp",
        )
        .with_suffix(".json")
        .as_posix()
    )

    ### Upload Transcript + NLP
    staging_redacted_transcript_blob = staging_bucket.get_blob_client(
        staging_redacted_transcript_path
    )
    staging_redacted_nlp_blob = staging_bucket.get_blob_client(staging_redacted_nlp_path)

    staging_redacted_transcript_blob.upload_blob(
        data=dumps(redacted_transcript_json), content_type="application/json", overwrite=True
    )
    staging_redacted_nlp_blob.upload_blob(
        data=dumps(redacted_nlp_json), content_type="application/json", overwrite=True
    )
    ### Confirm uploaded
    if not staging_redacted_transcript_blob.exists():
        abort(500, "redacted transcript was not successfully uploaded")
    if not staging_redacted_nlp_blob.exists():
        abort(500, "redacted nlp was not successfully uploaded")

    out_files["redacted_transcript"] = create_outgoing_file_ref(
        staging_redacted_transcript_blob
    )
    out_files["redacted_nlp"] = create_outgoing_file_ref(staging_redacted_nlp_blob)

    # logging.warning(dumps({"timings": redacted_timings}))
    # response_json["timings"] = redacted_timings

    ### MEDIA
    # IF there are any words to redact (len(redacted_timings)>0) then redact,
    # otherise return original file location.

    ### Redact MKA (if present)
    for media_type, media_file in [
        (key, val)
        for key, val in CONFIG.input_files.items()
        if key.lower() not in ["transcript", "nlp"] and val != None
    ]:
        media_file: Config.InputFiles.InputFile = media_file
        if (redacted_timings == None) or len(redacted_timings) < 1:
            # Do Nothing, return file
            out_files["redacted_" + str(media_type)] = create_outgoing_file_ref(
                media_file
            )
            continue
        ### Get Media Blob
        media_blob = storage_client.get_container_client(media_file.bucket_name).get_blob_client(
            media_file.full_path, #version_id=media_file.version
        )

        try:
        ### Try to fetch blob properties with the condition that the ETag must match the desired_etag
            media_etag_value = media_blob.get_blob_properties(if_match=media_file.version)
            logging.info(f'Media Blob Name: {media_blob.blob_name}')
            logging.info(f'Media Blob ETag: {media_etag_value["etag"]}')

        except ResourceNotFoundError:
            # Handle the case where the blob with the specified ETag is not found
            abort(404, "Media file not found on bucket")

        ### if not media_blob:
        #     abort(404, "{} input_file not found on bucket".format(media_type))
            
        ### If blob exists, Generate Shared Access Signature (SAS) Token
        sas_token = generate_blob_sas(
                account_name=storage_client.account_name,
                account_key=storage_client.credential.account_key,
                container_name=media_file.bucket_name,
                blob_name=media_file.full_path,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(minutes=30),
        )

        #### Log SAS token and request details
        logging.info(f"SAS Token: {sas_token}")
        logging.info(f"Request Headers: {dict(req.headers)}")

        media_blob_url = media_blob.url
        logging.info(f'Media Blob URL: {media_blob_url}')
    
        ### Combine the blob URL with the SAS token to get the signed URL
        media_signed_url = f"{media_blob_url}?{sas_token}"
        ### Logging the staged_media_signed_url
        logging.info(f"Staged Media Signed URL: {media_signed_url}")
    
        ### Create Temp mem folder
        with TemporaryDirectory() as tmpdir:
            # Run Redaction
            redacted_media_temp_file_path = redact_media_file(
                tmpdir,
                media_file.full_path,
                media_signed_url,
                redacted_timings,
                redacted_transcript_json["metadata"]["duration"],
            )
            ### Generate Redacted Media Staging Path
            staging_redacted_media_path = (
                Path(
                    CONFIG.staging_config.folder_path,
                    CONFIG.staging_config.file_prefix + "_" + "redacted_" + str(media_type),
                )
                .with_suffix(Path(media_file.full_path).suffix)
                .as_posix()
            )
            #### Generate Redacted Media upload blob
            staging_redacted_media_blob = staging_bucket.get_blob_client(staging_redacted_media_path)

            ### Upload Redacted Media File
            # staging_redacted_media_blob.upload_from_filename(redacted_media_temp_file_path)
            with open(redacted_media_temp_file_path, "rb") as f:
                staging_redacted_media_blob.upload_blob(f, timeout=300, overwrite=True)

            if not staging_redacted_media_blob.exists():
                abort(500, "redacted {} was not successfully uploaded".format(media_type))
            #### Add to return list
            out_files["redacted_" + str(media_type)] = create_outgoing_file_ref(
                staging_redacted_media_blob
            )

    # Return with all the locations
    response_json["status"] = "success"
    response_json["staged_files"] = out_files
    # return response_json, 200
    logging.info(f"response_json_output: {response_json}")
    return func.HttpResponse(body=dumps(response_json), status_code=200, mimetype='application/json') 