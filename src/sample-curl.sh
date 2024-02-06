###############################################
#############  Sample Curl ####################
###############################################

#!/bin/bash

# Replace with your Azure Function App URL
###"https://<function-app-name>.azurewebsites.net/api/<function-key>"
function_url="https://<function-app-name>.azurewebsites.net/api/<function-key>"

# Replace with your Azure Function App key or any other authentication mechanism
api_key="<function-key>"  ###"your-api-key (master-key)"
set -x

curl -m 59 -X POST "$function_url" -H "x-functions-key: $api_key" -H "Content-Type: application/json" -d '{"context": {"azure_subscription": "sub-dev","azure_location": "east us","client_id": "customer1","interaction_id": "test","execution_id": "id-1234"},"input_files": {"transcript":{"bucket_name": "247ai-stg-cca-customer1-staging","full_path": "2024/01/19/test/20240119035220_id-1234/test_transcript.json","version": "0x8DC18AB030D9E34"}, "nlp":{"bucket_name": "247ai-stg-cca-customer1-staging","full_path": "2024/01/19/test/20240119035220_id-1234/test_nlp.json","version": "0x8DC18B3C97BE33B"},"audio":{"bucket_name": "247ai-stg-cca-customer1-staging","full_path": "2024/01/19/test/20240119035220_id-1234/test_compressed_audio.mka","version": "0x8DC18A9D6477626"}},"staging_config": {"bucket_name": "247ai-stg-cca-customer1-staging","folder_path": "2024/01/19/test/20240119035220_id-1234","file_prefix": "test"},"function_config": {"signing_account": "","redact_config": {"types_to_redact": ["PERSON", "ADDRESS", "EMAIL"]}}}'

#####################################
#################### OR #############
#!/bin/bash

# Replace with your Azure Function App URL
###"https://<function-app-name>.azurewebsites.net/api/<function-key>"
function_url="https://<function-app-name>.azurewebsites.net/api/<function-key>"

# Replace with your Azure Function App key or any other authentication mechanism
api_key="<function-key>"  ###"your-api-key (master-key)"
set -x

curl -m 59 -X POST "$function_url" \
-H "x-functions-key: $api_key" \
-H "Content-Type: application/json" \
-d '{
    "context": {
        "azure_subscription": "sub-dev",
        "azure_location": "east us",
        "client_id": "customer1",
        "interaction_id": "test",
        "execution_id": "id-1234"
    },
    "input_files": {
        "transcript":{
            "bucket_name": "247ai-stg-cca-customer1-staging",
            "full_path": "2024/01/19/test/20240119035220_id-1234/test_transcript.json",
            "version": "0x8DC18AB030D9E34"
        }, 
        "nlp":{
            "bucket_name": "247ai-stg-cca-customer1-staging",
            "full_path": "2024/01/19/test/20240119035220_id-1234/test_nlp.json",
            "version": "0x8DC18B3C97BE33B"
        },
        "audio":{
            "bucket_name": "247ai-stg-cca-customer1-staging",
            "full_path": "2024/01/19/test/20240119035220_id-1234/test_compressed_audio.mka",
            "version": "0x8DC18A9D6477626"
        }
    },
    "staging_config": {
        "bucket_name": "247ai-stg-cca-customer1-staging",
        "folder_path": "2024/01/19/test/20240119035220_id-1234",
        "file_prefix": "test"
    },
    "function_config": {
        "signing_account": "",
        "redact_config": {
            "types_to_redact": ["PERSON", "ADDRESS", "EMAIL"]
        }
    }
}
'


