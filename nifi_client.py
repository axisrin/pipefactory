import requests

NIFI_API = "http://localhost:8081/nifi-api"

def create_nifi_flow(payload):
    root_pg = requests.get(f"{NIFI_API}/flow/process-groups/root").json()
    root_id = root_pg["processGroupFlow"]["id"]

    return {
        "created_in_group": root_id,
        "message": "Flow generation logic placeholder"
    }