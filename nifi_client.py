import requests
import uuid
import re
import time

# 🔧 Конфигурация подключения
NIFI_API = "https://localhost:8443/nifi-api"
USERNAME = "admin"
PASSWORD = "111111111111111"
VERIFY_SSL = False

def guess_target_table(select_query: str) -> str:
    match = re.search(r"from\s+([a-zA-Z0-9_]+)", select_query, re.IGNORECASE)
    return f"{match.group(1)}_summary" if match else "default_summary"

def get_token() -> str:
    response = requests.post(
        f"{NIFI_API}/access/token",
        data={"username": USERNAME, "password": PASSWORD},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=VERIFY_SSL
    )
    if response.status_code not in (200, 201):
        raise Exception(f"Не удалось получить токен: {response.status_code} {response.text}")
    return response.text.strip()

def create_process_group(name="etl_autogen", position=(100.0, 100.0)):
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    root = requests.get(
        f"{NIFI_API}/flow/process-groups/root",
        headers=headers,
        verify=VERIFY_SSL
    )
    if root.status_code != 200:
        raise Exception(f"Ошибка получения root_id: {root.status_code}: {root.text}")

    root_id = root.json()["processGroupFlow"]["id"]

    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": position[0], "y": position[1]}
        }
    }

    resp = requests.post(
        f"{NIFI_API}/process-groups/{root_id}/process-groups",
        headers=headers,
        json=body,
        verify=VERIFY_SSL
    )
    if resp.status_code not in (200, 201):
        raise Exception(f"Ошибка создания группы: {resp.status_code}: {resp.text}")

    return resp.json()["id"], headers

def create_controller_service(group_id, service_type, service_name, properties, headers):
    body = {
        "revision": {"version": 0},
        "component": {
            "name": service_name,
            "type": service_type,
            "properties": properties,
            "state": "ENABLED"
        }
    }

    resp = requests.post(
        f"{NIFI_API}/process-groups/{group_id}/controller-services",
        headers=headers,
        json=body,
        verify=VERIFY_SSL
    )

    if resp.status_code not in (200, 201):
        raise Exception(f"Ошибка создания Controller Service {service_name}: {resp.status_code}: {resp.text}")

    return resp.json()["component"]["id"]

def add_processor(group_id, name, type_, position, config_props, headers):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": type_,
            "name": name,
            "position": {"x": position[0], "y": position[1]},
            "config": {
                "properties": config_props,
                "schedulingPeriod": "1 sec",
                "autoTerminatedRelationships": ["success"]
            }
        }
    }

    resp = requests.post(
        f"{NIFI_API}/process-groups/{group_id}/processors",
        headers=headers,
        json=body,
        verify=VERIFY_SSL
    )

    if resp.status_code not in (200, 201):
        raise Exception(f"Ошибка добавления процессора {name}: {resp.status_code}: {resp.text}")

    return resp.json()["id"]

def connect_processors(group_id, source_id, dest_id, headers):
    connection_name = f"conn-{uuid.uuid4().hex[:8]}"
    body = {
        "revision": {"version": 0},
        "component": {
            "name": connection_name,
            "source": {"id": source_id, "type": "PROCESSOR", "groupId": group_id},
            "destination": {"id": dest_id, "type": "PROCESSOR", "groupId": group_id},
            "selectedRelationships": ["success"],
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
            "bends": [],
            "loadBalanceStrategy": "DO_NOT_LOAD_BALANCE",
            "loadBalanceCompression": "DO_NOT_COMPRESS",
            "labelIndex": 1
        }
    }

    resp = requests.post(
        f"{NIFI_API}/process-groups/{group_id}/connections",
        headers=headers,
        json=body,
        verify=VERIFY_SSL
    )
    if resp.status_code not in (200, 201):
        raise Exception(f"Ошибка соединения процессоров: {resp.status_code}: {resp.text}")

def generate_etl_flow(select_query: str):
    target_table = guess_target_table(select_query)
    pg_id, headers = create_process_group()

    dbcp_id = create_controller_service(
        group_id=pg_id,
        service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
        service_name="AutoGen_DB_Connection",
        properties={
            "Database Connection URL": "jdbc:postgresql://localhost:5432/superset",
            "Database Driver Class Name": "org.postgresql.Driver",
            "Database Driver Location(s)": "/opt/nifi/nifi-current/lib/postgresql-42.7.4.jar",
            "Database User": "superset",
            "Password": "superset"
        },
        headers=headers
    )

    reader_id = create_controller_service(
        group_id=pg_id,
        service_type="org.apache.nifi.avro.AvroReader",
        service_name="AutoGen_AvroReader",
        properties={
            "schema-access-strategy": "embedded-avro-schema"
        },
        headers=headers
    )

    writer_id = create_controller_service(
        group_id=pg_id,
        service_type="org.apache.nifi.avro.AvroRecordSetWriter",
        service_name="AutoGen_AvroWriter",
        properties={},
        headers=headers
    )

    time.sleep(3)

    sql_id = add_processor(
        group_id=pg_id,
        name="ExecuteSQL",
        type_="org.apache.nifi.processors.standard.ExecuteSQL",
        position=(0.0, 0.0),
        config_props={
            "SQL Query": select_query,
            "Database Connection Pooling Service": dbcp_id
        },
        headers=headers
    )

    convert_id = add_processor(
        group_id=pg_id,
        name="ConvertRecord",
        type_="org.apache.nifi.processors.standard.ConvertRecord",
        position=(200.0, 0.0),
        config_props={
            "Record Reader": reader_id,
            "Record Writer": writer_id
        },
        headers=headers
    )

    put_id = add_processor(
        group_id=pg_id,
        name="PutDatabaseRecord",
        type_="org.apache.nifi.processors.standard.PutDatabaseRecord",
        position=(400.0, 0.0),
        config_props={
            "put-db-record-table-name": target_table,
            "put-db-record-dcbp-service": dbcp_id,
            "put-db-record-record-reader": reader_id,
            "db-type": "PostgreSQL",
            "put-db-record-statement-type": "UPSERT"
        },
        headers=headers
    )

    time.sleep(3)
    connect_processors(pg_id, sql_id, convert_id, headers)
    connect_processors(pg_id, convert_id, put_id, headers)

    return {
        "status": "ok",
        "message": "Flow created successfully",
        "process_group_id": pg_id
    }
