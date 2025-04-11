import requests
import uuid
import re
import time

# 🔧 Конфигурация подключения
NIFI_API = "https://localhost:8443/nifi-api"
USERNAME = "admin"
PASSWORD = "111111111111111"
VERIFY_SSL = False  # для самоподписанных сертификатов

print("🔁 nifi_client.py загружен ✅")

def guess_target_table(select_query: str) -> str:
    match = re.search(r"from\s+([a-zA-Z0-9_]+)", select_query, re.IGNORECASE)
    return f"{match.group(1)}_summary" if match else "default_summary"

def get_token() -> str:
    print("🔐 Получение токена...")
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
    print("🏗️ Создание Process Group...")

    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Получение root ID
    root = requests.get(
        f"{NIFI_API}/flow/process-groups/root",
        headers=headers,
        verify=VERIFY_SSL
    )

    print("📡 Ответ от NiFi на /root:")
    print("  ↳ Статус:", root.status_code)
    print("  ↳ Тело:", repr(root.text[:400]))

    if root.status_code != 200:
        raise Exception(f"⚠️ Ошибка получения root_id: {root.status_code}: {root.text}")

    root_id = root.json()["processGroupFlow"]["id"]

    # Создание новой группы
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

    print("📤 Ответ от NiFi на создание Process Group:")
    print("  ↳ Статус:", resp.status_code)
    print("  ↳ Тело:", repr(resp.text[:400]))

    if resp.status_code not in (200, 201):
        raise Exception(f"⚠️ Ошибка создания группы: {resp.status_code}: {resp.text}")

    return resp.json()["id"], headers  # возвращаем ID и заголовки

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
        "revision": {
            "version": 0
        },
        "component": {
            "name": connection_name,
            "source": {
                "id": source_id,
                "type": "PROCESSOR",
                "groupId": group_id
            },
            "destination": {
                "id": dest_id,
                "type": "PROCESSOR",
                "groupId": group_id
            },
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

    print("🔗 Ответ от NiFi на соединение процессоров:")
    print("  ↳ Статус:", resp.status_code)
    print("  ↳ Тело:", repr(resp.text[:1000]))

    if resp.status_code not in (200, 201):
        raise Exception(f"Ошибка соединения процессоров: {resp.status_code}: {resp.text}")


def generate_etl_flow(select_query: str):
    print("🚀 Вход в generate_etl_flow()")

    target_table = guess_target_table(select_query)
    pg_id, headers = create_process_group()

    sql_id = add_processor(
        group_id=pg_id,
        name="ExecuteSQL",
        type_="org.apache.nifi.processors.standard.ExecuteSQL",
        position=(0.0, 0.0),
        config_props={
            "SQL Query": select_query,
            "Database Connection Pooling Service": "7fffd473-8df8-33d3-24b7-4e766c3b62dd"
        },
        headers=headers
    )

    convert_id = add_processor(
        group_id=pg_id,
        name="ConvertRecord",
        type_="org.apache.nifi.processors.standard.ConvertRecord",
        position=(200.0, 0.0),
        config_props={
            "Record Reader": "80652a44-c9ce-3d0d-5049-a66a736d1a23",
            "Record Writer": "2b57bfdb-903d-3351-9ed2-a14c5d3efd34"
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
            "put-db-record-dcbp-service": "7fffd473-8df8-33d3-24b7-4e766c3b62dd",
            "put-db-record-record-reader": "72ea0e0d-40b2-30e6-0d6f-f7b2943930fa"
        },
        headers=headers
    )

    print("⏳ Ожидание инициализации процессоров (3 сек)...")
    time.sleep(3)

    connect_processors(pg_id, sql_id, convert_id, headers)
    connect_processors(pg_id, convert_id, put_id, headers)

    return {
        "status": "ok",
        "message": "Flow created successfully",
        "process_group_id": pg_id
    }