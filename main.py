from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from flow_generator import generate_flow_payload
from nifi_client import create_nifi_flow

app = FastAPI()

class ETLRequest(BaseModel):
    source_query: str
    target_table: str
    target_columns: list[str]

@app.post("/generate-etl-flow")
def generate_etl_flow(request: ETLRequest):
    try:
        flow_payload = generate_flow_payload(request.source_query, request.target_table, request.target_columns)
        result = create_nifi_flow(flow_payload)
        return {"status": "ok", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
