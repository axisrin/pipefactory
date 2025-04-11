from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from nifi_client import generate_etl_flow

app = FastAPI()

class FlowRequest(BaseModel):
    select_query: str

@app.post("/generate-flow")
def generate_flow(req: FlowRequest):
    try:
        return generate_etl_flow(req.select_query)
    except Exception as e:
        print("❌ Ошибка:", e)
        raise HTTPException(status_code=500, detail=str(e))
