from pydantic import BaseModel

class FlowGenerationRequest(BaseModel):
    select_query: str
    target_table: str
