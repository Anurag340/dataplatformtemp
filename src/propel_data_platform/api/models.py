from typing import Any, Dict, Optional, Union

from pydantic import BaseModel


class SourceCreateRequest(BaseModel):
    org_id: Union[str, int]
    workspace_id: Union[str, int]
    tool_id: Union[str, int]
    config: Union[str, Dict[str, Any]]
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class SourceCreateResponse(BaseModel):
    valid: bool
    task_id: Optional[str] = None


class PingResponse(BaseModel):
    message: str
