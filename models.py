from pydantic import BaseModel
from typing import Optional, Any

class Message(BaseModel):
    id: str
    source: str
    sourceLabel: str
    category: str
    title: str
    tldr: str
    body: str
    time: str
    timestamp: str
    priority: str
    status: str
    meta: Optional[dict[str, Any]] = None
