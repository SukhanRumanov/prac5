from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

class ProductCreate(BaseModel):
    url: str

class ProductResponse(BaseModel):
    id: int
    url: str
    name: Optional[str] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    created_at: datetime

    class Config:
        from_attributes = True

class PriceHistoryResponse(BaseModel):
    id: int
    product_id: int
    price: float
    created_at: datetime

    class Config:
        from_attributes = True

class PriceUpdateResponse(BaseModel):
    product_id: int
    product_name: str
    old_price: Optional[float] = None
    new_price: float
    timestamp: str